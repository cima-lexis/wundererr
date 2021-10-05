package wundprepare

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/cima-lexis/wundererr/core"
)

// represents a station as read from json file
type station struct {
	ID        string
	Latitude  float64
	Longitude float64
	Tz        int
}

// read list of stations to read from a JSON file.
func readStationsFromFile() []station {
	jsonFile, err := os.Open("data/euro-stations.json")
	if err != nil {
		log.Panic(err)
	}
	defer jsonFile.Close()

	byteValue, err := ioutil.ReadAll(jsonFile)
	if err != nil {
		log.Panic(err)
	}

	// we initialize our Users array
	var stations []station

	// we unmarshal our byteArray which contains our
	// jsonFile's content into 'users' which we defined above
	err = json.Unmarshal(byteValue, &stations)
	if err != nil {
		log.Panic(err)
	}

	return stations
}

type elev struct {
	elevation float64
	lat, lon  float64
}

func readElevationsFromFile() map[string]elev {
	csvFile, err := os.Open("data/elevations.csv")
	if err != nil {
		log.Panic(err)
	}
	defer csvFile.Close()

	csvReader := csv.NewReader(csvFile)

	stations := make(map[string]elev)

	for {
		rec, err := csvReader.Read()
		if err != nil {
			if err != io.EOF {
				log.Panic(err)
			}

			break
		}

		ID := rec[0]
		elevValue, err := strconv.ParseFloat(rec[3], 64)
		if err != io.EOF && err != nil {
			log.Panic(err)
		}

		// wunderground elevation is in feet
		if elevValue != -10000 {
			elevValue *= 0.3048
		}

		latValue, err := strconv.ParseFloat(rec[1], 64)
		if err != io.EOF && err != nil {
			log.Panic(err)
		}

		lonValue, err := strconv.ParseFloat(rec[2], 64)
		if err != io.EOF && err != nil {
			log.Panic(err)
		}

		stations[ID] = elev{
			elevation: elevValue,
			lon:       lonValue,
			lat:       latValue,
		}

	}

	return stations
}

func domainForStations(stations []station) *core.Domain {
	domain := &core.Domain{
		MaxLat: -999,
		MaxLon: -999,
		MinLat: 999,
		MinLon: 999,
	}

	for _, st := range stations {
		if st.Latitude > domain.MaxLat {
			domain.MaxLat = st.Latitude
		}

		if st.Longitude > domain.MaxLon {
			domain.MaxLon = st.Longitude
		}

		if st.Latitude < domain.MinLat {
			domain.MinLat = st.Latitude
		}

		if st.Longitude < domain.MinLon {
			domain.MinLon = st.Longitude
		}
	}

	domain.MaxLat = math.Ceil(domain.MaxLat)
	domain.MaxLon = math.Ceil(domain.MaxLon)
	domain.MinLat = math.Floor(domain.MinLat)
	domain.MinLon = math.Floor(domain.MinLon)

	return domain
}

func readObservationsFromFile(date string, obsRead chan map[string]interface{}) {
	sourceFile := "data/wund-" + date + ".json"

	checkOrPanic := func(err error) {
		if err != nil {
			log.Panicf("Error while reading file %s: %s", sourceFile, err)
		}
	}

	jsonFile, err := os.Open(sourceFile)
	checkOrPanic(err)

	defer jsonFile.Close()

	jsonReader := bufio.NewReader(jsonFile)

	// skip firt line - [
	line, _, err := jsonReader.ReadLine()
	checkOrPanic(err)

	if strings.TrimSpace(string(line)) != "[" {
		checkOrPanic(errors.New("Expecting ["))
	}

	for {
		// read observations of one station
		obsString := []byte{}
		for i := 0; i < 5; i++ {
			line, err := jsonReader.ReadString('\n')
			checkOrPanic(err)
			obsString = append(obsString, line...)
		}
		//fmt.Println(string(obsString))
		var observation map[string]interface{}

		err = json.Unmarshal(obsString, &observation)
		checkOrPanic(err)

		obsRead <- observation

		// skip sep line - ,
		line, _, err = jsonReader.ReadLine()
		checkOrPanic(err)

		if strings.TrimSpace(string(line)) != "," {
			break
		}
	}

	close(obsRead)
}

type stationDataBuffer struct {
	Tz           int
	observations []interface{}
	daysRead     int
}

func buildStationsByCode(stations []station) map[string]*stationDataBuffer {
	index := make(map[string]*stationDataBuffer)
	for _, st := range stations {
		index[st.ID] = &stationDataBuffer{
			Tz:           st.Tz,
			observations: []interface{}{},
			daysRead:     0,
		}
	}
	return index
}

// Run ...
func Run(date string) *core.Domain {
	targetFile := "data/prep-wund-" + date + ".json"
	stations := readStationsFromFile()
	stationsByCode := buildStationsByCode(stations)

	_, err := os.Stat(targetFile)
	if err == nil {
		fmt.Printf("[2] ✔️ Skipping, Wunderground prepared observations file exists: `%s`\n", targetFile)
		return domainForStations(stations)
	}

	obsRead := make(chan map[string]interface{})

	go readObservationsFromFile(date, obsRead)

	elevations := readElevationsFromFile()

	outFile, err := os.Create(targetFile)
	if err != nil {
		log.Fatal(err)
	}
	defer outFile.Close()
	_, err = outFile.WriteString("[")
	if err != nil {
		log.Fatal(err)
	}
	firstLine := true

	tot := len(stations)
	idx := 0
	lastProgress := 0.0
	for obs := range obsRead {
		idx++
		el := elevations[obs["ID"].(string)]

		obs["elevation"] = el.elevation
		obs["latitude"] = el.lat
		obs["longitude"] = el.lon

		station := stationsByCode[obs["ID"].(string)]

		if station.Tz > 0 {
			currObs := obs["data"].(map[string]interface{})["observations"].([]interface{})

			if station.daysRead == 0 {
				station.daysRead++
				station.observations = currObs
				continue
			} else {
				totObs := append(station.observations, currObs...)
				resObs := []interface{}{}
				for _, o := range totObs {
					tmpMap := o.(map[string]interface{})
					dtS, ok := tmpMap["obsTimeUtc"].(string)
					if !ok {
						dtS = tmpMap["ObsTimeUtc"].(string)
					}

					//2018-07-23T22:59:59Z
					dt, err := time.Parse(time.RFC3339, dtS)
					if err != nil {
						panic(err)
					}

					if dt.Format("20060102") == date {
						resObs = append(resObs, o)
					}
				}

				obs["data"].(map[string]interface{})["observations"] = resObs
			}
		}

		data, err := json.Marshal(obs)
		if err != nil {
			log.Fatal(err)
		}

		if firstLine {
			firstLine = false
			_, err = outFile.WriteString("\n")
			if err != nil {
				log.Fatal(err)
			}
		} else {
			_, err = outFile.WriteString("\n,")
			if err != nil {
				log.Fatal(err)
			}
		}

		_, err = outFile.Write(data)
		if err != nil {
			log.Fatal(err)
		}

		currProgr := math.Round(float64(idx)*100*100/float64(tot)) / 100

		if currProgr != lastProgress {
			fmt.Printf("\033[F")
			fmt.Printf("\033[K")
			fmt.Printf("[2] 🡒 Preparing Wunderground observations file: %.2f %%\n", currProgr)
		}
		lastProgress = currProgr
	}

	_, err = outFile.WriteString("\n]\n")
	if err != nil {
		log.Fatal(err)
	}

	/*
		for currProgr := range progress {

		}
	*/
	fmt.Printf("\033[F")
	fmt.Printf("\033[K")
	fmt.Printf("[2] ✔️ Prepared Wunderground observations file: `%s`\n", targetFile)

	return domainForStations(stations)
}

// completely read from a stream and concat into a byte buffer
func streamToBytes(stream io.Reader) ([]byte, error) {
	buf := new(bytes.Buffer)
	_, err := buf.ReadFrom(stream)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
