package wundprepare

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"os"
	"strconv"
	"strings"

	"github.com/cima-lexis/wundererr/core"
)

// represents a station as read from json file
type station struct {
	ID        string
	Latitude  float64
	Longitude float64
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
	jsonFile, err := os.Open(sourceFile)
	if err != nil {
		log.Panic(err)
	}
	defer jsonFile.Close()

	jsonReader := bufio.NewReader(jsonFile)

	// skip firt line - [
	line, _, err := jsonReader.ReadLine()
	if err != nil {
		log.Panic(err)
	}
	if strings.TrimSpace(string(line)) != "[" {
		log.Panic(err)
	}

	for {
		// read observations of one station
		obsString := []byte{}
		for i := 0; i < 5; i++ {
			line, err := jsonReader.ReadString('\n')
			//fmt.Println(i, string(line))
			if err != nil {
				log.Panic(err)
			}
			obsString = append(obsString, line...)
		}
		//fmt.Println(string(obsString))
		var observation map[string]interface{}

		err = json.Unmarshal(obsString, &observation)
		if err != nil {
			log.Panic(err)
		}
		obsRead <- observation

		// skip sep line - ,
		line, _, err = jsonReader.ReadLine()
		if err != nil {
			log.Panic(err)
		}

		if strings.TrimSpace(string(line)) != "," {
			break
		}
	}

	close(obsRead)
}

func Run(date string) *core.Domain {
	targetFile := "data/prep-wund-" + date + ".json"
	stations := readStationsFromFile()

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
		obs["latitute"] = el.lat
		obs["longitude"] = el.lon

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
