package finaljoin

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/cima-lexis/wundererr/core"
	"github.com/fhs/go-netcdf/netcdf"
)

// dimensions lengths
var lonLen = uint64(3600)
var latLen = uint64(1801)
var timeLen = uint64(24)

func prepareInputFile(date string) (eraData netcdf.Dataset, timeMap map[string]int, lonMap []float32, latMap []float32, timeValues []int32) {
	eraFile := "data/era5-merged-" + date + ".nc"

	eraData, err := netcdf.OpenFile(eraFile, netcdf.NOWRITE)
	if err != nil {
		panic(err)
	}
	timeV, err := eraData.Var("time")
	if err != nil {
		panic(err)
	}

	timeValues = make([]int32, 24)
	err = timeV.ReadInt32s(timeValues)
	if err != nil {
		panic(err)
	}

	lonV, err := eraData.Var("longitude")
	if err != nil {
		panic(err)
	}

	lonMap = make([]float32, lonLen)
	err = lonV.ReadFloat32s(lonMap)
	if err != nil {
		panic(err)
	}

	latV, err := eraData.Var("latitude")
	if err != nil {
		panic(err)
	}

	latMap = make([]float32, latLen)
	err = latV.ReadFloat32s(latMap)
	if err != nil {
		panic(err)
	}

	return
}

func readObservationsFromFile(date string, obsRead chan map[string]interface{}) {
	sourceFile := "data/prep-wund-" + date + ".json"
	jsonFile, err := os.Open(sourceFile)
	if err != nil {
		log.Panic(err)
	}
	defer jsonFile.Close()

	jsonReader := bufio.NewReader(jsonFile)

	// skip firt line - [
	line, err := jsonReader.ReadString('\n')
	if err != nil {
		log.Panic(err)
	}

	if strings.TrimSpace(line) != "[" {
		log.Panic(err)
	}

	// read first line, no initial comma
	line, err = jsonReader.ReadString('\n')
	if err != nil {
		log.Panic(err)
	}

	var observation map[string]interface{}
	err = json.Unmarshal([]byte(line), &observation)
	if err != nil {
		log.Panic(err)
	}
	obsRead <- observation

	for {
		line, err := jsonReader.ReadString('\n')
		if err != nil {
			log.Panic(err)
		}

		if strings.TrimSpace(line) == "]" {
			break
		}

		var observation map[string]interface{}

		err = json.Unmarshal([]byte(line[1:]), &observation)
		if err != nil {
			log.Panic(err)
		}

		obsRead <- observation
	}

	close(obsRead)
}

func parseDate(dt int32) time.Time {
	return time.Unix(int64(dt)*60*60-int64(2208988800), 0)
}

func calcHumRel(d2m_c, t2m_c float64) float64 {
	return (d2m_c - 0.84*t2m_c + 19.2) / (0.198 + 0.0017*t2m_c)
}

func Run(date string, domain *core.Domain) {
	targetFile := "data/results-" + date + ".csv"
	errsFile := "data/errs-" + date + ".csv"

	_, err := os.Stat(targetFile)
	if err == nil {
		fmt.Printf("[5] ✔️ Skipping result file exists: `%s`\n", targetFile)
		return
	}

	//eraDataBefore, timeMapBefore := prepareInputFile(dateBefore)

	eraData, _, lonMap, latMap /*, timeValues*/, _ := prepareInputFile(date)
	defer eraData.Close()

	obsRead := make(chan map[string]interface{})
	go readObservationsFromFile(date, obsRead)

	//fmt.Println(latMap)
	//fmt.Println(lonMap)
	//fmt.Println(timeMap)
	//fmt.Println(eraData)

	findLatIdx := func(coordToFind float32, coordMap []float32) uint64 {
		searchFn := func(i int) bool {
			return coordMap[i] < coordToFind
		}

		idx := sort.Search(len(coordMap), searchFn)
		if idx < len(coordMap) {
			return uint64(idx)
		}

		log.Panicf("%f not found\n", coordToFind)
		return 0
	}

	findLonIdx := func(coordToFind float32, coordMap []float32) uint64 {
		searchFn := func(i int) bool {
			return coordMap[i] > coordToFind
		}

		idx := sort.Search(len(coordMap), searchFn)
		if idx < len(coordMap) {
			return uint64(idx)
		}

		log.Panicf("%f not found\n", coordToFind)
		return 0
	}

	t2m := make([]float32, timeLen*latLen*lonLen)

	t2mV, err := eraData.Var("t2m")
	if err != nil {
		panic(err)
	}

	err = t2mV.ReadFloat32s(t2m)
	if err != nil {
		panic(err)
	}

	d2m := make([]float32, timeLen*latLen*lonLen)

	d2mV, err := eraData.Var("d2m")
	if err != nil {
		panic(err)
	}

	err = d2mV.ReadFloat32s(d2m)
	if err != nil {
		panic(err)
	}



	u10 := make([]float32, timeLen*latLen*lonLen)

	u10V, err := eraData.Var("u10")
	if err != nil {
		panic(err)
	}

	err = u10V.ReadFloat32s(u10)
	if err != nil {
		panic(err)
	}



	v10 := make([]float32, timeLen*latLen*lonLen)

	v10V, err := eraData.Var("v10")
	if err != nil {
		panic(err)
	}

	err = v10V.ReadFloat32s(v10)
	if err != nil {
		panic(err)
	}




	/*
		for _, dt := range timeValues {
			dt := parseDate(int32(dt)).UTC()
			fmt.Println(dt.Format("2006010215"))
		}
	*/
	timeStride := latLen * lonLen
	latStride := lonLen

	outFile, err := os.Create(targetFile)
	if err != nil {
		panic(err)
	}

	defer outFile.Close()
	fmt.Fprintf(outFile, "ID,hour,era_t2m,wund_t2m,era_d2m,wund_d2m,era_hum,wund_hum,era_u10,wund_u10,era_v10,wund_v10\n")

	errorsFile, err := os.Create(errsFile)
	if err != nil {
		panic(err)
	}

	defer errorsFile.Close()
	fmt.Fprintf(errorsFile, "ID,tot_hours,err_t2m,err_d2m,err_hum\n")

	stations := readStationsFromFile()
	idx := 0.0
	stationsLen := float64(len(stations))
	lastProgress := 0.0

	for station := range obsRead {

		errHum := 0.0
		errT2m := 0.0
		errD2m := 0.0

		progress := math.Round(idx*100*100/stationsLen) / 100
		if progress != lastProgress {
			fmt.Printf("\033[F")
			fmt.Printf("\033[K")
			fmt.Printf("[5] 🡒 Preparing results file: %.2f %%\n", progress)
			lastProgress = progress
		}

		idx++
		latitude := float32(station["latitude"].(float64))
		longitude := float32(station["longitude"].(float64))
		stID := station["ID"].(string)
		latIdx := findLatIdx(latitude, latMap)
		lonIdx := findLonIdx(longitude, lonMap)

		var observations []interface{} = station["data"].(map[string]interface{})["observations"].([]interface{})

		totHours := 0

		for _, obsInterface := range observations {
			totHours++
			obsTimeUtc := obsInterface.(map[string]interface{})["obsTimeUtc"].(string)
			metric := obsInterface.(map[string]interface{})["metric"].(map[string]interface{})
			tempWund, ok := metric["tempAvg"].(float64)
			if !ok {
				tempWund = math.NaN()
			}

			humidityMayBe := obsInterface.(map[string]interface{})["humidityAvg"]
			humidityWund := math.NaN()
			if humidityMayBe != nil {
				humidityWund = humidityMayBe.(float64)
			}

			dewpointWund, ok := metric["dewptAvg"].(float64)
			if !ok {
				dewpointWund = math.NaN()
			}

			dt, err := time.Parse(time.RFC3339, obsTimeUtc)
			if err != nil {
				panic(err)
			}
			timeIdx := uint64(dt.Hour())
			//fmt.Println("calculated:", t2m[timeIdx*timeStride+latIdx*latStride+lonIdx])
			//valIndex, err := t2mV.ReadFloat32At([]uint64{timeIdx, latIdx, lonIdx})
			//if err != nil {
			//	panic(err)
			//}
			t2mEra := float64(t2m[timeIdx*timeStride+latIdx*latStride+lonIdx])
			d2mEra := float64(d2m[timeIdx*timeStride+latIdx*latStride+lonIdx])
			humidityEra := float64(calcHumRel(d2mEra, t2mEra))
			era_u10,wund_u10,era_v10,wund_v10
			fmt.Fprintf(
				outFile,
				"%s,%d,%f,%f,%f,%f,%f,%f\n",
				stID,
				dt.Hour(),
				t2mEra,
				tempWund,
				d2mEra,
				dewpointWund,
				humidityEra,
				humidityWund,
			)

			errHum += math.Pow(humidityEra, 2) - math.Pow(humidityWund, 2)
			errT2m += math.Pow(d2mEra, 2) - math.Pow(dewpointWund, 2)
			errD2m += math.Pow(t2mEra, 2) - math.Pow(tempWund, 2)
		}

		errHum /= stationsLen
		errT2m /= stationsLen
		errD2m /= stationsLen
		errHum = math.Sqrt(errHum)
		errT2m = math.Sqrt(errT2m)
		errD2m = math.Sqrt(errD2m)

		fmt.Fprintf(errorsFile, "%s,%d,%f,%f,%f\n", stID, totHours, errT2m, errD2m, errHum)

	}

	fmt.Printf("\033[F")
	fmt.Printf("\033[K")
	fmt.Printf("[5] ✔️ Prepared result file: `%s`\n", targetFile)

}

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
