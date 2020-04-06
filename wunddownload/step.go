package wunddownload

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/cima-lexis/wundererr/wundarchive"
)

// represents a station as read from json file
type station struct {
	ID        string
	Latitude  float64
	Longitude float64
	Tz        int
}

// kind of result for a single station read
type resultKind int

const (
	resultKindDownloaded   resultKind = 0 // result from website
	resultKindFromCache    resultKind = 1 // result from cache
	resultKindErr          resultKind = 2 // an error occurred
	resultKindNotAvailable resultKind = 3 // the stations has no obervations for the day
)

// result for a single station read
type stationResult struct {
	ID     string
	buffer []byte
	err    error
	kind   resultKind
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

type readRequest struct {
	stationID string
	date      time.Time
}

func Download(date string) {
	targetFile := "data/wund-" + date + ".json"
	stations := readStationsFromFile()

	_, err := os.Stat(targetFile)
	if err == nil {
		fmt.Printf("[1] ✔️ Skipping, Wunderground observations file exists: `%s`\n", targetFile)
		return
	}

	// write id of stations that downloadObservations
	// should download
	stationsToRead := make(chan readRequest)
	// read data downloaded in byte buffers chunks
	stationsRead := make(chan stationResult)
	// read save operation progress in percentage
	progress := make(chan float32)

	allDownloadCompleted := &sync.WaitGroup{}
	for i := 0; i < 5; i++ {
		allDownloadCompleted.Add(1)
		go downloadObservations(stationsToRead, stationsRead, allDownloadCompleted)
	}

	go saveJSON(len(stations), date, stationsRead, progress)

	go func() {
		for _, st := range stations {
			dt, err := time.Parse("20060102", date)
			if err != nil {
				panic(err)
			}
			stationsToRead <- readRequest{st.ID, dt}
			// fmt.Println(st.Tz)
			if st.Tz > 0 {
				dt = dt.AddDate(0, 0, 1)
				stationsToRead <- readRequest{st.ID, dt}
			}
		}

		close(stationsToRead)

		// wait for all downloadObservations goroutines
		// to complete their downloads
		allDownloadCompleted.Wait()

		close(stationsRead)
	}()

	for currProgr := range progress {
		fmt.Printf("\033[F")
		fmt.Printf("\033[K")
		fmt.Printf("[1] 🡒 Building Wunderground observations file: %.2f %%\n", currProgr)
	}

	fmt.Printf("\033[F")
	fmt.Printf("\033[K")
	fmt.Printf("[1] ✔️ Built Wunderground observations file: `%s`\n", targetFile)

}

// read downloaded observations from stationsRead chan,
// and write each buffer to a giant json file.
// write progress of operations to progress chan.
// this is to be run as a single go routines that
// consumes all data read from multiple other go rountines.
func saveJSON(totalStations int, date string, stationsRead chan stationResult, progress chan float32) {
	defer close(progress)

	f, err := os.Create("data/wund-" + date + ".json")
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	w := bufio.NewWriter(f)

	_, err = w.WriteString("[\n")
	if err != nil {
		log.Fatal(err)
	}
	firstChunk := true

	runningCount := 0.0
	currentProgress := 0.0
	for chunk := range stationsRead {
		newProgress := math.Round(runningCount*10000/float64(totalStations)) / 100
		if currentProgress != newProgress {
			progress <- float32(newProgress)
		}

		currentProgress = newProgress
		runningCount += 1.0

		if chunk.kind != resultKindErr {
			if !firstChunk {
				_, err = w.WriteString(",\n")
				if err != nil {
					log.Fatal(err)
				}
			}
			firstChunk = false

			if chunk.kind == resultKindNotAvailable || len(chunk.buffer) == 0 {
				_, err := w.WriteString("{\n  \"ID\": \"" + chunk.ID + "\",\n  \"empty\": true,\n  \"data\": {\"observations\":[]}\n}\n")
				if err != nil {
					log.Fatal(err)
				}
			} else {
				_, err := w.WriteString("{\n  \"ID\": \"" + chunk.ID + "\",\n  \"empty\": false,\n  \"data\": ")
				if err != nil {
					log.Fatal(err)
				}

				if string(chunk.buffer[len(chunk.buffer)-1]) != "\n" {
					chunk.buffer = append(chunk.buffer, []byte("\n")[0])
				}

				_, err = w.Write(chunk.buffer)
				if err != nil {
					log.Fatal(err)
				}

				_, err = w.WriteString("}\n")
				if err != nil {
					log.Fatal(err)
				}
				if err != nil {
					log.Fatal(err)
				}
			}

			w.Flush()

			continue
		}

		// fmt.Fprintln(os.Stderr, "Error downloading stations", chunk.err, chunk.kind)
		// fmt.Fprintf(os.Stderr, "Error downloading stations %s: %s\n", chunk.ID, chunk.err.Error())

	}
	_, err = w.WriteString("\n]\n")
	if err != nil {
		log.Fatal(err)
	}
	w.Flush()

}

// download observations hourly aggregations data from weather.com for given date.
// ID of stations to get is read from stationsToRead channel, and every ID
// cause a separate GET. This function can be concurrently run on multiple goroutines
// file downloaded are saved as-is indirectory cache. If same url
// is required again, that file is read to avoid an http call.
// buffers read are the emitted on stationsRead channel.
func downloadObservations(stationsToRead chan readRequest, stationsRead chan stationResult, allDownloadCompleted *sync.WaitGroup) {
	for stReq := range stationsToRead {
		dtReq := stReq.date.Format("20060102")
		cacheDir := fmt.Sprintf("data/cache/%s", dtReq)
		archiveFile := fmt.Sprintf("data/wundarchive/%s.tar.gz", dtReq)

		if _, err := os.Stat(cacheDir); err == nil {
			if err := os.MkdirAll(cacheDir, os.FileMode(0755)); err != nil {
				log.Fatal(err)
			}

			if _, err := os.Stat(archiveFile); err == nil {
				wundarchive.PrepareArchive(dtReq)
			}

		}

		fileName := fmt.Sprintf("%s/%s.json", cacheDir, stReq.stationID)

		apiKey := os.Getenv("WUNDER_HIST_KEY")
		if apiKey == "" {
			log.Fatal("You must set WUNDER_HIST_KEY environment variable to the IBM API key.")
		}

		// if file is already stored in cache directory, we read
		// that file instead of retrieving data for web
		_, err := os.Stat(fileName)
		if err == nil {
			buff, err := ioutil.ReadFile(fileName)

			if err != nil {
				stationsRead <- stationResult{
					ID:     stReq.stationID,
					buffer: nil,
					err:    err,
					kind:   resultKindErr,
				}
				continue
			}

			stationsRead <- stationResult{
				ID:     stReq.stationID,
				buffer: buff,
				err:    nil,
				kind:   resultKindFromCache,
			}
			continue
		}
		/*
			stationsRead <- stationResult{
				ID:     stID,
				buffer: nil,
				err:    nil,
				kind:   resultKindNotAvailable,
			}
		*/

		log.Fatal("NO DOWNLOAD", stReq)
		/*
			err = ioutil.WriteFile(fileName, []byte("{\"observations\": []}"), os.FileMode(0644))
			if err != nil {
				log.Fatal(err)
			}
		*/
		url := "https://api.weather.com/v2/pws/history/hourly?stationId=" + stReq.stationID + "&format=json&units=m&date=" + stReq.date.Format("20060102") + "&apiKey=" + apiKey

		buff, err := downloadFile(fileName, url)
		if err != nil {
			err2 := ioutil.WriteFile(fileName, []byte("{\"observations\": []}"), os.FileMode(0644))
			if err2 != nil {
				log.Fatal(err2)
			}
			stationsRead <- stationResult{
				ID:     stReq.stationID,
				buffer: nil,
				err:    err,
				kind:   resultKindErr,
			}
			continue
		}

		stationsRead <- stationResult{
			ID:     stReq.stationID,
			buffer: buff,
			err:    nil,
			kind:   resultKindDownloaded,
		}
	}

	allDownloadCompleted.Done()
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

// sync download a single url to filepath
func downloadFile(filepath string, url string) ([]byte, error) {

	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != 200 {
		return nil, errors.New(resp.Status)
	}

	defer resp.Body.Close()

	out, err := os.Create(filepath)
	if err != nil {
		return nil, err
	}
	defer out.Close()

	reader := io.TeeReader(resp.Body, out)
	buff, err := streamToBytes(reader)
	if err != nil {
		return nil, err
	}
	return buff, nil
}
