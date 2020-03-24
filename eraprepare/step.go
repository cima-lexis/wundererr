package eraprepare

import (
	"fmt"
	"log"
	"time"

	"github.com/cima-lexis/wundererr/core"
	"github.com/fhs/go-netcdf/netcdf"
)

func parseDate(dt int32) time.Time {
	return time.Unix(int64(dt)*60*60-int64(2208988800), 0)
}

/*
func createOutputFile(date string) {
	eraOutFile := "data/era5-merged-" + date + ".nc"
	eraOutData, err := netcdf.OpenFile(eraOutFile, netcdf.WRITE)
	if err != nil {
		log.Fatal(err)
	}

	dims := make([]netcdf.Dim, 3)
	ht, wd := 5, 4
	dims[0], err = eraOutData.AddDim("height", uint64(ht))
	if err != nil {
		return err
	}
	dims[1], err = eraOutData.AddDim("width", uint64(wd))
	if err != nil {
		return err
	}

	eraOutData.AddVar("d2m", netcdf.SHORT)

	return eraOutData
}
*/
func Run(date string, domain *core.Domain) {
	eraFile := "data/era5-" + date + ".nc"
	eraData, err := netcdf.OpenFile(eraFile, netcdf.NOWRITE)
	if err != nil {
		log.Fatal(err)
	}
	defer eraData.Close()

	// time
	timeV, err := eraData.Var("time")
	if err != nil {
		log.Fatal(err)
	}

	time, err := netcdf.GetInt32s(timeV)
	if err != nil {
		log.Fatal(err)
	}

	timeDims, err := timeV.LenDims()
	if err != nil {
		log.Fatal(err)
	}
	/*
		// latitude
		latitudeV, err := eraData.Var("latitude")
		if err != nil {
			log.Fatal(err)
		}

		latitude, err := netcdf.GetInt32s(latitudeV)
		if err != nil {
			log.Fatal(err)
		}

		latitudeDims, err := latitudeV.LenDims()
		if err != nil {
			log.Fatal(err)
		}

		// longitude
		longitudeV, err := eraData.Var("longitude")
		if err != nil {
			log.Fatal(err)
		}

		longitude, err := netcdf.GetInt32s(longitudeV)
		if err != nil {
			log.Fatal(err)
		}

		longitudeDims, err := longitudeV.LenDims()
		if err != nil {
			log.Fatal(err)
		}
	*/

	//eraOutData := createOutputFile(date)
	//defer eraOutData.Close()

	i := 0
	for t := 0; t < int(timeDims[0]); t++ {
		dt := parseDate(time[i])
		//if dt.Format("20060102") == date {
		fmt.Printf("date: %s \n", dt.Format("2006010215"))
		//}

		i++
	}

}
