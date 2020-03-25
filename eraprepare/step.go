package eraprepare

import (
	"fmt"
	"log"
	"math"
	"time"

	"github.com/cima-lexis/wundererr/core"
	"github.com/fhs/go-netcdf/netcdf"
)

func parseDate(dt int32) time.Time {
	return time.Unix(int64(dt)*60*60-int64(2208988800), 0)
}

func createOutputFile(date string) netcdf.Dataset {
	eraOutFile := "data/era5-merged-" + date + ".nc"
	eraOutData, err := netcdf.CreateFile(eraOutFile, netcdf.NETCDF4)
	if err != nil {
		log.Println("QUa")
		panic(err)
	}

	lonDim, err := eraOutData.AddDim("longitude", uint64(3600))
	if err != nil {
		panic(err)
	}
	latDim, err := eraOutData.AddDim("latitude", uint64(1801))
	if err != nil {
		panic(err)
	}
	timeDim, err := eraOutData.AddDim("time", uint64(24))
	if err != nil {
		panic(err)
	}

	lonVar, err := eraOutData.AddVar("longitude", netcdf.FLOAT, []netcdf.Dim{lonDim})
	if err != nil {
		panic(err)
	}
	if err := lonVar.Attr("units").WriteBytes([]byte("degrees_east")); err != nil {
		panic(err)
	}
	if err := lonVar.Attr("long_name").WriteBytes([]byte("longitude")); err != nil {
		panic(err)
	}

	latVar, err := eraOutData.AddVar("latitude", netcdf.FLOAT, []netcdf.Dim{latDim})
	if err != nil {
		panic(err)
	}
	if err := latVar.Attr("units").WriteBytes([]byte("degrees_north")); err != nil {
		panic(err)
	}
	if err := latVar.Attr("long_name").WriteBytes([]byte("latitude")); err != nil {
		panic(err)
	}

	timeVar, err := eraOutData.AddVar("time", netcdf.INT, []netcdf.Dim{timeDim})
	if err != nil {
		panic(err)
	}
	if err := timeVar.Attr("units").WriteBytes([]byte("hours since 1900-01-01 00:00:00.0")); err != nil {
		panic(err)
	}
	if err := timeVar.Attr("long_name").WriteBytes([]byte("time")); err != nil {
		panic(err)
	}
	if err := timeVar.Attr("calendar").WriteBytes([]byte("gregorian")); err != nil {
		panic(err)
	}

	threeDims := []netcdf.Dim{timeDim, latDim, lonDim}
	u10Var, err := eraOutData.AddVar("u10", netcdf.FLOAT, threeDims)
	if err != nil {
		panic(err)
	}
	if err := u10Var.Attr("units").WriteBytes([]byte("m s**-1")); err != nil {
		panic(err)
	}
	if err := u10Var.Attr("long_name").WriteBytes([]byte("10 metre U wind component")); err != nil {
		panic(err)
	}

	v10Var, err := eraOutData.AddVar("v10", netcdf.FLOAT, threeDims)
	if err != nil {
		panic(err)
	}
	if err := v10Var.Attr("units").WriteBytes([]byte("m s**-1")); err != nil {
		panic(err)
	}
	if err := v10Var.Attr("long_name").WriteBytes([]byte("10 metre V wind component")); err != nil {
		panic(err)
	}

	d2mVar, err := eraOutData.AddVar("d2m", netcdf.FLOAT, threeDims)
	if err != nil {
		panic(err)
	}
	if err := d2mVar.Attr("units").WriteBytes([]byte("C")); err != nil {
		panic(err)
	}
	if err := d2mVar.Attr("long_name").WriteBytes([]byte("2 metre dewpoint temperature")); err != nil {
		panic(err)
	}

	t2mVar, err := eraOutData.AddVar("t2m", netcdf.FLOAT, threeDims)
	if err != nil {
		panic(err)
	}
	if err := t2mVar.Attr("units").WriteBytes([]byte("C")); err != nil {
		panic(err)
	}
	if err := t2mVar.Attr("long_name").WriteBytes([]byte("2 metre temperature")); err != nil {
		panic(err)
	}

	elevationVar, err := eraOutData.AddVar("elevation", netcdf.FLOAT, threeDims)
	if err != nil {
		panic(err)
	}
	if err := elevationVar.Attr("units").WriteBytes([]byte("m")); err != nil {
		panic(err)
	}
	if err := elevationVar.Attr("long_name").WriteBytes([]byte("elevation")); err != nil {
		panic(err)
	}

	return eraOutData
}

func Run(date, dateBefore string, domain *core.Domain) {
	targetFile := "data/era5-merged-" + date + ".nc"

	eraFile := "data/era5-" + date + ".nc"
	eraData, err := netcdf.OpenFile(eraFile, netcdf.NOWRITE)
	if err != nil {
		log.Println("QUI")
		panic(err)
	}
	defer eraData.Close()

	// time
	timeV, err := eraData.Var("time")
	if err != nil {
		panic(err)
	}

	time, err := netcdf.GetInt32s(timeV)
	if err != nil {
		panic(err)
	}

	timeDims, err := timeV.LenDims()
	if err != nil {
		panic(err)
	}

	// latitude
	latitudeV, err := eraData.Var("latitude")
	if err != nil {
		panic(err)
	}
	/*
		latitude, err := netcdf.GetInt32s(latitudeV)
		if err != nil {
			panic(err)
		}
	*/
	latitudeDims, err := latitudeV.LenDims()
	if err != nil {
		panic(err)
	}

	// longitude
	longitudeV, err := eraData.Var("longitude")
	if err != nil {
		panic(err)
	}
	/*
		longitude, err := netcdf.GetInt32s(longitudeV)
		if err != nil {
			panic(err)
		}
	*/
	longitudeDims, err := longitudeV.LenDims()
	if err != nil {
		panic(err)
	}

	eraOutData := createOutputFile(date)
	defer eraOutData.Close()

	outTimeIdx := uint64(0)
	tot := timeDims[0] * latitudeDims[0] * longitudeDims[0]
	runningIdx := uint64(0)
	lastProgress := -1.0

	reportProgress := func() {
		progress := math.Round(float64(runningIdx)*100*100/float64(tot)) / 100
		if progress != lastProgress {
			fmt.Printf("\033[F")
			fmt.Printf("\033[K")
			fmt.Printf("[4] 🡒 Preparing Era5 single file: %.2f %%\n", progress)
			lastProgress = progress
		}
	}

	for t := uint64(0); t < uint64(timeDims[0]); t++ {
		dt := parseDate(time[t])
		if dt.Format("20060102") != date {
			runningIdx += latitudeDims[0] * longitudeDims[0]
			reportProgress()
			continue
		}

		for lat := uint64(0); lat < uint64(latitudeDims[0]); lat++ {
			for lon := uint64(0); lon < uint64(longitudeDims[0]); lon++ {
				latValue, err := latitudeV.ReadFloat32At([]uint64{lat})
				if err != nil {
					panic(err)
				}
				lonValue, err := longitudeV.ReadFloat32At([]uint64{lon})
				if err != nil {
					panic(err)
				}
				if float64(latValue) < domain.MinLat || float64(latValue) > domain.MaxLat ||
					float64(lonValue) < domain.MinLon || float64(lonValue) > domain.MaxLon {
					writeEmptyVar(eraOutData, "u10", t, lat, lon, outTimeIdx)
					writeEmptyVar(eraOutData, "v10", t, lat, lon, outTimeIdx)
					writeEmptyVar(eraOutData, "d2m", t, lat, lon, outTimeIdx)
					writeEmptyVar(eraOutData, "t2m", t, lat, lon, outTimeIdx)
					runningIdx++
					reportProgress()
					continue
				}

				copyVar(eraData, eraOutData, "u10", t, lat, lon, outTimeIdx, 0)
				copyVar(eraData, eraOutData, "v10", t, lat, lon, outTimeIdx, 0)
				copyVar(eraData, eraOutData, "d2m", t, lat, lon, outTimeIdx, -273.15)
				copyVar(eraData, eraOutData, "t2m", t, lat, lon, outTimeIdx, -273.15)
				runningIdx++
				reportProgress()
			}
		}
		outTimeIdx++
	}

	fmt.Printf("\033[F")
	fmt.Printf("\033[K")
	fmt.Printf("[4] ✔️ Prepared Era5 single file: `%s`\n", targetFile)

}

func copyVar(eraData, eraOutData netcdf.Dataset, varName string, t, lat, lon uint64, outTimeIdx uint64, deltaConversion float64) {
	inVar, err := eraData.Var(varName)
	if err != nil {
		panic(err)
	}

	outVar, err := eraOutData.Var(varName)
	if err != nil {
		panic(err)
	}

	val, err := inVar.ReadInt16At([]uint64{t, lat, lon})
	if err != nil {
		panic(err)
	}

	scaleFactor := []float64{0}
	err = inVar.Attr("scale_factor").ReadFloat64s(scaleFactor)
	if err != nil {
		panic(err)
	}

	addOffset := []float64{0}
	err = inVar.Attr("add_offset").ReadFloat64s(scaleFactor)
	if err != nil {
		panic(err)
	}

	valF := float32(float64(val)*scaleFactor[0] + addOffset[0] + deltaConversion)

	err = outVar.WriteFloat32At([]uint64{outTimeIdx, lat, lon}, valF)
	if err != nil {
		panic(err)
	}
}

func writeEmptyVar(eraOutData netcdf.Dataset, varName string, t, lat, lon uint64, outTimeIdx uint64) {

	outVar, err := eraOutData.Var(varName)
	if err != nil {
		panic(err)
	}

	err = outVar.WriteFloat32At([]uint64{outTimeIdx, lat, lon}, 0)
	if err != nil {
		panic(err)
	}
}
