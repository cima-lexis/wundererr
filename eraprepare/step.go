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

func createOutputFile(date string, inputData netcdf.Dataset) netcdf.Dataset {
	eraOutFile := "data/era5-merged-" + date + ".nc"
	eraOutData, err := netcdf.CreateFile(eraOutFile, netcdf.NETCDF4)
	if err != nil {
		log.Println("QUa")
		panic(err)
	}

	// dimensions lengths
	lonLen := uint64(3600)
	latLen := uint64(1801)
	timeLen := uint64(24)

	// create dimensions
	lonDim, err := eraOutData.AddDim("longitude", lonLen)
	if err != nil {
		panic(err)
	}
	latDim, err := eraOutData.AddDim("latitude", latLen)
	if err != nil {
		panic(err)
	}
	timeDim, err := eraOutData.AddDim("time", timeLen)
	if err != nil {
		panic(err)
	}

	// input data geo coord var
	inputLonVar, err := inputData.Var("longitude")
	if err != nil {
		panic(err)
	}
	inputLatVar, err := inputData.Var("latitude")
	if err != nil {
		panic(err)
	}

	// create lat and lon variables
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

	// fill lat and lon variables with same values
	// as input dataset
	inputLatVarData := make([]float32, latLen)
	err = inputLatVar.ReadFloat32s(inputLatVarData)
	if err != nil {
		panic(err)
	}
	err = latVar.WriteFloat32s(inputLatVarData)
	if err != nil {
		panic(err)
	}

	inputLonVarData := make([]float32, lonLen)
	err = inputLonVar.ReadFloat32s(inputLonVarData)
	if err != nil {
		panic(err)
	}
	err = lonVar.WriteFloat32s(inputLonVarData)
	if err != nil {
		panic(err)
	}

	// create time variable
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

	// fill time variable
	dt, err := time.Parse("20060102", date)
	if err != nil {
		panic(err)
	}

	hoursFrom1900AtMidnight := int32(dt.Unix()/(60*60) + 613608)
	inputTimeVarData := make([]int32, lonLen)

	for h := int32(0); h < 24; h++ {
		inputTimeVarData[h] = h + hoursFrom1900AtMidnight
	}

	err = timeVar.WriteInt32s(inputTimeVarData)
	if err != nil {
		panic(err)
	}

	// create u10 var

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

	// create v10 var

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

	// create d2m var

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

	// create t2m var

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

	// create elevation var

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

	eraOutData := createOutputFile(date, eraData)
	defer eraOutData.Close()

	copyVar(0, eraData, eraOutData, "d2m", 0)
	copyVar(1, eraData, eraOutData, "t2m", 0)
	copyVar(2, eraData, eraOutData, "u10", -273.15)
	copyVar(3, eraData, eraOutData, "v10", -273.15)

	fmt.Printf("\033[F")
	fmt.Printf("\033[K")
	fmt.Printf("[4] ✔️ Prepared Era5 single file: `%s`\n", targetFile)

}

func copyVar(idxVar int, eraData, eraOutData netcdf.Dataset, varName string, deltaConversion float64) {
	inVar, err := eraData.Var(varName)
	if err != nil {
		panic(err)
	}

	varLen, err := inVar.Len()
	if err != nil {
		panic(err)
	}

	varData := make([]int16, varLen)
	varDataOut := make([]float32, varLen)

	err = inVar.ReadInt16s(varData)
	if err != nil {
		panic(err)
	}

	outVar, err := eraOutData.Var(varName)
	if err != nil {
		panic(err)
	}

	scaleFactorVec := []float64{0}
	err = inVar.Attr("scale_factor").ReadFloat64s(scaleFactorVec)
	if err != nil {
		panic(err)
	}

	addOffsetVec := []float64{0}
	err = inVar.Attr("add_offset").ReadFloat64s(addOffsetVec)
	if err != nil {
		panic(err)
	}

	scaleFactor := scaleFactorVec[0]
	addOffset := addOffsetVec[0]
	idx := uint64(0)
	lastProgress := float64(0)

	reportProgress := func() {
		progress := float64(idxVar)*25.0 + math.Round(float64(idx)*100*100/float64(varLen))/100/4
		if progress != lastProgress {
			fmt.Printf("\033[F")
			fmt.Printf("\033[K")
			fmt.Printf("[4] 🡒 Preparing Era5 single file: %.2f %%\n", progress)
			lastProgress = progress
		}
	}

	for idx = (0); idx < varLen; idx++ {
		varDataOut[idx] = float32(float64(varData[idx])*scaleFactor + addOffset + deltaConversion)
		reportProgress()
	}

	err = outVar.WriteFloat32s(varDataOut)

	if err != nil {
		panic(err)
	}
}
