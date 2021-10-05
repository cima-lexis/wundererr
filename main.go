package main

import (
	"os"

	"github.com/cima-lexis/wundererr/eradownload"
	"github.com/cima-lexis/wundererr/eraprepare"
	"github.com/cima-lexis/wundererr/finaljoin"
	"github.com/cima-lexis/wundererr/wunddownload"
	"github.com/cima-lexis/wundererr/wundprepare"
)

func main() {
	date := os.Args[1]

	wunddownload.Download(date)
	return
	domain := wundprepare.Run(date)
	// fmt.Printf("%f:%f - %f:%f\n", domain.MinLat, domain.MinLon, domain.MaxLat, domain.MaxLon)

	eradownload.Download(date)
	//eradownload.Download(datePrev(date))

	eraprepare.Run(date, domain)
	finaljoin.Run(date, domain)
}
