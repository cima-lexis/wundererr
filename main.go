package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/cima-lexis/wundererr/wunddownload"
	"github.com/cima-lexis/wundererr/wundprepare"
)

func datePrev(date string) string {
	dt, err := time.Parse("20060102", date)
	if err != nil {
		log.Fatal(err)
	}
	return dt.AddDate(0, 0, -1).Format("20060102")
}

func main() {
	date := os.Args[1]

	wunddownload.Download(date)

	domain := wundprepare.Run(date)
	fmt.Printf("%f:%f - %f:%f\n", domain.MinLat, domain.MinLon, domain.MaxLat, domain.MaxLon)

	// eradownload.Download(date)
	// eradownload.Download(datePrev(date))
	// eraprepare.Run(datePrev(date), domain)
	// eraprepare.Run(date, domain)
}
