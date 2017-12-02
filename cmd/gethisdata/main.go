package main

import (
	"os"
	"fmt"
	"github.com/stephenlyu/TdxProtocol/network"
	"flag"
	"github.com/stephenlyu/tds/period"
	"github.com/stephenlyu/tds/entity"
)

const HOST_ONLY = "125.39.80.98"

func chk(err error) {
	if err != nil {
		fmt.Printf("[ERROR] error: %s\n", err.Error())
		os.Exit(1)
	}
}

func main() {
	periodStr := flag.String("period", "D1", "Period to get")
	startDate := flag.Int("start-date", 0, "Start date to get data")
	dataDir := flag.String("data-dir", "data", "Data directory")
	smart := flag.Bool("smart", false, "Data directory")
	flag.Parse()

	err, dp := period.PeriodFromString(*periodStr)
	chk(err)

	if len(flag.Args()) == 0 {
		return
	}

	err, api := network.CreateBizApi(HOST_ONLY)
	chk(err)
	defer api.Cleanup()
	api.SetWorkDir(*dataDir)

	for _, code := range flag.Args() {
		security, err := entity.ParseSecurity(code)
		if err != nil {
			fmt.Printf("[ERROR] bad security code %s\n", code)
			continue
		}

		if *smart {
			err = api.DownloadLatestPeriodHisData(security, dp)
		} else {
			err = api.DownloadPeriodHisData(security, dp,  uint32(*startDate), 0)
		}
		if err != nil {
			fmt.Printf("[ERROR] download %s data fail, error: %v\n", code, err)
		}
	}
}
