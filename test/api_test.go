package test

import (
	. "github.com/onsi/ginkgo"

	"github.com/stephenlyu/TdxProtocol/network"
	"fmt"
	"github.com/stephenlyu/tds/datasource/tdx"
	"github.com/stephenlyu/tds/period"
)


var _ = Describe("GetInfoEx", func () {
	It("test", func() {
		err, api := network.CreateAPI(HOST)
		if err != nil {
			fmt.Println(err)
			return
		}
		defer api.Cleanup()

		_, result := api.GetInfoEx([]string{"600000", "000001"})
		for k, l := range result {
			fmt.Println(k)
			for _, t := range l {
				fmt.Println(t)
			}
		}
	})
})

var _ = Describe("GetMinuteData", func () {
	It("test", func() {
		err, api := network.CreateAPI(HOST)
		if err != nil {
			fmt.Println(err)
			return
		}
		defer api.Cleanup()

		_, result := api.GetMinuteData("600000", 0, 10)
		for _, t := range result {
			fmt.Println(t)
		}
	})
})

var _ = Describe("GetPeriodHisData", func () {
	It("test", func() {
		err, api := network.CreateAPI(HOST)
		if err != nil {
			fmt.Println(err)
			return
		}
		defer api.Cleanup()

		_, data := api.GetPeriodHisData("600000", network.PERIOD_DAY, 20170703, 20171130)
		_, p := period.PeriodFromString("D1")

		var r tdxdatasource.TDXRecord
		for i := 0; i < len(data); i+=tdxdatasource.TDX_RECORD_SIZE {
			rb := data[i:i+tdxdatasource.TDX_RECORD_SIZE]
			tdxdatasource.TDXRecordFromBytes(p, rb, &r)
			fmt.Printf("%+v\n", r)
		}
	})
})
