package test

import (
	. "github.com/onsi/ginkgo"

	"github.com/stephenlyu/TdxProtocol/network"
	"fmt"
	"sort"
	"time"
	"github.com/stephenlyu/tds/period"
	"github.com/stephenlyu/tds/entity"
	"github.com/stephenlyu/tds/datasource/tdx"
	"os"
)

var _ = Describe("BizApiGetSZStockCodes", func () {
	It("test", func() {
		fmt.Println("test GetSZStockCodes...")

		err, api := network.CreateBizApi(HOST_ONLY)
		if err != nil {
			fmt.Println(err)
			return
		}
		defer api.Cleanup()

		start := time.Now().UnixNano()
		_, result := api.GetSHStockCodes()
		fmt.Println("got:", len(result), "time cost:", (time.Now().UnixNano() - start) / 1000000, "ms")
		sort.Strings(result)

		for _, c := range result {
			fmt.Println(c)
		}
	})
})

var _ = Describe("BizApiGetInfoEx", func () {
	It("test", func() {
		fmt.Println("test GetInfoEx...")
		err, api := network.CreateBizApi(HOST_ONLY)
		if err != nil {
			fmt.Println(err)
			return
		}
		defer api.Cleanup()

		_, codes := api.GetSZStockCodes()

		securities := make([]*entity.Security, len(codes))
		for i, code := range codes {
			securities[i] = entity.ParseSecurityUnsafe(code)
		}

		start := time.Now().UnixNano()
		_, result := api.GetInfoEx(securities)
		fmt.Println("got:", len(result), "time cost:", (time.Now().UnixNano() - start) / 1000000, "ms")
		for k, l := range result {
			fmt.Println(k)
			for _, t := range l {
				fmt.Println(t)
			}
		}
	})
})

var _ = Describe("BizApiGetInfoEx", func () {
	It("test", func() {
		fmt.Println("test GetInfoEx...")
		err, api := network.CreateBizApi(HOST_ONLY)
		if err != nil {
			fmt.Println(err)
			return
		}
		defer api.Cleanup()

		_, codes := api.GetSZStockCodes()

		securities := make([]*entity.Security, len(codes))
		for i, code := range codes {
			securities[i] = entity.ParseSecurityUnsafe(code)
		}

		start := time.Now().UnixNano()
		_, result := api.GetInfoEx(securities)
		fmt.Println("got:", len(result), "time cost:", (time.Now().UnixNano() - start) / 1000000, "ms")
		for k, l := range result {
			fmt.Println(k)
			for _, t := range l {
				fmt.Println(t)
			}
		}
	})
})

var _ = Describe("BizApiGetDayData", func () {
	It("test", func() {
		fmt.Println("test GetDayData...")
		err, api := network.CreateBizApi(HOST_ONLY)
		if err != nil {
			fmt.Println(err)
			return
		}
		defer api.Cleanup()

		start := time.Now().UnixNano()
		_, result := api.GetLatestDayData(entity.ParseSecurityUnsafe("600000.SH"), 500)
		fmt.Println("got:", len(result), "time cost:", (time.Now().UnixNano() - start) / 1000000, "ms")
		for _, t := range result {
			fmt.Println(t)
		}
	})
})

var _ = Describe("BizApiMinuteDataPerf", func () {
	It("test", func() {
		fmt.Println("test BizApiMinuteDataPerf...")
		err, api := network.CreateBizApi(HOST_ONLY)
		if err != nil {
			fmt.Println(err)
			return
		}
		defer api.Cleanup()

		api.SetTimeOut(1 * 1000)

		_, scodes := api.GetAStockCodes()
		sort.Strings(scodes)

		securities := make([]*entity.Security, len(scodes))
		for i, code := range scodes {
			securities[i] = entity.ParseSecurityUnsafe(code)
		}

		//codes = codes[:10]

		nThread := 10

		doneChans := make([]chan int, nThread)
		recordCh := make(chan map[string]interface{}, len(securities) + 1)

		count := (len(securities) + 4) / nThread
		start := time.Now().UnixNano()
		for i := 0; i < nThread; i++ {
			doneChans[i] = make(chan int)

			start := i * count
			end := (i + 1) * count
			if end > len(securities) {
				end = len(securities)
			}

			go func(securities []*entity.Security, doneCh chan int) {
				for _, security := range securities {
					_, result := api.GetLatestMinuteData(security, 0, 5)
					recordCh <- map[string]interface{}{"code": security.String(), "record": result}
				}
				doneCh <- 1
			}(securities[start:end], doneChans[i])
		}

		for i := 0; i < nThread; i++ {
			_ = <- doneChans[i]
			close(doneChans[i])
		}
		fmt.Println("time cost:", (time.Now().UnixNano() - start) / 1000000, "ms")

		recordCh <- map[string]interface{}{"code": ""}

		for {
			d := <- recordCh
			if d["code"] == "" {
				break
			}

			result, ok := d["record"].([]*network.Record)
			if ok {
				fmt.Println("stock: ", d["code"])
				for _, r := range result {
					fmt.Println(r.MinuteString())
				}
			}
		}

		close(recordCh)
	})
})

var _ = Describe("BizApiGetFile", func () {
	It("test", func() {
		fmt.Println("test GetFile...")
		err, api := network.CreateBizApi(HOST_ONLY)
		chk(err)
		defer api.Cleanup()

		start := time.Now().UnixNano()
		err = api.DownloadFile("bi/bigdata.zip", "tmp")
		chk(err)
		fmt.Println("got:", "time cost:", (time.Now().UnixNano() - start) / 1000000, "ms")
	})
})

var _ = Describe("BizApiGetNameData", func () {
	It("test", func() {
		fmt.Println("test get name data...")
		err, api := network.CreateBizApi(HOST_ONLY)
		chk(err)
		defer api.Cleanup()

		os.RemoveAll("tmp")
		start := time.Now().UnixNano()
		err = api.DownloadAStockNamesData("tmp")
		chk(err)
		fmt.Println("got:", "time cost:", (time.Now().UnixNano() - start) / 1000000, "ms")
	})
})

var _ = Describe("BizApiDownloadDayHisData", func () {
	It("test", func() {
		fmt.Println("test download day his data data...")
		err, api := network.CreateBizApi(HOST_ONLY)
		chk(err)
		defer api.Cleanup()

		security, _ := entity.ParseSecurity("999999.SH")
		_, dp := period.PeriodFromString("D1")

		os.RemoveAll("temp")

		api.SetWorkDir("temp")
		start := time.Now().UnixNano()
		err = api.DownloadPeriodHisData(security, dp,  0, 0)
		chk(err)
		fmt.Println("got:", "time cost:", (time.Now().UnixNano() - start) / 1000000, "ms")

		ds := tdxdatasource.NewDataSource("temp", true)
		err, records := ds.GetData(security, dp)
		chk(err)
		fmt.Printf("record count: %d\n", len(records))
	})
})

var _ = Describe("BizApiDownloadM5HisData", func () {
	It("test", func() {
		fmt.Println("test download 5 minute his data data...")
		err, api := network.CreateBizApi(HOST_ONLY)
		chk(err)
		defer api.Cleanup()

		security, _ := entity.ParseSecurity("000001.SZ")
		_, dp := period.PeriodFromString("M5")

		os.RemoveAll("temp")

		api.SetWorkDir("temp")
		start := time.Now().UnixNano()
		err = api.DownloadPeriodHisData(security, dp,  20170101, 0)
		chk(err)
		fmt.Println("got:", "time cost:", (time.Now().UnixNano() - start) / 1000000, "ms")

		ds := tdxdatasource.NewDataSource("temp", true)
		err, records := ds.GetData(security, dp)
		chk(err)
		fmt.Printf("record count: %d\n", len(records))
		fmt.Printf("%+v\n", records[0].String())
		fmt.Printf("%+v\n", records[len(records) - 1].String())
	})
})

var _ = Describe("BizApiDownloadM1HisData", func () {
	It("test", func() {
		fmt.Println("test download 1 minute his data data...")
		err, api := network.CreateBizApi(HOST_ONLY)
		chk(err)
		defer api.Cleanup()

		security, _ := entity.ParseSecurity("000001.SZ")
		_, dp := period.PeriodFromString("M1")

		os.RemoveAll("temp")

		api.SetWorkDir("temp")
		start := time.Now().UnixNano()
		err = api.DownloadPeriodHisData(security, dp,  20171101, 0)
		chk(err)
		fmt.Println("got:", "time cost:", (time.Now().UnixNano() - start) / 1000000, "ms")

		ds := tdxdatasource.NewDataSource("temp", true)
		err, records := ds.GetData(security, dp)
		chk(err)
		fmt.Printf("record count: %d\n", len(records))
		fmt.Printf("%+v\n", records[0].String())
		fmt.Printf("%+v\n", records[len(records) - 1].String())
	})
})