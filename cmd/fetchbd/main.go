package main

import (
	"github.com/stephenlyu/TdxProtocol/network"
	"fmt"
	"time"
	"encoding/json"
	"io/ioutil"
	"flag"
	"github.com/stephenlyu/tds/entity"
	"strings"
)


const (
	HOST = "125.39.80.98"
)

func getInfoEx(api *network.BizApi) (error, map[string][]*network.InfoExItem) {
	err, codes := api.GetAStockCodes()
	if err != nil {
		return err, nil
	}
	securities := make([]*entity.Security, len(codes))
	for i, code := range codes {
		securities[i] = entity.ParseSecurityUnsafe(code)
	}

	return api.GetInfoEx(securities)
}

func tryGetInfoEx(host string) (error, map[string][]*network.InfoExItem) {
	err, api := network.CreateBizApi(host)
	if err != nil {
		panic(err)
	}
	defer api.Cleanup()

	return getInfoEx(api)
}

func saveFormat1(result map[string][]*network.InfoExItem, filePath string) {
	finalResult := map[string]interface{}{}

	for code, items := range result {
		security := entity.ParseSecurityUnsafe(code)
		market := strings.ToLower(security.GetExchange())

		if _, ok := finalResult[market]; !ok {
			finalResult[market] = map[string]interface{}{}
		}

		marketValues, _ := finalResult[market]

		infoEx := marketValues.(map[string]interface{})

		infoEx[code] = map[string]interface{}{
			"info": map[string]string{},
			"ex": items,
		}
	}

	bytes, _ := json.MarshalIndent(finalResult, "", "  ")
	err := ioutil.WriteFile(filePath, bytes, 0666)
	if err != nil {
		panic(err)
	}
}

func saveFormat2(result map[string][]*network.InfoExItem, filePath string) {
	infoEx := map[string][]*network.InfoExItem{}

	for code, items := range result {
		security := entity.ParseSecurityUnsafe(code)
		market := strings.ToLower(security.GetExchange())

		infoEx[fmt.Sprintf("%s%s", market, security.GetCode())] = items
	}

	bytes, _ := json.MarshalIndent(infoEx, "", "  ")
	err := ioutil.WriteFile(filePath, bytes, 0666)
	if err != nil {
		panic(err)
	}
}

func main() {
	host := flag.String("host", HOST, "服务器地址")
	filePath := flag.String("output", "./info_ex.json", "文件名")
	saveFormat := flag.Int("format", 1, "文件保存格式")
	flag.Parse()

	var err error
	var result map[string][]*network.InfoExItem
	for {
		err, result = tryGetInfoEx(*host)
		if err != nil {
			fmt.Println("try get info ex error", err)
			time.Sleep(time.Second)
			continue
		}
		break
	}

	switch *saveFormat {
	case 1:
		saveFormat1(result, *filePath)
	case 2:
		saveFormat2(result, *filePath)
	}
}
