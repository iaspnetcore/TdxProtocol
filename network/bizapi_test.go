package network

import (
	"testing"
	"github.com/stephenlyu/tds/entity"
	"log"
	"fmt"
)

const HOST_ONLY = "125.39.80.98"

func chk(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func TestBizApi_GetInstantTransaction(t *testing.T) {
	security, _ := entity.ParseSecurity("000001.SZ")

	err, api := CreateBizApi(HOST_ONLY)
	chk(err)
	defer api.Cleanup()

	err, transcations := api.GetHistoryTransaction(security, 20181102, 0, 10)
	chk(err)

	for _, t := range transcations {
		fmt.Printf("%+v\n", &t)
	}
}
