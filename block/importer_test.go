package block

import (
	"testing"
	"os"
)


func TestExportBlock(t *testing.T) {
	os.MkdirAll("temp", 0777)
	ExportBlock("temp", "今日集合竞价涨停试盘", []string{"000001", "000002", "600000"})
}
