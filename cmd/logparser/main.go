package main

import (
	"flag"
	"fmt"
	"os"
	"errors"
	"bufio"
	"encoding/hex"
	"github.com/stephenlyu/TdxProtocol/network"
	"strings"
)

func chk(err error) {
	if err != nil {
		fmt.Printf("[Error] %s\n", err.Error())
		os.Exit(1)
	}
}

func parseBid(text string) {
	data, _ := hex.DecodeString(text)
	parser1 := network.NewRespParser(data)
	req := network.Header{
		Cmd: parser1.GetCmd(),
		SeqId: parser1.GetSeqId(),
	}

	parser := network.NewBidParser(&req, data)
	err, bids := parser.Parse()
	chk(err)

	for _, bid := range bids {
		fmt.Printf("%+v\n", bid)
	}
}

func parseLog(filePath string) {
	f, err := os.Open(filePath)
	chk(err)
	defer f.Close()

	scaner := bufio.NewScanner(f)
	for scaner.Scan() {
		line := strings.TrimSpace(scaner.Text())
		if line == "" {
			continue
		}
		fmt.Println("")
		fmt.Println(line)
		parseBid(line)
	}
}

func main() {
	logFilePath := flag.String("log-file", "", "日志文件路径")
	flag.Parse()

	if *logFilePath == "" {
		chk(errors.New("log-file required"))
	}


	parseLog(*logFilePath)
}
