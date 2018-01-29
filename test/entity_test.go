package test

import (
	. "github.com/onsi/ginkgo"

	"github.com/stephenlyu/TdxProtocol/network"
	"fmt"
	"bytes"
	"net"
	"encoding/hex"
	"sort"
	"time"
	"io/ioutil"
	"os"
	"path/filepath"
	"encoding/json"
	"strings"
	"github.com/stephenlyu/TdxProtocol/util"
	"github.com/stephenlyu/tds/datasource/tdx"
	"github.com/stephenlyu/tds/period"
	"github.com/stephenlyu/tds/entity"
)

const (
	HOST_ONLY = "125.39.80.98"
	HOST = "125.39.80.98:7709"
)

func chk(err error) {
	if err == nil {
		return
	}

	fmt.Println("error:", err)
	panic(err)
}

var _ = Describe("TestBidReq", func() {

	BuildBidBuffer := func () (*bytes.Buffer, *network.BidReq) {
		req := network.NewBidReq(1)
		req.AddCode(entity.ParseSecurityUnsafe("000001.SZ"))
		//req.AddCode(entity.ParseSecurityUnsafe("000002.SZ"))
		//req.AddCode(entity.ParseSecurityUnsafe("999999.SH"))
		buf := new(bytes.Buffer)
		req.Write(buf)
		return buf, req
	}

	It("test", func () {
		fmt.Println("TestBidReq...")
		buf, req := BuildBidBuffer()

		conn, err := net.Dial("tcp", HOST)
		chk(err)
		defer conn.Close()

		sendReq(conn, "0c0218940001030003000d0001")
		sendReq(conn, "0c031899000120002000db0fb3a4bdadd6a4c8af0000009a993141090000000000000000000000000003")

		fmt.Println(hex.EncodeToString(buf.Bytes()))
		_, err = conn.Write(buf.Bytes())
		chk(err)

		err, buffer := network.ReadResp(conn)
		chk(err)

		parser := network.NewBidParser(req, buffer)
		_, result := parser.Parse()
		parser.TryParse()
		fmt.Println(hex.EncodeToString(parser.Data))

		fmt.Println("total:", parser.Total, " got:", len(result))
		codes := []string{}
		for k, _ := range result {
			codes = append(codes, k)
		}
		sort.Strings(codes)

		for _, c := range codes {
			fmt.Printf("%s: %+v\n", c, result[c])
		}
	})
})

func BuildInfoExBuffer() (*bytes.Buffer, *network.InfoExReq) {
	req := network.NewInfoExReq(1)
	req.AddCode(entity.ParseSecurityUnsafe("000099.SZ"))
	fmt.Println(req)
	buf := new(bytes.Buffer)
	req.Write(buf)
	fmt.Println(buf.Bytes())
	return buf, req
}

var _ = Describe("TestInfoExReq", func() {
	It("test", func () {
		fmt.Println("TestInfoExReq...")
		buf, req := BuildInfoExBuffer()

		conn, err := net.Dial("tcp", HOST)
		chk(err)
		defer conn.Close()

		_, err = conn.Write(buf.Bytes())
		chk(err)

		err, buffer := network.ReadResp(conn)
		chk(err)

		parser := network.NewInfoExParser(req, buffer)
		_, result := parser.Parse()
		fmt.Println(hex.EncodeToString(parser.Data))

		for k, l := range result {
			fmt.Println(k)
			for _, t := range l {
				fmt.Println(t)
			}
		}
	})
})

func BuildFinanceBuffer() (*bytes.Buffer, *network.FinanceReq) {
	req := network.NewFinanceReq(1)
	req.AddCode(entity.ParseSecurityUnsafe("600000.SH"))
	req.AddCode(entity.ParseSecurityUnsafe("000001.SZ"))
	req.AddCode(entity.ParseSecurityUnsafe("000488.SZ"))
	buf := new(bytes.Buffer)
	req.Write(buf)
	fmt.Println(buf.Bytes())
	return buf, req
}

var _ = Describe("TestFinanceReq", func() {
	It("test", func () {
		fmt.Println("TestFinanceReq...")
		buf, req := BuildFinanceBuffer()

		conn, err := net.Dial("tcp", HOST)
		chk(err)
		defer conn.Close()

		_, err = conn.Write(buf.Bytes())
		chk(err)

		err, buffer := network.ReadResp(conn)
		chk(err)

		//parser := network.NewRespParser(buffer)
		//parser.TryParse()
		//fmt.Println(hex.EncodeToString(parser.Data))

		parser := network.NewFinanceParser(req, buffer)
		err, finances := parser.Parse()
		chk(err)

		for code, finance := range finances {
			bytes, _ := json.MarshalIndent(finance, "", "  ")
			fmt.Println(code, string(bytes))
		}
	})
})

func BuildInstantTransBuffer() (*bytes.Buffer, *network.InstantTransReq){
	req := network.NewInstantTransReq(1, entity.ParseSecurityUnsafe("600000.SH"), 0, 100)
	buf := new(bytes.Buffer)
	req.Write(buf)
	return buf, req
}

var _ = Describe("TestInstantTransReq", func() {
	It("test", func() {
		fmt.Println("TestInstantTransReq...")
		buf, req := BuildInstantTransBuffer()

		conn, err := net.Dial("tcp", HOST)
		chk(err)
		defer conn.Close()

		_, err = conn.Write(buf.Bytes())
		chk(err)

		err, buffer := network.ReadResp(conn)
		chk(err)

		parser := network.NewInstantTransParser(req, buffer)
		_, result := parser.Parse()
		//fmt.Println(hex.EncodeToString(parser.Data))

		fmt.Println("record count: ", len(result))
		for _, t := range result {
			fmt.Println(t)
		}
	})
})

func BuildHisTransBuffer() (*bytes.Buffer, *network.HisTransReq) {
	req := network.NewHisTransReq(1, 20170414, entity.ParseSecurityUnsafe("600000.SH"), 0, 100)
	buf := new(bytes.Buffer)
	req.Write(buf)
	return buf, req
}

var _ = Describe("TestHisTransReq", func() {
	It("test", func() {
		fmt.Println("TestHisTransReq...")
		buf, req := BuildHisTransBuffer()

		start := time.Now().UnixNano()
		conn, err := net.Dial("tcp", HOST)
		chk(err)
		defer conn.Close()

		_, err = conn.Write(buf.Bytes())
		chk(err)

		err, buffer := network.ReadResp(conn)
		chk(err)
		fmt.Println("time cost: ", time.Now().UnixNano() - start)

		parser := network.NewHisTransParser(req, buffer)
		_, result := parser.Parse()

		fmt.Println("record count: ", len(result))
		for _, t := range result {
			fmt.Println(t)
		}
	})
})

func BuildPeriodDataBuffer() (*bytes.Buffer, *network.PeriodDataReq) {
	req := network.NewPeriodDataReq(1, entity.ParseSecurityUnsafe("600000.SH"), network.PERIOD_DAY, 0, 0x118)
	buf := new(bytes.Buffer)
	req.Write(buf)
	return buf, req
}

var _ = Describe("TestPeriodDataReq", func() {
	It("test", func() {
		fmt.Println("TestPeriodDataReq...")
		buf, req := BuildPeriodDataBuffer()

		start := time.Now().UnixNano()
		conn, err := net.Dial("tcp", HOST)
		chk(err)
		defer conn.Close()

		_, err = conn.Write(buf.Bytes())
		chk(err)

		err, buffer := network.ReadResp(conn)
		chk(err)
		fmt.Println("time cost: ", time.Now().UnixNano() - start)

		parser := network.NewPeriodDataParser(req, buffer)
		_, result := parser.Parse()
		fmt.Println(hex.EncodeToString(parser.Data))

		fmt.Println("record count: ", len(result))
		for _, t := range result {
			fmt.Println(t)
		}
	})
})

func BuildPeriodHisDataBuffer() (*bytes.Buffer, *network.PeriodHisDataReq) {
	req := network.NewPeriodHisDataReq(1, entity.ParseSecurityUnsafe("600000.SH"), network.PERIOD_DAY, 20170703, 20170705)
	buf := new(bytes.Buffer)
	req.Write(buf)
	return buf, req
}

var _ = Describe("TestPeriodHisDataReq", func() {
	It("test", func() {
		fmt.Println("TestPeriodHisDataReq...")
		buf, req := BuildPeriodHisDataBuffer()

		util.DumpBytes(buf.Bytes())

		start := time.Now().UnixNano()
		conn, err := net.Dial("tcp", HOST)
		chk(err)
		defer conn.Close()

		_, err = conn.Write(buf.Bytes())
		chk(err)

		err, buffer := network.ReadResp(conn)
		chk(err)
		fmt.Println("time cost: ", time.Now().UnixNano() - start)

		parser := network.NewPeriodHisDataParser(req, buffer)
		_, data := parser.Parse()
		util.Assert(len(data) % tdxdatasource.TDX_RECORD_SIZE == 0, "")

		util.DumpBytes(data)

		_, p := period.PeriodFromString("D1")

		var r tdxdatasource.TDXRecord
		for i := 0; i < len(data); i+=tdxdatasource.TDX_RECORD_SIZE {
			rb := data[i:i+tdxdatasource.TDX_RECORD_SIZE]
			tdxdatasource.TDXRecordFromBytes(p, rb, &r)
			fmt.Printf("%+v\n", r)
		}
	})
})

func BuildGetFileLenBuffer(fileName string) (*bytes.Buffer, *network.GetFileLenReq) {
	req := network.NewGetFileLenReq(1, fileName)
	buf := new(bytes.Buffer)
	req.Write(buf)
	return buf, req
}

var _ = Describe("TestGetFileLenReq", func() {
	It("test", func() {
		fmt.Println("TestGetFileLenReq...")
		buf, req := BuildGetFileLenBuffer("zhb.zip")

		start := time.Now().UnixNano()
		conn, err := net.Dial("tcp", HOST)
		chk(err)
		defer conn.Close()

		_, err = conn.Write(buf.Bytes())
		chk(err)

		err, buffer := network.ReadResp(conn)
		chk(err)
		fmt.Println("time cost: ", time.Now().UnixNano() - start)

		parser := network.NewGetFileLenParser(req, buffer)
		_, length := parser.Parse()
		fmt.Println(hex.EncodeToString(parser.Data))

		fmt.Println("file length: ", length)
	})
})

func BuildGetFileDataBuffer(fileName string, offset uint32, length uint32) (*bytes.Buffer, *network.GetFileDataReq) {
	req := network.NewGetFileDataReq(1, fileName, offset, length)
	buf := new(bytes.Buffer)
	req.Write(buf)
	return buf, req
}

var _ = Describe("TestGetFileDataReq", func() {
	var getFileLen = func (fileName string) uint32 {
		buf, req := BuildGetFileLenBuffer(fileName)

		start := time.Now().UnixNano()
		conn, err := net.Dial("tcp", HOST)
		chk(err)
		defer conn.Close()

		_, err = conn.Write(buf.Bytes())
		chk(err)

		err, buffer := network.ReadResp(conn)
		chk(err)
		fmt.Println("time cost: ", time.Now().UnixNano() - start)

		parser := network.NewGetFileLenParser(req, buffer)
		_, length := parser.Parse()
		return length
	}

	It("test", func() {
		fmt.Println("TestGetFileDataReq...")
		fileName := "bi/bigdata.zip"

		length := getFileLen(fileName)

		start := time.Now().UnixNano()
		conn, err := net.Dial("tcp", HOST)
		chk(err)
		defer conn.Close()

		fileData := make([]byte, length)

		var offset uint32 = 0
		var count uint32 = 30000

		for offset < length {
			buf, req := BuildGetFileDataBuffer(fileName, offset, count)
			_, err = conn.Write(buf.Bytes())
			chk(err)

			err, buffer := network.ReadResp(conn)
			chk(err)
			fmt.Println("time cost: ", time.Now().UnixNano() - start)

			parser := network.NewGetFileDataParser(req, buffer)
			_, packetLength, data := parser.Parse()
			util.Assert(packetLength == uint32(len(data)), "")

			copy(fileData[offset:offset + packetLength], data[:])

			offset += count
		}

		filePath := filepath.Join("temp", fileName)
		os.MkdirAll(filepath.Dir(filePath), 0777)
		ioutil.WriteFile(filePath, fileData, 0666)
	})
})

var _ = Describe("TestNamesLenReq", func() {

	BuildNamesLenBuffer := func(block uint16) (*bytes.Buffer, *network.NamesLenReq) {
		req := network.NewNamesLenReq(1, block)
		buf := new(bytes.Buffer)
		req.Write(buf)
		return buf, req
	}

	It("test", func() {
		fmt.Println("TestNamesLenReq...")
		buf, req := BuildNamesLenBuffer(0)

		start := time.Now().UnixNano()
		conn, err := net.Dial("tcp", HOST)
		chk(err)
		defer conn.Close()

		_, err = conn.Write(buf.Bytes())
		chk(err)

		err, buffer := network.ReadResp(conn)
		chk(err)
		fmt.Println("time cost: ", time.Now().UnixNano() - start)

		parser := network.NewNamesLenParser(req, buffer)
		_, length := parser.Parse()
		fmt.Println(hex.EncodeToString(parser.Data))

		fmt.Println("length: ", length)
	})
})

func sendReq(conn net.Conn, reqHex string) {
	reqData, _ := hex.DecodeString(reqHex)

	util.DumpBytes(reqData)
	_, err := conn.Write(reqData)
	chk(err)

	err, buffer := network.ReadResp(conn)
	chk(err)

	parser := network.NewRespParser(buffer)
	parser.Parse()

	fmt.Printf("data len: %d\n", len(parser.Data))
	util.DumpBytes(parser.Data)
}


var _ = Describe("TestNamesReq", func () {

	BuildBuffer := func(seqId uint32, offset uint16) (*bytes.Buffer, *network.NamesReq) {
		req := network.NewNamesReq(seqId, 0, offset)
		buf := new(bytes.Buffer)
		req.Write(buf)
		return buf, req
	}

	It("test", func() {
		conn, err := net.Dial("tcp", HOST)
		chk(err)
		defer conn.Close()

		sendReq(conn, "0c0218940001030003000d0001")
		sendReq(conn, "0c031899000120002000db0fb3a4bdadd6a4c8af0000009a993141090000000000000000000000000003")

		var fileData []byte

		var offset uint32 = 0
		var count uint32 = 1000
		var seqId uint32 = 1

		for offset < 0x1c3b {
			buf, req := BuildBuffer(seqId, uint16(offset))
			_, err = conn.Write(buf.Bytes())
			chk(err)

			err, buffer := network.ReadResp(conn)
			chk(err)

			parser := network.NewNamesParser(req, buffer)
			_, packetLength, data := parser.Parse()
			fmt.Println("packetLength:", packetLength)

			fileData = append(fileData, data...)

			offset += count
		}

		filePath := filepath.Join("temp", "sz-names.dat")
		os.MkdirAll(filepath.Dir(filePath), 0777)
		ioutil.WriteFile(filePath, fileData, 0666)
	})
})

var _ = Describe("TestReqData", func () {
	It("test", func() {
		reqHex := "0c57086401011c001c002d050000333030313732040001000100180100000000000000000000"
		reqData, _ := hex.DecodeString(reqHex)

		conn, err := net.Dial("tcp", HOST)
		chk(err)
		defer conn.Close()

		_, err = conn.Write(reqData)
		chk(err)

		err, buffer := network.ReadResp(conn)
		chk(err)

		parser := network.NewRespParser(buffer)
		parser.Parse()

		fmt.Println(hex.EncodeToString(parser.Data))
	})
})

var _ = Describe("TestCmd06b9", func () {
	It("test", func() {
		reqHex := "0c5e186a00016e006e00b906b01e0400307500007a68622e7a6970000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
		reqData, _ := hex.DecodeString(reqHex)

		conn, err := net.Dial("tcp", HOST)
		chk(err)
		defer conn.Close()

		_, err = conn.Write(reqData)
		chk(err)

		err, buffer := network.ReadResp(conn)
		chk(err)

		parser := network.NewRespParser(buffer)
		parser.Parse()

		fmt.Printf("data len: %d\n", len(parser.Data))
		fmt.Println(hex.EncodeToString(parser.Data))
	})
})

var _ = Describe("TestCmd000d", func () {
	It("test", func() {
		reqHex := "0c333a87000114001400cd0f00003030323036377ac933017ac933010000"
		reqData, _ := hex.DecodeString(reqHex)

		fmt.Println("")
		util.DumpBytes(reqData)

		conn, err := net.Dial("tcp", HOST)
		chk(err)
		defer conn.Close()

		_, err = conn.Write(reqData)
		chk(err)

		err, buffer := network.ReadResp(conn)
		chk(err)

		parser := network.NewRespParser(buffer)
		parser.Parse()

		fmt.Printf("data len: %d\n", len(parser.Data))

		parser.TryParse()

		util.DumpBytes(parser.Data)
	})
})

var _ = Describe("TestCmd0450", func () {
	var sendReq = func (conn net.Conn, reqHex string) {
		reqData, _ := hex.DecodeString(reqHex)

		fmt.Println("\n========================================")

		util.DumpBytes(reqData)
		_, err := conn.Write(reqData)
		chk(err)

		err, buffer := network.ReadResp(conn)
		chk(err)

		parser := network.NewRespParser(buffer)
		parser.Parse()

		fmt.Printf("data len: %d\n", len(parser.Data))
		util.DumpBytes(parser.Data)
	}

	It("test", func() {
		conn, err := net.Dial("tcp", HOST)
		chk(err)
		defer conn.Close()

		//sendReq(conn, "0c01187b00011a011a010b0048e1d86490e8728cdd0188730982f4e1749933ae27700357749933ae27700357749933ae27700357749933ae27700357749933ae27700357749933ae27700357749933ae27700357749933ae27700357749933ae27700357749933ae27700357749933ae27700357749933ae27700357614dfecb8146951fbaf69b07b20bc100749933ae27700357749933ae27700357749933ae27700357749933ae27700357749933ae27700357749933ae2770035748e1d86490e8728cdd0188730982f4e1749933ae27700357749933ae27700357749933ae27700357749933ae27700357749933ae27700357749933ae27700357749933ae27700357749933ae27700357749933ae27700357749933ae27700357749933ae27700357")
		sendReq(conn, "0c0218940001030003000d0001")
		sendReq(conn, "0c031899000120002000db0fb3a4bdadd6a4c8af0000009a993141090000000000000000000000000003")
		//sendReq(conn, "0c1518950001020002000200")
		//sendReq(conn, "0c1618930001380038000a00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000") // 0a

		//sendReq(conn, "0c17186f000102000200de0f")
		sendReq(conn, "0c18186b0001080008004e0400007ac93301")
		//sendReq(conn, "0c30186d000106000600500401000000")
	})
})

var _ = Describe("TestParseData", func () {
	It("test", func() {
		reqHexes := []string{
			"b1cb74001c23186e001f5004671a4a71789c7d5ddb761bb792cd9af3365fc306d000f9348fe71ff23cdf316b7e65225bc792ad5822258b5749a42ea4c4ab444a8a1ddb712ccb3e761c47f1251725d61a74ed36b2126cc54f5edeab9ba88d42d54601d5fef73f0aa5a460f47f7f96a8dae5c94152fcccfff98fcf3efbdfffcafe2260fa09548518b4014c62d0795067608180a500ea084c9300a6316802188f364d03588a41fb094c6253d230da241e6d1a469bc4a3b561b4493c5aab02686330984288b7c1942436c50653c8acd8305a158fd6150268623098a262535c668ab9663e9d0920f94d1b40f25a17c09821570a60cc50b110c098a162f20924935dd4015431184c21935d0ca325f3592c06d0456029d047d64a290d603c2ba5401f99955218908a07540a0352d1809242a04f45f425858ca154b88d18f2ab378011437eb1043032c5fb660023533c7f0124a32d0530f204ffe713184f7692a80046c42749b033f6842409a624b129890b60447c920453624f4854218031f12a8c3676934485d1aa78b42acc4abcec1315888f7d2851c194d8871215885731f13a23de5ee3265a05301e90d6018cb9d56900e301691bc0985bed0248465b0c604cbc2e7d02890f996027711313ec8c034662829dc4878c09603c2b2690401ccc04128883996027713013ec8c5347921602187b5f1a4820de97061288f7a5c14ee27d69b093789fe46cc7934e2239db5de37d6931806440a500c6b3620b018c472bd9de5de39a92eddd35ae69750063d71429e0ae714d9102ee1ad7b4812112de6c6088f8ad0d0c11bfb58121e2b72e3044fcd6058688dfbac010f15b17188af550e20243c4a99d3034f88a4eb60b0c118f778121e2f12e30443cde058688c717034371424f8a8121b21c8a81a1bf2c87a4a00a85c27ffa9cbdbe52fe787290fddb3ffe042619b8f6e2d372f833a83270f09a8326039f3dd9bac95e9b66e0d649f79c8089cdc093a3e125035d06cedad56f1858ccc049e7d3cafe13a8c4cee98f1c143b37163263625006d4986fdd62a00ca87559e932b024a3dd3a7c4f402d03ead4e8935a4c193d3d99103015e2ab0794f8341badaddcad31faac3c595e98f419f8077df16bad9872daa4af75da83e9e072f433014bd980d27bb7d75b319888f7a5934eabc140e1f674cc0694c894a5fd6336202f793c58183f676ee2554df6dafddb9f56f69f4023031aed4cfe8f3c6932bf4d2b37980ff92c9881b376679d3c991641c2749180564c39a053e68371061ebedb7c404010df7a3b604f3a19ede8191dadb3018cb92dca6b1babcc359392809def1b643924a5ec37edf64fc37b9f47af5505d05766af55056168f80b1bad77a200c6af4d8c90706bfd0bf25aadc294c54f1a01d756869be44923af3d99d4be62a08c766361738f80a986275053522161af55bf224fca2af3b372b4434057c464afb1013979f2d9dd6cf9466051ecacbd63aea930d9abef8677188859b9c516922aa9302bd142527013014d0c0ab783d7939dcf63861030667b3b0f3fff2c066540bd51f7221e909674e5e96379458bf7d9fe77d5af3f8f46ab1359f6f539e69a3a5100eb2482e94463656ffdc440196db61c182853d63b3e3e26a044b0f4e89c0e483212b84d63d005d0c660312c8798044957b6f33d9b6cadc56fcb1fb74e0888c07838a24f4abab2f5331f13a2c9d679bae2c4a7992976bab5b1c4c012e6b3c53c41121de250fc9b563c617ade66f36985bec6fcc941eb5604baccc1ecdef156933ce932a7b6ed5f77070c94df1c9d378f0828ebd356a7d44d647dda9d2b4a4251e85bbbc19f047daf28b7a5cce3edc3568f44135d123b87df1c525046bbd762395b23013417f96f16e5b557f53906c2fb2a93dd1834a2136cedecd57e3c9f062bfb70748fcc8a91d4e1279b454d238bd7561f33fa4c22afed7ddc26aac6884ab5cd932191764696bddd7bcf1832b2",
			"ec3dc8b2bd1195ea47bb49748211c963bb4f983631a26a6ce3117ff28fd817d327d1c4ff666b8981e243e54a7d8e81c50046c9d564d1e49f89dd58f0dbab0814f16bb9c2305aecec9cb35c6624a1dbc1ed49870c48cb7cd6e79814305a186a5d6ed7d993c2d0ea6efd8a8116dceefec05e2b0c1d7a68782f06e1f1bfb1c56b0c4878c48294116d028f8fb93562e7f8f9803e9986f9244f5a7802d343c638841aba562079246793274b615662302d80783ad96992b9896b5f323741eaa8fc0f13bf4664966ddd5a2e13125299ec9d71ed217b3205b7ed570cb461ad10531c9eec11816644e4dbe6f601d9b419a42b99cf28f31a0b866e315168ac3064b38c1edb9925ba1c8c476b85a1674fa8f759616863817a1f52e45a87292993a5c87ccac86f662438be4732a26fddc1e93a11dcc615c26b638624f3bafef109f304a7047c3c2379c5c81ec9adf6c62c304ace76d7c478c9d9feb534a4ca06ca9d3c587ec3c0cc4ddc83df7bef18280cf13daf111d6fcf6ff72ec8ac140bf2da9774211585a16c3e19286ee2ca79cef90b280cadccf3d70a43a3071c141286574cf719d93bb82cbc31503ca1fd2b2541b4893b9cd20420dac4d59edcbbcd40f184c3114daeb2eb70d56faa5f33301586b2741533245b12b7326e11ef4b65d7e186bf0dc92a4ba14d5eeeb2259816e4b5873f30074ba5fae1a65b8cf8345735072cd1a5b29971276fe89322795ced8c31948a7071df52219a427e64bf19bb662afb1537deae910a5aaae437f7deafae125012baabbfa6bf2939dbd59fae7c60a0f8d0f4d53ed1f1a9f9239a44f3994a8af4f35950e44949576ef9f742c2400b37a9b2d74a76f0dcb2724c2a09c0cdda4c49a5920084be38e9a412c65da3427d4822b56bacf6bf24601e52cb97cc4e84d4d3219d1584d4e5df87a4649022bc9dff8bd5355384b7bdf7d4fb10a466fd21291ea545ccca3c5d0e4591932eab5845c45bac95fe312bc758787c637567cc40cce7a775f46750d4b8abaeef90b46c4514fad72e97192803dade67245803c9d33863bf09d79cbe6202cd8a367107156aa7ec5c3d7dcca92d267b7276f29880b2fff4a69489dfda127c68ed0619ad2b08b7fbdd514c82c6d1825a7f4356b6c6d1827a764118d2385a50dbfb24af7830fb4d75f29278bcc6b9833a1cf1d766dcaaad9bc4e33d9871abf61ff227336ed5e8e7ec282406338f57b5cbe5385de9820463d59c27f1d68342c2e823994f0f8a292757a7f176cf8329c0da0d068a2995b9353a2031e5a8cd0724a6943f9250e3c12c82a9f1f3e34302ca5a516de6f11ae72b6a749be46c0fca64ef7f450a391e94c9aefcce5f2b0c759f5007933dafaa7cc59f1486566fd5ce18280cf52e2843b2e75534b96a1cf8a8e98f14943dafdab9e2a03034bb4952870785a1a33e3981f2a030d43fe6af1512ce2e28b712c1d431d3261e1412b25d24038584d93d124d3c58fa9b2765cfeb57d95a1c8c3d2824f41fd3952d1b6275ef43679b8142c2fe9b76ac873c286ed25e1c7fc74061a839a224c886587597eaf146d18342c25e8bba89880835e5d124153bbbaf6978933daf0f5224187b50ec9c2e920db10711305ecd5834913dafea7dc57f533c81d6a43c2824ec77b92932d9edfb141455a34e5975d28342c2ee73fa9b52bf55cd56bfc2408d785bd00c343968182824b4de1269e7419b8371fed40527a64c3ad44e49ae6a7a93da294a4a752f6b2cf6c9fed32f410eca681bab74d93b8cf682940c3c2853f672973f89c5bbc74d91f95c99a3b322fb4f1f6a48fdd6834242e5c6b8c74090f084bf56e6b3c10724fb4fd5986fdf6720189aa7e14d36a73e5253126473aaeedd1e505048a84e8910d585129cfa476a4a099e704e0ed83d9847b0067dad90b0c96e19785048b8f892ec963d2824b47fe5a3b5f021ca6d49dc64659efaad14ebd5ea0f1c1486d63a6cb438d457f43cdb8309b86571",
			"2881ee5bfb76fc928119437a7887a9b704ba8f9e2d7b5048a82faecd3350ec1c3d65092011f5a677ae4825c283b9c7d3df9443433dbb7940325292208c9f57e243430f8a29abefd6bf65a0c5932b2b0c84b4ebf301899de333e69a09a45d637d40e2500269d75b646b05f730d4ee80657bdcc3d0d57fd10189b4d33b630e664ead471d3a2b506fed5ff8930ed1847a1fd4dbf83d7dad0834bddf612a351181a697cbf4b5d0603b572bccc1a486a17bec48cc8362e7dab72c012422d0f4e0367fd209b7535202f260f16f5c5334982e9fd2c52b1a4c3717b77e63a0cce7f4bc175f16f3a0cce78015203d0812a81ac7bd1a0f52d794130b4dcf743c28245c7c49a74c049a6e5dd49e3030f3787dfc9669cd24c5b21fd3df14f5a62b37e86845bdf980d121913a4911c1d81d170f9a00fe75e3ef412c8726a54fd49bae7e415d530e25f4e8236548d49be67b5e5c4bd2bd0bca900834dddca6615c049a9fcf517c4ae241b1b3758bffa6d8597f498e5034ae42e9d61229067ad0612151ef9343096f27b94ee74121a1b1b81e9f75e844749f1e6f4fa60ccc3d81fea6e83e4d6f36685cdcf2a166ca0624a250d333748d5b5d9a5e6dd3b8d5a547cfc8019e0785a1d103fea430d4bccb7f5318aa9dcde242ab4e4414eaf7df510713dda7376e6fb20189eed3adb76c139e88eed3f438c3837093395298f3a0431cda8c0f5f3c28769e3ea6768aeed3abefd8163311dda7d7df9032a207c513ea4dcaade83efde003fd4dd17d7ab4c3412161e503dbafe0ae5d0692fd4a2202cd93c014a31281a64f676cb44a049a8f7d1c1453f65a03322b0a02edd94a95fe2696fd5b66276e06eae91326ed94a8373d7e39a3a04cf6f290bf16b1ef67e6430ad28edefdf0a090503de0a090704497a01269a7e9c9a907c5ce8d5d16a915a4dda45326e2572558bc3ff3df143b7b1fab645fa644dae9de7d267994483bddbadc898fc93d985c1f4d14a4ddc642bbca4038f517e464c683e209f486b20765b2b77f62c95589b4d3d9a28ff3a712690730ca9f4a0a73de4e26b3140a735c7e28e8bef67db6e7c5ed52cd37c40aba6ff73d1d2d745ff7bc4ef28a4261aef33d9d3291767a78abbdcf40b1f36bbadd53d07df531ab192be8bec301db45aa5cf72dd20141f755e6a86b42f7addfa17108baafde9c31e2a1fb9e3de1a6382474feda225cb3fa6f06e6319ebe16baafff98296305dd472fa8795018aa2fd20141f74d3a63727aa0a0fb2a7307f1c9a9072d1caccc629f14e67c166cc537773d282474ead4fba0fb048cd78a0509af597255d07d95ab497c86ee418db5425776aefbd82d200fc2135ed3e500dd57bb62425441f7b52e06a452a8a0fba6e72c67e3b2b50757894e50d07dd3f3e15b0626f8cde153062226bc588f6f7a7a50ec6c5f5649fd5641bdb5de327daba0de78550057c3fd68fb2cbc41bdd5e7b6d8e2857a1bdea22b5b4a7afae02e4d00506fa367ac34a2a0debadf1f326ea1de8677b6eb0cccb77be4b2b5073f4d361d90d8b9f5137530a8b7ca0d723fc18362e77d7a16895beed7c521a8b7dd85f6af0c3458bcacdea7a0de76aed8810faec0eb832f58514549d54e0f17683491aa9d5f82ebccc1200a473fb3c9d61085b33d267970b35ed3bbea1e94aa5ded82c53e0d51381ef448fd564314d6c7ab24dee2c2be1e7ec3a2898628bcf78155223474df76931ddbe80296c377bdf8b6b0461f80df7fb250a3210aebdf8d9f33107a68999db9a249c07b02b9fded4121e1847a8286627cb542b915c5e8f3ca09911f1a8ad16f6b490d038d097eefc09c1a8d09bafc91120f513863d79234ba16ae29e96988c2c1eb9311038584e5d3a35f1808d9bcb04f0724249cccb19c8d66087d3261e7f66886f051b34e74029a2154e311250195c2f62553521aa270fcfcf84706a204b4441982622c2ff0dfccf7bc1cb43085456a8d4ae17089320439b9fd13",
			"8d093a4f9174b49093c7bb2bccfb2027879d75a24d34e4647789a5650d3979f89ec53e349a784f60a9434331f65e52863e89c21a399ed21085d53956c3d069be47a2bf0951b8d93f79c4c07cb7dcae3150267b7fc0349846bd6ff888120fddd77a4b6ed17a50ec5ca659103d339a5e77f52e8414b9bfcc02068a81b4e14da3db46778f9970d1d07dedfb47f4b529420db928eb4199cfc619933ce8d3d1a38fd4ce5cbd3d62da044d3c6665a542148646616eaf453a81353a7c74ff8a3a18a4ddbd0fb50506e6eb7314771078d001fc9e255748bbf1f30db2ebd0506fe587e76c40d06087bb34011473217ac8325231cf2b47070c94d11e6f3688e4d128aff5e8f52b0d81767a40932b045afd57b65fd17979ed57ba5620d036e6b619431068ed3bec22818640a3576c353a99f4e1371c8454e7c91502ed6b5ae0400f943e614d591a3d50bad2605b69f4405d23f20d045ab3c56aa90655bb357627df833a8011b70602ad5169dc65af4501729e654153c8abb02c301a08b42115dce8d8f29b700a42a0d19e530f2601fc6bcfa90715f60eec600b8d60fa805e634123985fd994bec40630a60f1aacb544e7131a6cba450784c21cbdc8aed108e6a5000b6f061aacf282c578030d76d46fc4d77a355ac874fb179605d142a6f96d27b490e9ca88dcacd76821cb2a68646f8f16323d6b6f923b850655bbe10d56b5430b99a974c925648d1632bd774ca72c1768e74c0fa1854c35e6cf48790d2d649962dc6220041a3db6410b999f3276b48016329fd037c82d5ab490e9a315fe5a61a84da326facbfc16936d490c04dae653fea430443f1da0d17ce6d51b074d1850bc1ca0de060b15c62dd45b7375874d36d45b56a961bf89a839c7e4079acff4c943fa24d4dbf9bfa82740bd55bf648a11fd653a2bf2b3d78a9db3166948d5e82fd3b4275ca3854c1fbfe5af45607c4e41a8b7c93607138c76c67e13eaadbcc79fcccfe8d82564b490e9a353fea490d0b8e6b542c20b9a5c4d5ed2e3f11625bd4c1232103afec77b2ca4421466f56df2244a7a2f387d28e9d5e7c8f74d345ac84ca3c29f946faa1c758fe993f24d956b4c116967b250ccc024b849bccae4ccd56c2cb0b293f47a154a7ca36844149afe3169fbf6a07c8d65ff6b3a65c5fc7b35ec0805bd5ea6f7809a22bacf1c701f12dd67aaac034fa3d7cbb496c6e4968111dd678edaac888d4630d3bdc57f534878b5ff2a6ee3d7680483f7c5c48bee33873f50b124bacf641715e3df4c21ede4a8287a2dfacb3c096c56d05f669a237661025d62266b132660f2c772887f33c16fd21b90682133c32596cb52a966197e1b31152565667be443241e94ef2cf51699584af1e127596564b4c510fb082824345629f1f82ad4e0f629d9e9a0e12d1b105135687833ed3b7b64d3868637b349a5400a9925f9331eadc82cb332a6f4e14b54a78ff7c8ce35c597a8a60ff749d929c597a8f815be144a2a3bc3240c899202188f56fff1252af2a490b0b1c72a4b68ec33d373762327152565b69becf838152565b62a942151527ed997499922c547b53af47054fa05bd9df5ca1a0385047e492315b1643a2d4a828825d3be4f7dc820ded2189f8a1e323cd4a472fee96785349f7950ec9ccdb10b4da9882533e930b1948a1e32fc6a5b8acf8ef55eb0cbd6a91c719ae6216548f49099d22314e9a8cc40ea26287589c2884910b164babb943e114b86cb66b45b424ec6af15b164d61fb09bf529bea0566dcc487640a3a6692eb24a7e2a62c94cfa74ca442c99ee6b3e5a6168708f7cafc68379d2a1cb019f743b79c5366d293ee9c63b5fa473344fcb3143a8a055bf61fb4f748e9afe6b76a6933a2c075a764aa1a4760fe868a5bc66f6bb4c36a7525e4bd6bea5c44b79cd5c7cc94a23e85635dd275ba43e944283355629b7d060a26f6386a0c1b67a945b29cce1c389b1df4283ed8c87e4202495c21c420df94d",
			"b1b3bace5f2b7636b7991e4a21d0ea732d461f0a73b2d3897fb394078c619b3d89af0dd27ba92904da98c7040834de2f9842a055bf205feff0a0c580a6cb0c747f93e84a39436c40165fc9acd1fda785401bd0fe158baf64f23bbfb6a00318716b21ed46ec73961e143beb07335238473bb469bd65551e2bb537b3f1ac470a73168a91778e5a518c5ebdb1556673c5b84e4d91da9be18556f467fb29639b700b3959bbd82555750b39b9759329460b39c9ab7616729257ed2cbe233a6bb3b30e9b7f47f4bc4b56b685629cfcc63cde4231768f592793856294eff2c4a3c5172bb34e1206e6470bd4944c14164add27ac0a6ba5f666f6e805350bc5d83f66d9de42316e3ea5f309c5f8ec0d8b9a168ab13e1e900460a1184f87743ea11877d628b71085ebdcc1200a65ff1933045138d96565442be535c3db672d44616b8985540bddd76c5153a0fbdaf44aa685eea3df7bf3a098d2995f3b65a0985269d0c986eea39fa4d11645b232cd6516a2f0f00d5d82108547cfd9fd040b51d8a7b7ba6c8a4df86b6a272a682fe88ecee68a718b7a822846c39b9a2d14638d9e3b5828c6bd167d5214a3192e74c8d995b5f96eb94f2a11d6e6e518265c2c14e3904fb6cd6533e5d6e6db20b647c2a720fce29db1250851c85b8ef09d0873bcc9a49d85b49bbca62b1bd2ae71b1416ec75848bbc102dbee5948bbca0ba618ad483bf32d3dd4b79076f4e3441e2c6221f1d1627bd0a7c4e358554e0f62ef2b22c6d3e66d8b4b71526d264f6207407b886d318f09a743060a09e58f6ce36fa1fb76c6acec64a1fbba741b64a1fbe80722b545618e5f9db19076cd11f52148bb496793710b69b7d9a7cb01d2ae7646fd16d24e2aa231b790769defb92942c2fa1bca5009f5a133f2491aeda0fb761eb0cb280ebaaf7d9f399883eeab3f6587ddf86c8ae97dd5215575873357fa69660f4a6094631b020a43cb43b6561c44e1949e5838e8bee572939902dd37e9d3d742f78ddfb138e4927c07c0569983eeeb2e3139e920edfa3bcce35d92a7483e2094a21fb0db310eea4d3e681f9300f5d67dc26ef33ba8b7e5df59ddc441bdedd1faad837aab362843a8f7752f37481b8543bd6fadc3428dcbeb7de7ecfa95837adb6ab230eef293537a1dc041bd55ee3235eea0dea6e774403a2f7ab23b110e27a772761533847a5f7dcc7f534868ffd266930d69377dc452a483b45b2ef3010943a39f7be4a8c8a118d8a53529877a5f8d168f1c74df116d4d7672386a78f7b183281c3d63f521075158a7edb30ea27070bb1c7f9ad98342c270e9608381280b1fd0d78a28f4297295dc7b73e9df6441075138da61b2d94114660d81c4fbd2dc13d895120769978d953d89edfb22a50fd26e48dbf81da41daf4e3a48bbfd3747a42ceca0deb66e52ef43bdaf3c60777e1da4dd075aab7190761bcb75d208e6a0def8d73b1c4a7aed79fa5a94f4048cfd16baeffe8f740942da8dd8ff0ce141b193f7233948bb49674a5ab01da45d67c0ce221da4ddf65dca2da45d79b0c618caa5dd2f342d43bdf1ddb243d58e7fdac389b433fb0f774925df41bdf17db6837ae35d620eea8db4e7fd3f8a85cfa6",
		}
		buffer, _ := hex.DecodeString(strings.Join(reqHexes, ""))
		parser := network.NewRespParser(buffer)
		parser.Parse()

		fmt.Printf("data len: %d\n", len(parser.Data))
		fmt.Println(hex.EncodeToString(parser.Data))
	})
})

var _ = Describe("TestTryParse", func () {
	It("test", func() {
		strHex := "b1cb74001cc98e00006226050502fa06789cd3b5b4e4e4e4e6e5e2b2b4b45c6402247c622c2ce1a05167ed04ab45268b4cacad2d2dcd2d4904608379818c6d6640a2351ec9e0bbeb2ce6afb6d962b6c54c4f0fc89d6181c308bc46f300193b558184681c92fe978c1b1fdb4f579daecacf0fe4c992633048e334437483d78aec9c6533d570aae15a8bb5406115124d8618cd0764cc320612d925c87e9effcb9a54e3904d650432fa8c304ced7d624889a90c40c62e694bb46038b9c16af131c3bdd27ba56d6c80dc562b328ce6e50432a6a801092964a35bffdb2c3d693d576dae9ab0b0a5e53e3b725ccdcb01642c0125b87dc829f9eb4f8b99ff6d979a2d35535202729bc84970bcdc40c6162d4bf460de3f4d8f5497229b0aca7a6da0ac8712ccbd92f3f71bb499b499686b5b5aea90653028ebadd305121ac806cf34dd7ad37aa9ee52dde5d6cb81898ee41402361a94f536e903094364a3ef5acc9c67bf5e7fbdbe81019047561083b2de7a5049538a5c5cec35df3ac766bdf97a732360223720cbc5a08cb70da41525945ba556b65b2d345868d0a9d1a9616949723c828d06e5be152037a304c6d282fdfbac5699af32b702e61031b20c0665c0950ae86e7e283df5a4ed6e85dd0a5b2cb68084f5c9309a079401674802096594b49137f5a3ed0cc919929b4d379b5a5a3292e36a1e50069c2a62899e4b66cfee253545a0180bca7cd30c308c5d3891d402080a00f366aed5"
		buffer, _ := hex.DecodeString(strHex)

		parser := network.NewRespParser(buffer)

		decrypt := func () {
			for i, b := range parser.Data {
				parser.Data[i] = b ^ 57
			}
		}

		parser.Parse()
		decrypt()

		parser.TryParse()

		util.DumpBytes(parser.Data)
	})
})

var _ = Describe("TestTryParseBid", func () {
	It("test", func() {
		strHex := "b1cb74001cc98e00006226050502fa06789cd3b5b4e4e4e4e6e5e2b2b4b45c6402247c622c2ce1a05167ed04ab45268b4cacad2d2dcd2d4904608379818c6d6640a2351ec9e0bbeb2ce6afb6d962b6c54c4f0fc89d6181c308bc46f300193b558184681c92fe978c1b1fdb4f579daecacf0fe4c992633048e334437483d78aec9c6533d570aae15a8bb5406115124d8618cd0764cc320612d925c87e9effcb9a54e3904d650432fa8c304ced7d624889a90c40c62e694bb46038b9c16af131c3bdd27ba56d6c80dc562b328ce6e50432a6a801092964a35bffdb2c3d693d576dae9ab0b0a5e53e3b725ccdcb01642c0125b87dc829f9eb4f8b99ff6d979a2d35535202729bc84970bcdc40c6162d4bf460de3f4d8f5497229b0aca7a6da0ac8712ccbd92f3f71bb499b499686b5b5aea90653028ebadd305121ac806cf34dd7ad37aa9ee52dde5d6cb81898ee41402361a94f536e903094364a3ef5acc9c67bf5e7fbdbe81019047561083b2de7a5049538a5c5cec35df3ac766bdf97a732360223720cbc5a08cb70da41525945ba556b65b2d345868d0a9d1a9616949723c828d06e5be152037a304c6d282fdfbac5699af32b702e61031b20c0665c0950ae86e7e283df5a4ed6e85dd0a5b2cb68084f5c9309a079401674802096594b49137f5a3ed0cc919929b4d379b5a5a3292e36a1e50069c2a62899e4b66cfee253545a0180bca7cd30c308c5d3891d402080a00f366aed5"

		data, _ := hex.DecodeString(strHex)
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
	})
})
