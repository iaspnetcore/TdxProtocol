package network

import (
	"encoding/binary"
	"math"
	"errors"
	"compress/zlib"
	"bytes"
	"io"
	"net"
	"fmt"
	"github.com/z-ray/log"
	"encoding/hex"
	"reflect"
	"strings"
	"github.com/stephenlyu/tds/entity"
	"github.com/stephenlyu/tds/datasource/tdx"
)

const (
	BS_BUY = 0
	BS_SELL = 1
)

const (
	STOCK_CODE_LEN = 6
	RESP_HEADER_LEN = 16
)

type Transaction struct {
	Date uint32
	Minute uint16
	Price uint32
	Volume uint32
	Count uint32
	BS byte
}

type InfoExItem struct {
	Date uint32					`json:"date"`
	Bonus float32				`json:"bonus"`
	DeliveredShares float32		`json:"delivered_shares"`
	RationedSharePrice float32	`json:"rationed_share_price"`
	RationedShares float32		`json:"rationed_shares"`
}

type Finance struct {
	BShares float32				`json:"bShares"`
	HShares float32				`json:"hShares"`
	ProfitPerShare float32		`json:"profitPerShare"`
	TotalAssets float32			`json:"totalAssets"`
	CurrentAssets float32 		`json:"currentAssets"`
	FixedAssets float32 		`json:"fixedAssets"`
	IntangibleAssets float32 	`json:"intangibleAssets"`
	ShareHolders float32 		`json:"shareHolders"`
	CurrentLiability float32 	`json:"currentLiability"`
	MinorShareRights float32 	`json:"minorShareRights"`
	PublicReserveFunds float32 	`json:"publicReserveFunds"`
	NetAssets float32 			`json:"netAssets"`
	OperatingIncome float32 	`json:"operatingIncome"`
	OperatingCost float32 		`json:"operatingCost"`
	Receivables float32 		`json:"receivables"`
	OperationProfit float32 	`json:"operatingProfit"`
	InvestProfit float32 		`json:"investProfit"`
	OperatingCash float32 		`json:"operatingCash"`
	TotalCash float32 			`json:"totalCash"`
	Inventory float32 			`json:"inventory"`
	TotalProfit float32 		`json:"totalProfit"`
	NOPAT float32 				`json:"nopat"`				// 税后利润
	NetProfit float32 			`json:"netProfit"`
	UndistributedProfit float32 `json:"undistributedProfit"`
	NetAdjustedAssets float32 	`json:"netAdjustedAssets"`		// 调整后净资
}

func (this *Finance) String() string {
	value := reflect.ValueOf(this).Elem()
	t := reflect.TypeOf(this).Elem()
	lines := make([]string, t.NumField() + 2)
	lines[0] = "{"
	lines[len(lines) - 1] = "}"
	for i := 0; i < value.NumField(); i++ {
		f := value.Field(i)
		v := f.Float()
		name := t.Field(i).Name
		lines[i+1] = fmt.Sprintf("%20s: %.02f", name, v)
	}
	return strings.Join(lines, "\n")
}

type Bid struct {
	StockCode string
	Close uint32
	YesterdayClose uint32
	Open uint32
	High uint32
	Low uint32

	Vol uint32
	Amount float32
	InnerVol uint32
	OuterVol uint32

	BuyPrice1 uint32
	SellPrice1 uint32
	BuyVol1 uint32
	SellVol1 uint32

	BuyPrice2 uint32
	SellPrice2 uint32
	BuyVol2 uint32
	SellVol2 uint32

	BuyPrice3 uint32
	SellPrice3 uint32
	BuyVol3 uint32
	SellVol3 uint32

	BuyPrice4 uint32
	SellPrice4 uint32
	BuyVol4 uint32
	SellVol4 uint32

	BuyPrice5 uint32
	SellPrice5 uint32
	BuyVol5 uint32
	SellVol5 uint32
}

type RespParser struct {
	RawBuffer []byte
	Current int
	Data []byte
}

type InstantTransParser struct {
	RespParser
	Req Request
}

type HisTransParser struct {
	RespParser
	Req Request
}

type InfoExParser struct {
	RespParser
	Req Request
}

type NamesParser struct {
	RespParser
	Req Request
}

type NamesLenParser struct {
	RespParser
	Req Request
}

type FinanceParser struct {
	RespParser
	Req Request
}

type BidParser struct {
	RespParser
	Req Request
	Total uint16
}

type PeriodDataParser struct {
	RespParser
	Req Request
}

type PeriodHisDataParser struct {
	RespParser
	Req Request
}

type GetFileLenParser struct {
	RespParser
	Req Request
}

type GetFileDataParser struct {
	RespParser
	Req Request
}

func (this *RespParser) GetCmd() uint16 {
	return binary.LittleEndian.Uint16(this.RawBuffer[10:12])
}

func (this *RespParser) getHeaderLen() int {
	return RESP_HEADER_LEN
}

func (this *RespParser) getLen() uint16 {
	return binary.LittleEndian.Uint16(this.RawBuffer[12:14])
}

func (this *RespParser) getLen1() uint16 {
	return binary.LittleEndian.Uint16(this.RawBuffer[14:16])
}

func (this *RespParser) GetSeqId() uint32 {
	return binary.LittleEndian.Uint32(this.RawBuffer[5:9])
}

func (this *RespParser) skipByte(count int) {
	this.Current += count
}

func (this *RespParser) skipData(count int) {
	for count >= 0 {
		if this.Data[this.Current] < 0x80 {
			this.skipByte(1)
		} else if this.Data[this.Current + 1] < 0x80 {
			this.skipByte(2)
		} else {
			this.skipByte(3)
		}

		count--
	}
}

func (this *RespParser) getByte() byte {
	ret := this.Data[this.Current]
	this.Current++
	return ret
}

func (this *RespParser) getUint16() uint16 {
	ret := binary.LittleEndian.Uint16(this.Data[this.Current:this.Current + 2])
	this.Current += 2
	return ret
}

func (this *RespParser) getUint32() uint32 {
	ret := binary.LittleEndian.Uint32(this.Data[this.Current:this.Current + 4])
	this.Current += 4
	return ret
}

func (this *RespParser) getFloat32() float32 {
	bits := binary.LittleEndian.Uint32(this.Data[this.Current:this.Current + 4])
	ret := math.Float32frombits(bits)
	this.Current += 4
	return ret
}

func (this *RespParser) parseData() int {
	v := this.Data[this.Current]
	if v >= 0x40 && v < 0x80 || v >= 0xc0 {
		return 0x40 - this.parseData2()
	} else {
		return this.parseData2()
	}
}

func (this *RespParser) parseData2() int {
	 //8f ff ff ff 1f == -49
	 //bd ff ff ff 1f == -3
	 //b0 fe ff ff 1f == -80
	 //8c 01		 == 76
	 //a8 fb b6 01 == 1017 万
	 //a3 8e 11    == 14.02 万
	 //82 27         == 2498
	var v int
	var nBytes int = 0
	for this.Data[this.Current + nBytes] >= 0x80 {
		nBytes++
	}
	nBytes++

	switch(nBytes){
	case 1:
		v = int(this.Data[this.Current])
	case 2:
		v = int(this.Data[this.Current+1]) * 0x40 + int(this.Data[this.Current]) - 0x80;
	case 3:
		v = (int(this.Data[this.Current+2]) * 0x80 + int(this.Data[this.Current+1]) - 0x80) * 0x40 + int(this.Data[this.Current]) - 0x80;
	case 4:
		v = ((int(this.Data[this.Current+3]) * 0x80 + int(this.Data[this.Current+2]) - 0x80) * 0x80 + int(this.Data[this.Current+1] - 0x80)) * 0x40 + int(this.Data[this.Current]) - 0x80;
	case 5:
		// over flow, positive to negative
		v = (((int(this.Data[this.Current+4]) * 0x80 + int(this.Data[this.Current+3]) - 0x80) * 0x80 + int(this.Data[this.Current+2]) - 0x80) * 0x80 + int(this.Data[this.Current+1]) - 0x80)* 0x40 + int(this.Data[this.Current]) - 0x80;
	case 6:
		// over flow, positive to negative
		v = ((((int(this.Data[this.Current+5]) * 0x80 + int(this.Data[this.Current+4]) -0x80) * 0x80 +  int(this.Data[this.Current+3]) - 0x80) * 0x80 + int(this.Data[this.Current+2]) - 0x80) * 0x80 + int(this.Data[this.Current+1]) - 0x80) * 0x40 + int(this.Data[this.Current]) - 0x80;
	default:
		panic(errors.New("bad data"))
	}
	this.skipByte(nBytes)
	return v
}

func (this *RespParser) uncompressIf() {
	if this.getLen() == this.getLen1() {
		this.Data = this.RawBuffer[this.getHeaderLen():]
	} else {
		b := bytes.NewReader(this.RawBuffer[this.getHeaderLen():])
		var out bytes.Buffer
		r, _ := zlib.NewReader(b)
		io.Copy(&out, r)
		this.Data = out.Bytes()
	}

	this.Current = 0
}

func (this *RespParser) Parse() {
	if int(this.getLen()) + this.getHeaderLen() > len(this.RawBuffer) {
		panic(errors.New("incomplete data"))
	}
	this.uncompressIf()
}

func (this *RespParser) tryParseData() (err error, v int) {
	err = nil
	defer func() {
		if err1 := recover(); err1 != nil {
			err = err1.(error)
		}
	}()

	v = this.parseData()
	return
}

func (this *RespParser) tryParseData2() (err error, v int) {
	err = nil
	defer func() {
		if err1 := recover(); err1 != nil {
			err = err1.(error)
		}
	}()

	v = this.parseData2()
	return
}

func (this *RespParser) TryParse() {
	this.Current = 0

	var f float32
	var i16 uint16
	var i32 uint32
	var iData int

	var err error

	for i := 0; i < len(this.Data) - 2; i++ {
		end := i+4
		if end > len(this.Data) {
			end = len(this.Data)
		}
		fmt.Printf("%4d. %v\t", i, hex.EncodeToString(this.Data[i:end]))
		if i < len(this.Data) - 4 {
			f = this.getFloat32()
			fmt.Printf("\t%50.2f", f)
			this.Current -= 4
			i16 = this.getUint16()
			fmt.Printf("\t%6d", i16)
			this.Current -= 2
			i32 = this.getUint32()
			fmt.Printf("\t%10d", i32)
			this.Current -= 4
		}

		current := this.Current
		err, iData = this.tryParseData()
		if err != nil {
			fmt.Print("\tNaN")
		} else {
			fmt.Printf("\t%10d", iData)
		}
		this.Current = current

		err, iData = this.tryParseData2()
		if err != nil {
			fmt.Print("\tNaN")
		} else {
			fmt.Printf("\t%10d", iData)
		}
		this.Current = current

		fmt.Printf("\n")

		this.Current++
	}
}

func (this *InstantTransParser) Parse() (error, []Transaction) {
	if int(this.getLen()) + this.getHeaderLen() > len(this.RawBuffer) {
		return errors.New("incomplete data"), nil
	}

	if this.GetSeqId() != this.Req.GetSeqId() {
		return errors.New("bad seq id"), nil
	}

	if this.GetCmd() != this.Req.GetCmd() {
		return errors.New("bad cmd"), nil
	}

	this.uncompressIf()

	count := this.getUint16()
	result := make([]Transaction, count)

	first := true
	var priceBase int

	for i := 0; i < int(count); i++ {
		trans := &result[i]
		trans.Minute = this.getUint16()
		if first {
			priceBase = this.parseData2()
			trans.Price = uint32(priceBase)
			first = false
		} else {
			priceBase = this.parseData() + priceBase
			trans.Price = uint32(priceBase)
		}
		trans.Volume = uint32(this.parseData2())
		trans.Count = uint32(this.parseData2())
		trans.BS = this.getByte()
		this.skipByte(1)
	}
	return nil, result
}

func NewInstantTransParser(req Request, data []byte) *InstantTransParser {
	return &InstantTransParser{
		RespParser: RespParser{
			RawBuffer: data,
		},
		Req: req,
	}
}

func (this *HisTransParser) Parse() (error, []Transaction) {
	if int(this.getLen()) + this.getHeaderLen() > len(this.RawBuffer) {
		return errors.New("incomplete data"), nil
	}

	if this.GetSeqId() != this.Req.GetSeqId() {
		return errors.New("bad seq id"), nil
	}

	if this.GetCmd() != this.Req.GetCmd() {
		return errors.New("bad cmd"), nil
	}

	this.uncompressIf()

	count := this.getUint16()
	this.skipByte(4)

	result := make([]Transaction, count)

	first := true
	var priceBase int

	for i := 0; i < int(count); i++ {
		trans := &result[i]
		trans.Date = this.Req.(*HisTransReq).Date
		trans.Minute = this.getUint16()
		if first {
			priceBase = this.parseData2()
			trans.Price = uint32(priceBase)
			first = false
		} else {
			priceBase = this.parseData() + priceBase
			trans.Price = uint32(priceBase)
		}
		trans.Volume = uint32(this.parseData2())
		trans.BS = this.getByte()
		trans.Count = uint32(this.parseData2())
	}
	return nil, result
}

func NewHisTransParser(req Request, data []byte) *HisTransParser {
	return &HisTransParser{
		RespParser: RespParser{
			RawBuffer: data,
		},
		Req: req,
	}
}

func (this *InfoExParser) Parse() (error, map[string][]*InfoExItem) {
	if int(this.getLen()) + this.getHeaderLen() > len(this.RawBuffer) {
		return errors.New("incomplete data"), nil
	}

	if this.GetSeqId() != this.Req.GetSeqId() {
		return errors.New("bad seq id"), nil
	}

	if this.GetCmd() != this.Req.GetCmd() {
		return errors.New("bad cmd"), nil
	}

	this.uncompressIf()

	result := map[string][]*InfoExItem{}

	count := this.getUint16()

	for ; count > 0; count-- {
		loc := this.getByte()
		stockCode := GetFullCode(loc, string(this.Data[this.Current:this.Current + STOCK_CODE_LEN]))
		this.skipByte(STOCK_CODE_LEN)
		recordCount := this.getUint16()

		result[stockCode] = []*InfoExItem{}

		for ; recordCount > 0; recordCount-- {
			loc := this.getByte()
			stockCode1 := GetFullCode(loc, string(this.Data[this.Current:this.Current + STOCK_CODE_LEN]))
			this.skipByte(STOCK_CODE_LEN + 1)
			if stockCode != stockCode1 {
				return errors.New(fmt.Sprintf("bad stock code, stockCode: %s stockCode1: %s", stockCode, stockCode1)), nil
			}
			date := this.getUint32()
			tp := this.getByte()
			if tp != 1 {
				//f1, f2, f3, f4 := this.getFloat32(), this.getFloat32(), this.getFloat32(), this.getFloat32()
				//fmt.Println("tp:", tp, "date:", date, f1, f2, f3, f4)
				this.skipByte(16)
				continue
			}

			obj := &InfoExItem{}
			obj.Date = date
			obj.Bonus = this.getFloat32() / 10
			obj.RationedSharePrice = this.getFloat32()
			obj.DeliveredShares = this.getFloat32() / 10
			obj.RationedShares = this.getFloat32() / 10

			result[stockCode] = append(result[stockCode], obj)
		}
	}
	return nil, result
}

func NewInfoExParser(req Request, data []byte) *InfoExParser {
	return &InfoExParser{
		RespParser: RespParser{
			RawBuffer: data,
		},
		Req: req,
	}
}

func NewFinanceParser(req Request, data []byte) *FinanceParser {
	return &FinanceParser{
		RespParser: RespParser{
			RawBuffer: data,
		},
		Req: req,
	}
}

func (this *FinanceParser) Parse() (err error, finances map[string]*Finance) {
	if int(this.getLen()) + this.getHeaderLen() > len(this.RawBuffer) {
		err = errors.New("incomplete data")
		return
	}

	if this.GetSeqId() != this.Req.GetSeqId() {
		err = errors.New("bad seq id")
		return
	}

	if this.GetCmd() != this.Req.GetCmd() {
		err = errors.New("bad cmd")
		return
	}

	this.uncompressIf()

	finances = make(map[string]*Finance)

	count := this.getUint16()

	for ; count > 0; count-- {
		loc := this.getByte()
		stockCode := GetFullCode(loc, string(this.Data[this.Current:this.Current + STOCK_CODE_LEN]))
		this.skipByte(STOCK_CODE_LEN)

		finance := new(Finance)

		this.skipByte(41 - (3 + STOCK_CODE_LEN))

		finance.BShares = this.getFloat32()                // 41
		finance.HShares = this.getFloat32()                // 45
		finance.ProfitPerShare = this.getFloat32()        // 49
		finance.TotalAssets = this.getFloat32()            // 53
		finance.CurrentAssets = this.getFloat32()        // 57
		finance.FixedAssets = this.getFloat32()            // 61
		finance.IntangibleAssets = this.getFloat32()    // 65
		finance.ShareHolders = this.getFloat32()        // 69
		finance.CurrentLiability = this.getFloat32()    // 73
		finance.MinorShareRights = this.getFloat32()    // 77
		finance.PublicReserveFunds = this.getFloat32()    // 81
		finance.NetAssets = this.getFloat32()            // 85
		finance.OperatingIncome = this.getFloat32()        // 89
		finance.OperatingCost = this.getFloat32()        // 93
		finance.Receivables = this.getFloat32()            // 97
		finance.OperationProfit = this.getFloat32()        // 101
		finance.InvestProfit = this.getFloat32()        // 105
		finance.OperatingCash = this.getFloat32()        // 109
		finance.TotalCash = this.getFloat32()            // 113
		finance.Inventory = this.getFloat32()            // 117
		finance.TotalProfit = this.getFloat32()            // 121
		finance.NOPAT = this.getFloat32()                // 125
		finance.NetProfit = this.getFloat32()            // 129
		finance.UndistributedProfit = this.getFloat32()    // 133
		finance.NetAdjustedAssets = this.getFloat32()    // 137

		this.skipByte(4)

		finances[stockCode] = finance
	}

	return
}

func (this *BidParser) isStockValid(s []byte) bool {
	if len(s) < STOCK_CODE_LEN {
		return false
	}

	for i := 0; i < STOCK_CODE_LEN; i++ {
		if s[i] < 0x30 || s[i] > 0x39 {
			return false
		}
	}
	return true
}

func (this *BidParser) searchStockCode() int {
	for i := this.Current; i < len(this.Data); i++ {
		if this.isStockValid(this.Data[i:]) {
			return i - this.Current - 1
		}
	}
	panic(errors.New("no stock code found"))
}

func (this *BidParser) decrypt() {
	for i, b := range this.Data {
		this.Data[i] = b ^ 57
	}
}

func (this *BidParser) Parse() (error, map[string]*Bid) {
	if int(this.getLen()) + this.getHeaderLen() > len(this.RawBuffer) {
		return errors.New("incomplete data"), nil
	}

	if this.GetSeqId() != this.Req.GetSeqId() {
		return errors.New("bad seq id"), nil
	}

	if this.GetCmd() != this.Req.GetCmd() {
		return errors.New("bad cmd"), nil
	}

	this.uncompressIf()
	this.decrypt()

	result := map[string]*Bid{}

	count := this.getUint16()

	for ; count > 0; count-- {
		loc := this.getByte()
		stockCode := GetFullCode(loc, string(this.Data[this.Current:this.Current + STOCK_CODE_LEN]))
		this.skipByte(STOCK_CODE_LEN)
		this.skipByte(2) // 未知

		bid := &Bid{StockCode: stockCode}

		bid.Close = uint32(this.parseData2())
		bid.YesterdayClose = uint32(this.parseData() + int(bid.Close))
		bid.Open = uint32(this.parseData() + int(bid.Close))
		bid.High = uint32(this.parseData() + int(bid.Close))
		bid.Low = uint32(this.parseData() + int(bid.Close))

		this.skipByte(5)

		bid.Vol = uint32(this.parseData2())
		this.parseData2()
		bid.Amount = this.getFloat32()
		bid.InnerVol = uint32(this.parseData2())
		bid.OuterVol = uint32(this.parseData2())

		this.skipByte(1)
		this.parseData2()

		bid.BuyPrice1 = uint32(this.parseData() + int(bid.Close))
		bid.SellPrice1 = uint32(this.parseData() + int(bid.Close))
		bid.BuyVol1 = uint32(this.parseData2())
		bid.SellVol1 = uint32(this.parseData2())

		bid.BuyPrice2 = uint32(this.parseData() + int(bid.Close))
		bid.SellPrice2 = uint32(this.parseData() + int(bid.Close))
		bid.BuyVol2 = uint32(this.parseData2())
		bid.SellVol2 = uint32(this.parseData2())

		bid.BuyPrice3 = uint32(this.parseData() + int(bid.Close))
		bid.SellPrice3 = uint32(this.parseData() + int(bid.Close))
		bid.BuyVol3 = uint32(this.parseData2())
		bid.SellVol3 = uint32(this.parseData2())

		bid.BuyPrice4 = uint32(this.parseData() + int(bid.Close))
		bid.SellPrice4 = uint32(this.parseData() + int(bid.Close))
		bid.BuyVol4 = uint32(this.parseData2())
		bid.SellVol4 = uint32(this.parseData2())

		bid.BuyPrice5 = uint32(this.parseData() + int(bid.Close))
		bid.SellPrice5 = uint32(this.parseData() + int(bid.Close))
		bid.BuyVol5 = uint32(this.parseData2())
		bid.SellVol5 = uint32(this.parseData2())

		result[stockCode] = bid

		if count > 1 {
			n := this.searchStockCode()
			this.skipByte(n)
		}
	}
	return nil, result
}

func NewBidParser(req Request, data []byte) *BidParser {
	return &BidParser{
		RespParser: RespParser{
			RawBuffer: data,
		},
		Req: req,
	}
}

func NewPeriodDataParser(req Request, data []byte) *PeriodDataParser {
	return &PeriodDataParser{
		RespParser: RespParser{
			RawBuffer: data,
		},
		Req: req,
	}
}

func (this *PeriodDataParser) Parse() (error, []entity.Record) {
	if int(this.getLen()) + this.getHeaderLen() > len(this.RawBuffer) {
		return errors.New("incomplete data"), nil
	}

	if this.GetSeqId() != this.Req.GetSeqId() {
		return errors.New("bad seq id"), nil
	}

	if this.GetCmd() != this.Req.GetCmd() {
		return errors.New("bad cmd"), nil
	}

	this.uncompressIf()

	first := true
	count := this.getUint16()
	var priceBase int

	period := periodMap[this.Req.(*PeriodDataReq).Period]

	result := make([]entity.Record, count)

	for i := 0; i < int(count); i++ {
		record := &result[i]
		record.Date = tdxdatasource.DateToTimestamp(period, this.getUint32())

		if first {
			priceBase = this.parseData2()
			record.Open = int32(priceBase)
			first = false
		} else {
			record.Open = int32(this.parseData() + priceBase)
		}

		record.Close = int32(this.parseData() + int(record.Open))
		record.High = int32(this.parseData() + int(record.Open))
		record.Low = int32(this.parseData() + int(record.Open))
		record.Volume = this.getFloat32()
		record.Amount = this.getFloat32()

		priceBase = int(record.Close)
	}

	return nil, result
}

func NewPeriodHisDataParser(req Request, data []byte) *PeriodHisDataParser {
	return &PeriodHisDataParser{
		RespParser: RespParser{
			RawBuffer: data,
		},
		Req: req,
	}
}

func (this *PeriodHisDataParser) Parse() (error, []byte) {
	if int(this.getLen()) + this.getHeaderLen() > len(this.RawBuffer) {
		return errors.New("incomplete data"), nil
	}

	if this.GetSeqId() != this.Req.GetSeqId() {
		return errors.New("bad seq id"), nil
	}

	if this.GetCmd() != this.Req.GetCmd() {
		return errors.New("bad cmd"), nil
	}

	this.uncompressIf()

	return nil, this.Data[6:]
}

func NewGetFileLenParser(req Request, data []byte) *GetFileLenParser {
	return &GetFileLenParser{
		RespParser: RespParser{
			RawBuffer: data,
		},
		Req: req,
	}
}

func (this *GetFileLenParser) Parse() (err error, length uint32) {
	if int(this.getLen()) + this.getHeaderLen() > len(this.RawBuffer) {
		err = errors.New("incomplete data")
		return
	}

	if this.GetSeqId() != this.Req.GetSeqId() {
		err = errors.New("bad seq id")
		return
	}

	if this.GetCmd() != this.Req.GetCmd() {
		err = errors.New("bad cmd")
		return
	}

	this.uncompressIf()

	length = this.getUint32()

	return
}

func NewGetFileDataParser(req Request, data []byte) *GetFileDataParser {
	return &GetFileDataParser{
		RespParser: RespParser{
			RawBuffer: data,
		},
		Req: req,
	}
}

func (this *GetFileDataParser) Parse() (err error, length uint32, data []byte) {
	if int(this.getLen()) + this.getHeaderLen() > len(this.RawBuffer) {
		err = errors.New("incomplete data")
		return
	}

	if this.GetSeqId() != this.Req.GetSeqId() {
		err = errors.New("bad seq id")
		return
	}

	if this.GetCmd() != this.Req.GetCmd() {
		err = errors.New("bad cmd")
		return
	}

	this.uncompressIf()

	length = binary.LittleEndian.Uint32(this.Data[:4])
	data = this.Data[4:]

	return
}

func NewNamesParser(req Request, data []byte) *NamesParser {
	return &NamesParser{
		RespParser: RespParser{
			RawBuffer: data,
		},
		Req: req,
	}
}

func (this *NamesParser) Parse() (err error, length uint16, data []byte) {
	if int(this.getLen()) + this.getHeaderLen() > len(this.RawBuffer) {
		err = errors.New("incomplete data")
		return
	}

	if this.GetSeqId() != this.Req.GetSeqId() {
		err = errors.New("bad seq id")
		return
	}

	if this.GetCmd() != this.Req.GetCmd() {
		err = errors.New("bad cmd")
		return
	}

	this.uncompressIf()

	length = binary.LittleEndian.Uint16(this.Data[:2])
	data = this.Data[2:]

	return
}

func NewNamesLenParser(req Request, data []byte) *NamesLenParser {
	return &NamesLenParser{
		RespParser: RespParser{
			RawBuffer: data,
		},
		Req: req,
	}
}

func (this *NamesLenParser) Parse() (err error, length uint32) {
	if int(this.getLen()) + this.getHeaderLen() > len(this.RawBuffer) {
		err = errors.New("incomplete data")
		return
	}

	if this.GetSeqId() != this.Req.GetSeqId() {
		err = errors.New("bad seq id")
		return
	}

	if this.GetCmd() != this.Req.GetCmd() {
		err = errors.New("bad cmd")
		return
	}

	this.uncompressIf()

	length = uint32(this.getUint16())

	return
}

func NewRespParser(data []byte) *RespParser {
	return &RespParser{RawBuffer: data}
}

func ReadResp(conn net.Conn) (error, []byte) {
	header := make([]byte, RESP_HEADER_LEN)
	nRead := 0
	for nRead < RESP_HEADER_LEN {
		n, err := conn.Read(header[nRead:])
		if err != nil {
			log.Errorf("ReadResp - read header fail, error: %v", err)
			return err, nil
		} else {
			log.Debugf("ReadResp - read header success, n: %d", n)
		}
		nRead += n

		// Find magic number
		var i int
		for i < nRead - 4 {
			if header[i] != 0xb1 && header[i+1] != 0xeb && header[i+2] != 0x74 && header[i+3] != 00 {
				i++
			} else {
				break
			}
		}
		copy(header[0:nRead-i], header[i:nRead])
		nRead -= i
	}

	length := int(binary.LittleEndian.Uint16(header[12:14]))
	result := make([]byte, length + RESP_HEADER_LEN)
	copy(result[:RESP_HEADER_LEN], header[:])

	for nRead < length + RESP_HEADER_LEN {
		n, err := conn.Read(result[nRead:])
		if err != nil {
			log.Errorf("ReadResp - read data fail, error: %v", err)
			return err, nil
		}
		nRead += n
	}

	return nil, result
}

func ReadRespN(conn net.Conn, buffer []byte) (error, []byte) {
	var nRead int

	for nRead < len(buffer) {
		n, err := conn.Read(buffer[nRead:])
		fmt.Printf("read: %d\n", n)
		if err != nil {
			return err, nil
		}
		nRead += n
		fmt.Printf("nRead:", nRead)
	}

	return nil, buffer[:nRead]
}
