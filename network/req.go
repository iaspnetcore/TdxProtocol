package network

import "bytes"
import (
	"encoding/binary"
	"github.com/stephenlyu/tds/date"
	"github.com/stephenlyu/tds/entity"
)

const (
	CMD_INFO_EX = 0x000f
	CMD_FINANCE = 0x0010
	CMD_NAMES_LEN = 0x044e
	CMD_NAMES = 0x0450
	CMD_BID = 0x0526
	CMD_PERIOD_DATA = 0x052d
	CMD_INSTANT_TRANS = 0x0fc5
	CMD_PERIOD_HIS_DATA = 0x0fcd
	CMD_HIS_TRANS = 0x0fb5
	CMD_HEART_BEAT = 0x0523

	CMD_GET_FILE_LEN = 0x2c5
	CMD_GET_FILE_DATA = 0x6b9
)

const (
	BLOCK_SH_A = 0
	BLOCK_SH_B = 1
	BLOCK_SZ_A = 2
	BLOCK_SZ_B = 3
	BLOCK_INDEX = 11
	BLOCK_UNKNOWN = 99
)

const (
	PERIOD_MINUTE5 = 0x0000
	PERIOD_DAY = 0x0004
	PERIOD_MINUTE = 0x0007
)

type Request interface {
	GetSeqId() uint32
	GetCmd() uint16
}

type Header struct {
	Zip 	byte
	SeqId 	uint32
	PacketType byte
	Len 	uint16
	Len1 	uint16
	Cmd 	uint16
}

func (this Header) GetSeqId() uint32 {
	return this.SeqId
}

func (this Header) GetCmd() uint16 {
	return this.Cmd
}

type StockDef struct {
	MarketLocation 	byte
	StockCode string
}

type NamesReq struct {
	Header
	Block uint16
	Offset uint16
}

type NamesLenReq struct {
	Header
	Block uint16
}

type InfoExReq struct {
	Header
	Count uint16
	Stocks []*StockDef
}

type FinanceReq struct {
	Header
	Count uint16
	Stocks []*StockDef
}

type BidReq struct {
	Header
	Count uint16
	Stocks []*StockDef
	Pad uint32
}

type InstantTransReq struct {
	Header
	Location uint16
	StockCode string
	Offset uint16
	Count uint16
}

type HisTransReq struct {
	Header
	Date uint32
	Location uint16
	StockCode string
	Offset uint16
	Count uint16
}

type PeriodDataReq struct {
	Header
	Location uint16
	StockCode string
	Period uint16
	Unknown1 uint16 		// Always be 1
	Offset uint16
	Count uint16
	Unknown2 uint32			// 0
	Unknown3 uint32			// 0
	Unknown4 uint16			// 0
}

type PeriodHisDataReq struct {
	Header
	Location uint16
	StockCode string
	StartDate uint32
	EndDate uint32
	Period uint16
}

type GetFileLenReq struct {
	Header
	FileName string
}

type GetFileDataReq struct {
	Header
	Offset uint32
	Length uint32
	FileName string
}

func MarketLocationFromSecurity(security *entity.Security) byte {
	if security.Exchange == "SZ"{
		return 0
	}
	return 1
}

func BlockFromCode(stockCode string) int {
	data := []byte(stockCode)
	switch data[0] {
	case 0x30:
		return BLOCK_SZ_A
	case 0x32:
		return BLOCK_SZ_B
	case 0x33:
		if data[1] == 0x30 {
			return BLOCK_SZ_A
		} else {
			return BLOCK_INDEX
		}
	case 0x36:
		return BLOCK_SH_A
	case 0x99:
		if data[1] == 0x30 {
			return BLOCK_SH_B
		} else {
			return BLOCK_INDEX
		}
	default:
		return BLOCK_UNKNOWN
	}
}

func writeUInt16(writer *bytes.Buffer, v uint16) {
	var int16buf2 [2]byte
	binary.LittleEndian.PutUint16(int16buf2[:], v)
	writer.Write(int16buf2[:])
}

func writeUInt32(writer *bytes.Buffer, v uint32) {
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], v)
	writer.Write(buf[:])
}

func (this *Header) Write(writer *bytes.Buffer) {
	binary.Write(writer, binary.LittleEndian, *this)
}

func (this *Header) SetLength(length uint16) {
	this.Len = length
	this.Len1 = length
}

func (this *StockDef) Write(writer *bytes.Buffer) {
	writer.Write([]byte{this.MarketLocation})
	writer.Write([]byte(this.StockCode))
}

func (this *InfoExReq) Write(writer *bytes.Buffer) {
	this.Header.Write(writer)
	writeUInt16(writer, this.Count)

	for _, o := range this.Stocks {
		o.Write(writer)
	}
}

func (this *InfoExReq) Size() int {
	return 4 + 7 * len(this.Stocks)
}

func (this *InfoExReq) AddCode(security *entity.Security) {
	v := &StockDef{
		MarketLocationFromSecurity(security),
		security.GetCode(),
	}

	this.Stocks = append(this.Stocks, v)
	this.Count = uint16(len(this.Stocks))
	this.Header.SetLength(uint16(this.Size()))
}

func NewInfoExReq(seqId uint32) *InfoExReq {
	req := &InfoExReq{
		Header{
			Zip: 0xc,
			SeqId: seqId,
			PacketType: 0x1,
			Len: 0,
			Len1: 0,
			Cmd: CMD_INFO_EX,
		},
		0,
		[]*StockDef {},
	}
	return req
}

func (this *FinanceReq) Write(writer *bytes.Buffer) {
	this.Header.Write(writer)
	writeUInt16(writer, this.Count)

	for _, o := range this.Stocks {
		o.Write(writer)
	}
}

func (this *FinanceReq) Size() int {
	return 4 + 7 * len(this.Stocks)
}

func (this *FinanceReq) AddCode(security *entity.Security) {
	v := &StockDef{
		MarketLocationFromSecurity(security),
		security.GetCode(),
	}

	this.Stocks = append(this.Stocks, v)
	this.Count = uint16(len(this.Stocks))
	this.Header.SetLength(uint16(this.Size()))
}

func NewFinanceReq(seqId uint32) *FinanceReq {
	req := &FinanceReq{
		Header{
			Zip: 0xc,
			SeqId: seqId,
			PacketType: 0x1,
			Len: 0,
			Len1: 0,
			Cmd: CMD_FINANCE,
		},
		0,
		[]*StockDef {},
	}
	return req
}

func (this *BidReq) Write(writer *bytes.Buffer) {
	this.Header.Write(writer)
	writeUInt16(writer, this.Count)

	for _, o := range this.Stocks {
		o.Write(writer)
		writeUInt32(writer, 0)
	}
}

func (this *BidReq) Size() int {
	return 4 + 11 * len(this.Stocks)
}

func (this *BidReq) AddCode(security *entity.Security) {
	v := &StockDef{
		MarketLocationFromSecurity(security),
		security.GetCode(),
	}

	this.Stocks = append(this.Stocks, v)
	this.Count = uint16(len(this.Stocks))
	this.Header.SetLength(uint16(this.Size()))
}

func NewBidReq(seqId uint32) *BidReq {
	req := &BidReq{
		Header: Header{
			Zip: 0xc,
			SeqId: seqId,
			PacketType: 0x1,
			Len: 0,
			Len1: 0,
			Cmd: CMD_BID,
		},
	}

	req.Header.Len = uint16(req.Size())
	req.Header.Len1 = req.Header.Len

	return req
}

func (this *InstantTransReq) Write(writer *bytes.Buffer) {
	this.Header.Write(writer)
	writeUInt16(writer, this.Location)
	writer.Write([]byte(this.StockCode))
	writeUInt16(writer, this.Offset)
	writeUInt16(writer, this.Count)
}

func (this *InstantTransReq) Size() uint16 {
	return 14
}

func NewInstantTransReq(seqId uint32, security *entity.Security, offset uint16, count uint16) *InstantTransReq {
	req := &InstantTransReq{
		Header{
			Zip: 0xc,
			SeqId: seqId,
			PacketType: 0x1,
			Len: 0,
			Len1: 0,
			Cmd: CMD_INSTANT_TRANS,
		},
		uint16(MarketLocationFromSecurity(security)),
		security.GetCode(),
		offset,
		count,
	}

	req.Header.Len = uint16(req.Size())
	req.Header.Len1 = req.Header.Len

	return req
}

func (this *HisTransReq) Write(writer *bytes.Buffer) {
	this.Header.Write(writer)
	writeUInt32(writer, this.Date)
	writeUInt16(writer, this.Location)
	writer.Write([]byte(this.StockCode))
	writeUInt16(writer, this.Offset)
	writeUInt16(writer, this.Count)
}

func (this *HisTransReq) Size() uint16 {
	return 18
}

func NewHisTransReq(seqId uint32, date uint32, security *entity.Security, offset uint16, count uint16) *HisTransReq {
	req := &HisTransReq{
		Header{
			Zip: 0xc,
			SeqId: seqId,
			PacketType: 0x1,
			Len: 0,
			Len1: 0,
			Cmd: CMD_HIS_TRANS,
		},
		date,
		uint16(MarketLocationFromSecurity(security)),
		security.GetCode(),
		offset,
		count,
	}

	req.Header.Len = req.Size()
	req.Header.Len1 = req.Header.Len

	return req
}

func (this *PeriodDataReq) Write(writer *bytes.Buffer) {
	this.Header.Write(writer)
	writeUInt16(writer, this.Location)
	writer.Write([]byte(this.StockCode))
	writeUInt16(writer, this.Period)
	writeUInt16(writer, this.Unknown1)
	writeUInt16(writer, this.Offset)
	writeUInt16(writer, this.Count)
	writeUInt32(writer, this.Unknown2)
	writeUInt32(writer, this.Unknown3)
	writeUInt16(writer, this.Unknown4)
}

func (this *PeriodDataReq) Size() uint16 {
	return 28
}

func NewPeriodDataReq(seqId uint32, security *entity.Security, period uint16, offset uint16, count uint16) *PeriodDataReq {
	req := &PeriodDataReq{
		Header{
			Zip: 0xc,
			SeqId: seqId,
			PacketType: 0x1,
			Len: 0,
			Len1: 0,
			Cmd: CMD_PERIOD_DATA,
		},
		uint16(MarketLocationFromSecurity(security)),
		security.GetCode(),
		period,
		0,
		offset,
		count,
		0,
		0,
		0,
	}

	req.Header.Len = req.Size()
	req.Header.Len1 = req.Header.Len

	return req
}

func (this *PeriodHisDataReq) Write(writer *bytes.Buffer) {
	this.Header.Write(writer)
	writeUInt16(writer, this.Location)
	writer.Write([]byte(this.StockCode))
	writeUInt32(writer, this.StartDate)
	writeUInt32(writer, this.EndDate)
	writeUInt16(writer, this.Period)
}

func (this *PeriodHisDataReq) Size() uint16 {
	return 20
}

func NewPeriodHisDataReq(seqId uint32, security *entity.Security, period uint16, startDate uint32, endDate uint32) *PeriodHisDataReq {
	req := &PeriodHisDataReq{
		Header{
			Zip: 0xc,
			SeqId: seqId,
			PacketType: 0x1,
			Len: 0,
			Len1: 0,
			Cmd: CMD_PERIOD_HIS_DATA,
		},
		uint16(MarketLocationFromSecurity(security)),
		security.GetCode(),
		startDate,
		endDate,
		period,
	}

	req.Header.Len = req.Size()
	req.Header.Len1 = req.Header.Len

	return req
}

func (this *NamesReq) Write(writer *bytes.Buffer) {
	this.Header.Write(writer)
	writeUInt16(writer, this.Block)
	writeUInt16(writer, this.Offset)
}

func (this *NamesReq) Size() uint16 {
	return 6
}

func NewNamesReq(seqId uint32, block uint16, offset uint16) *NamesReq {
	req := &NamesReq{
		Header{
			Zip: 0xc,
			SeqId: seqId,
			PacketType: 0x1,
			Len: 0,
			Len1: 0,
			Cmd: CMD_NAMES,
		},
		block,
		offset,
	}

	req.Header.Len = req.Size()
	req.Header.Len1 = req.Header.Len

	return req
}

func (this *NamesLenReq) Write(writer *bytes.Buffer) {
	this.Header.Write(writer)
	writeUInt16(writer, this.Block)
	writeUInt32(writer, uint32(date.GetTodayInt()))
}

func (this *NamesLenReq) Size() uint16 {
	return 8
}

func NewNamesLenReq(seqId uint32, block uint16) *NamesLenReq {
	req := &NamesLenReq{
		Header{
			Zip: 0xc,
			SeqId: seqId,
			PacketType: 0x1,
			Len: 0,
			Len1: 0,
			Cmd: CMD_NAMES_LEN,
		},
		block,
	}

	req.Header.Len = req.Size()
	req.Header.Len1 = req.Header.Len

	return req
}

func (this *GetFileLenReq) Write(writer *bytes.Buffer) {
	this.Header.Write(writer)
	bytes := make([]byte, 40)
	copy(bytes, []byte(this.FileName))
	writer.Write(bytes)
}

func (this *GetFileLenReq) Size() uint16 {
	return 42
}

func NewGetFileLenReq(seqId uint32, fileName string) *GetFileLenReq {
	req := &GetFileLenReq{
		Header{
			Zip: 0xc,
			SeqId: seqId,
			PacketType: 0x1,
			Len: 0,
			Len1: 0,
			Cmd: CMD_GET_FILE_LEN,
		},
		fileName,
	}

	req.Header.Len = req.Size()
	req.Header.Len1 = req.Header.Len

	return req
}

func (this *GetFileDataReq) Write(writer *bytes.Buffer) {
	this.Header.Write(writer)
	writeUInt32(writer, this.Offset)
	writeUInt32(writer, this.Length)

	bytes := make([]byte, 100)
	copy(bytes, []byte(this.FileName))
	writer.Write(bytes)
}

func (this *GetFileDataReq) Size() uint16 {
	return 110
}

func NewGetFileDataReq(seqId uint32, fileName string, offset uint32, length uint32) *GetFileDataReq {
	req := &GetFileDataReq{
		Header{
			Zip: 0xc,
			SeqId: seqId,
			PacketType: 0x1,
			Len: 0,
			Len1: 0,
			Cmd: CMD_GET_FILE_DATA,
		},
		offset,
		length,
		fileName,
	}

	req.Header.Len = req.Size()
	req.Header.Len1 = req.Header.Len

	return req
}
