package network

import (
	"sync"
	"gopkg.in/fatih/pool.v2"
	"net"
	"bytes"
	"time"
	"encoding/hex"
)

type API struct {
	seqId			uint32
	lock    		sync.Mutex

	timeout 		int					// 毫秒数
	pool 			pool.Pool
}

func CreateAPI(host string) (error, *API) {
	api := &API {}
	err := api.Initialize(host)
	if err != nil {
		return err, nil
	}

	return nil, api
}

func (this *API) SetTimeOut(timeout int) {
	this.timeout = timeout
}

func (this *API) Initialize(host string) error {
	sendReq := func(conn net.Conn, reqHex string) error {
		reqData, _ := hex.DecodeString(reqHex)

		_, err := conn.Write(reqData)
		if err != nil {
			return err
		}

		err, _ = ReadResp(conn)
		return err
	}

	factory := func() (net.Conn, error) {
		conn, err := net.Dial("tcp", host)
		if err != nil {
			return conn, err
		}

		// Connection Prolog
		err = sendReq(conn, "0c0218940001030003000d0001")
		if err != nil {
			conn.Close()
			return nil, err
		}

		err = sendReq(conn, "0c031899000120002000db0fb3a4bdadd6a4c8af0000009a993141090000000000000000000000000003")
		if err != nil {
			conn.Close()
			return nil, err
		}
		return conn, nil
	}

	p, err := pool.NewChannelPool(5, 5, factory)
	if err != nil {
		return err
	}

	this.pool = p

	this.timeout = 10 * 1000

	return nil
}

func (this *API) Cleanup() error {
	if this.pool != nil {
		this.pool.Close()
		this.pool = nil
	}
	return nil
}

func (this *API) nextSeqId() uint32 {
	this.lock.Lock()
	defer this.lock.Unlock()
	this.seqId++
	return this.seqId
}

func (this *API) markConnUnusable(conn interface{}) {
	if poolConn, ok := conn.(pool.PoolConn); ok {
		poolConn.MarkUnusable()
	}
}

func (this *API) sendReq(data []byte) (error, []byte) {
	conn, err := this.pool.Get()
	if err != nil {
		return err, nil
	}
	defer conn.Close()

	if this.timeout > 0 {
		conn.SetDeadline(time.Now().Add(time.Duration(this.timeout) * time.Millisecond))
	}
	_, err = conn.Write(data)
	if err != nil {
		this.markConnUnusable(conn)
		return err, nil
	}

	if this.timeout > 0 {
		conn.SetDeadline(time.Now().Add(time.Duration(this.timeout) * time.Millisecond))
	}
	err, respData := ReadResp(conn)
	if err != nil {
		this.markConnUnusable(conn)
		return err, nil
	}

	return err, respData
}

func (this *API) GetInfoEx(codes []string) (error, map[string][]*InfoExItem) {
	req := NewInfoExReq(this.nextSeqId())
	for _, code := range codes {
		req.AddCode(code)
	}
	buf := new(bytes.Buffer)
	req.Write(buf)

	err, respData := this.sendReq(buf.Bytes())
	if err != nil {
		return err, nil
	}

	parser := NewInfoExParser(req, respData)
	return parser.Parse()
}

func (this *API) GetFinance(codes []string) (error, map[string]*Finance) {
	req := NewFinanceReq(this.nextSeqId())
	for _, code := range codes {
		req.AddCode(code)
	}
	buf := new(bytes.Buffer)
	req.Write(buf)

	err, respData := this.sendReq(buf.Bytes())
	if err != nil {
		return err, nil
	}

	parser := NewFinanceParser(req, respData)
	return parser.Parse()
}

func (this *API) GetPeriodData(code string, period, offset, count uint16) (error, []*Record) {
	req := NewPeriodDataReq(this.nextSeqId(), code, period, offset, count)
	buf := new(bytes.Buffer)
	req.Write(buf)

	err, respData := this.sendReq(buf.Bytes())
	if err != nil {
		return err, nil
	}

	parser := NewPeriodDataParser(req, respData)
	return parser.Parse()
}

func (this *API) GetPeriodHisData(code string, period uint16, startDate, EndDate uint32) (error, []byte) {
	req := NewPeriodHisDataReq(this.nextSeqId(), code, period, startDate, EndDate)
	buf := new(bytes.Buffer)
	req.Write(buf)

	err, respData := this.sendReq(buf.Bytes())
	if err != nil {
		return err, nil
	}

	parser := NewPeriodHisDataParser(req, respData)
	return parser.Parse()
}

func (this *API) GetFileLength(fileName string) (error, uint32) {
	req := NewGetFileLenReq(this.nextSeqId(), fileName)
	buf := new(bytes.Buffer)
	req.Write(buf)

	err, respData := this.sendReq(buf.Bytes())
	if err != nil {
		return err, 0
	}

	parser := NewGetFileLenParser(req, respData)
	return parser.Parse()
}

func (this *API) GetFileData(fileName string, offset uint32, length uint32) (error, uint32, []byte) {
	req := NewGetFileDataReq(this.nextSeqId(), fileName, offset, length)
	buf := new(bytes.Buffer)
	req.Write(buf)

	err, respData := this.sendReq(buf.Bytes())
	if err != nil {
		return err, 0, nil
	}

	parser := NewGetFileDataParser(req, respData)
	return parser.Parse()
}

func (this *API) GetNamesLength(block uint16) (error, uint32) {
	req := NewNamesLenReq(this.nextSeqId(), block)
	buf := new(bytes.Buffer)
	req.Write(buf)

	err, respData := this.sendReq(buf.Bytes())
	if err != nil {
		return err, 0
	}

	parser := NewNamesLenParser(req, respData)
	return parser.Parse()
}

func (this *API) GetNamesData(block uint16, offset uint16) (error, uint16, []byte) {
	req := NewNamesReq(this.nextSeqId(), block, offset)
	buf := new(bytes.Buffer)
	req.Write(buf)

	err, respData := this.sendReq(buf.Bytes())
	if err != nil {
		return err, 0, nil
	}

	parser := NewNamesParser(req, respData)
	return parser.Parse()
}

func (this *API) GetMinuteData(code string, offset, count uint16) (error, []*Record) {
	return this.GetPeriodData(code, PERIOD_MINUTE, offset, count)
}

func (this *API) GetDayData(code string, offset, count uint16) (error, []*Record) {
	return this.GetPeriodData(code, PERIOD_DAY, offset, count)
}
