package block

import (
	"github.com/docker/docker/pkg/random"
	"fmt"
	"io/ioutil"
	"golang.org/x/text/encoding/simplifiedchinese"
	"os"
	"path/filepath"
	"strings"
)

func RandString() string {
	return fmt.Sprintf("%X", random.Rand.Int63())
}

func unpad_zero(s []byte) []byte {
	i := len(s) - 1
	for i >= 0 {
		if s[i] != 0 {
			break
		}
		i -= 1
	}
	return s[:i+1]
}

func pad_zero(s []byte, l int) []byte {
	if len(s) >= l {
		return s
	}

	ret := make([]byte, len(s) + l)
	copy(ret, s)

	return ret
}

func LoadBlockCfg(file_path string) (error, []map[string]string) {
	bytes, err := ioutil.ReadFile(file_path)
	if err != nil {
		return err, nil
	}
	result := []map[string]string{}

	decoder := simplifiedchinese.GBK.NewDecoder()

	i := 0
	for i < len(bytes) {
		sitem := bytes[i : i + 120]

		name, err := decoder.Bytes(unpad_zero(sitem[:50]))
		if err != nil {
			return err, nil
		}
		blk_name, err := decoder.Bytes(unpad_zero(sitem[50:]))
		if err != nil {
			return err, nil
		}
		result = append(result, map[string]string{"name": string(name), "blk_name": string(blk_name)})
		i += 120
	}

	return nil, result
}

func fileExist(file_path string) bool {
	_, err := os.Stat(file_path)
	if os.IsNotExist(err) {
		return false
	}
	return true
}

func SaveBlockCfg(file_path string, cfg []map[string]string) error {
	if fileExist(file_path) {
		bkFile := file_path + ".bk"
		if fileExist(bkFile) {
			os.Remove(bkFile)
		}
		os.Rename(file_path, bkFile)
	}

	encoder := simplifiedchinese.GBK.NewEncoder()

	bytes := make([]byte, len(cfg) * 120)
	for i, item := range cfg {
		d, err := encoder.Bytes([]byte(item["name"]))
		if err != nil {
			return err
		}

		copy(bytes[i * 120: i*120+50], pad_zero(d, 50))

		d, err = encoder.Bytes([]byte(item["blk_name"]))
		if err != nil {
			return err
		}
		copy(bytes[i * 120 + 50 : i * 120 + 120], pad_zero(d, 70))
	}

	return ioutil.WriteFile(file_path, bytes, 0666)
}

func ExportBlock(block_dir string, name string, stock_codes []string) {
	block_cfg_file_path := filepath.Join(block_dir, "blocknew.cfg")
	_, cfg := LoadBlockCfg(block_cfg_file_path)


	var block map[string]string
	for _, item := range cfg {
		if item["name"] == name {
			block = item
			break
		}
	}

	new_block := false
	if block == nil {
		block = map[string]string {"name": name, "blk_name": RandString()}
		cfg = append(cfg, block)
		new_block = true
	}

	block_file_name := block["blk_name"] + ".blk"
    block_file_path := filepath.Join(block_dir, block_file_name)

	SaveBlockCodes(block_file_path, stock_codes)
	if new_block {
		SaveBlockCfg(block_cfg_file_path, cfg)
	}
}

func SaveBlockCodes(block_file_path string, stock_codes []string) error {
	codes := []string{}
	for _, c := range stock_codes {
		if c[:1] == "6" {
			codes = append(codes, "1" + c)
		} else {
			codes = append(codes, "0" + c)
		}
	}

	return ioutil.WriteFile(block_file_path, []byte(strings.Join(codes, "\r\n")), 0666)
}
