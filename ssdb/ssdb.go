package ssdb

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"fmt"
	_ "io"
	"io/ioutil"
	"log"
	"math"
	"net"
	"reflect"
	"strconv"
	"strings"
	"sync"
	_ "syscall"
	"time"
)

type Client struct {
	sock      net.Conn
	recv_buf  bytes.Buffer
	process   chan []interface{}
	result    chan ClientResult
	Id        string
	Ip        string
	Port      int
	Password  string
	Connected bool
	Retry     bool
	mu        *sync.Mutex
	Closed    bool
	init      bool
}

type ClientResult struct {
	Id    string
	Data  []string
	Error error
}

type HashData struct {
	HashName string
	Key      string
	Value    string
}

var debug bool = false
var version string = "0.1.6"

const layout = "2006-01-06 15:04:05"

func Connect(ip string, port int, auth string) (*Client, error) {
	client, err := connect(ip, port, auth)
	if err != nil {
		if debug {
			log.Printf("SSDB Client Connect failed:%s:%d error:%v\n", ip, port, err)
		}
		go client.RetryConnect()
		return client, err
	}
	if client != nil {
		return client, nil
	}
	return nil, nil
}

func connect(ip string, port int, auth string) (*Client, error) {
	log.Printf("SSDB Client Version:%s\n", version)
	var c Client
	c.Ip = ip
	c.Port = port
	c.Password = auth
	c.Id = fmt.Sprintf("Cl-%d", time.Now().UnixNano())
	c.mu = &sync.Mutex{}
	err := c.Connect()
	return &c, err
}

func (c *Client) Debug(flag bool) bool {
	debug = flag
	log.Println("SSDB Client Debug Mode:", debug)
	return debug
}

func (c *Client) Connect() error {
	log.Printf("Client[%s] connect to %s:%d\n", c.Id, c.Ip, c.Port)
	/*addr, err := net.ResolveTCPAddr("tcp", fmt.Sprintf("%s:%d", c.Ip, c.Port))
	if err != nil {
		log.Println("Client ResolveTCPAddr failed:", err)
		return err
	}*/
	seconds := 60
	timeOut := time.Duration(seconds) * time.Second
	sock, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", c.Ip, c.Port), timeOut)
	if err != nil {
		log.Println("SSDB Client dial failed:", err, c.Id)
		return err
	}
	/*sock, err := net.DialTCP("tcp", nil, addr)
	if err != nil {
		log.Println("SSDB Client dial failed:", err, c.Id)
		return err
	}*/
	c.sock = sock
	c.Connected = true
	if c.Retry {
		log.Printf("Client[%s] retry connect to %s:%d success.", c.Id, c.Ip, c.Port)
	} else {
		log.Printf("Client[%s] connect to %s:%d success\n", c.Id, c.Ip, c.Port)
	}
	c.Retry = false
	if !c.init {
		c.process = make(chan []interface{})
		c.result = make(chan ClientResult)
		go c.processDo()
		c.init = true
	}

	if c.Password != "" {
		c.Auth(c.Password)
	}

	return nil
}

func (c *Client) KeepAlive() {
	go c.HealthCheck()
}

func (c *Client) HealthCheck() {
	timeout := 60
	//wait client connect to server
	//time.Sleep(5 * time.Second)
	for {
		if c.Connected && !c.Retry && !c.Closed {
			result, err := c.Do("ping")
			if err != nil {
				log.Printf("Client Health Check Failed[%s]:%v\n", c.Id, err)
			} else {
				if debug {
					log.Printf("Client Health Check Success[%s]:%v\n", c.Id, result)
				}
			}
		}
		time.Sleep(time.Duration(timeout) * time.Second)
	}
}

func (c *Client) RetryConnect() {
	if !c.Retry {
		c.mu.Lock()
		c.Retry = true
		c.Connected = false
		c.mu.Unlock()
		log.Printf("Client[%s] retry connect to %s:%d Connected:%v Closed:%v\n", c.Id, c.Ip, c.Port, c.Connected, c.Closed)
		for {
			if !c.Connected && !c.Closed {
				log.Printf("Client[%s] retry connect to %s:%d\n", c.Id, c.Ip, c.Port)
				err := c.Connect()
				if err != nil {
					log.Printf("Client[%s] Retry connect to %s:%d Failed. Error:%v\n", c.Id, c.Ip, c.Port, err)
					time.Sleep(5 * time.Second)
				}
			} else {
				log.Printf("Client[%s] Retry connect to %s:%d stop by conn:%v closed:%v\n.", c.Id, c.Ip, c.Port, c.Connected, c.Closed)
				break
			}
		}
	}
}

func (c *Client) CheckError(err error) {
	//if err == io.EOF || strings.Contains(err.Error(), "connection") || strings.Contains(err.Error(), "timed out") || strings.Contains(err.Error(), "route") {
	if err != nil {
		if !c.Closed {
			log.Printf("Check Error:%v Retry connect.\n", err)
			c.sock.Close()
			go c.RetryConnect()
		}

	}
}

func (c *Client) processDo() {
	for args := range c.process {
		runId := args[0].(string)
		runArgs := args[1:]
		result, err := c.do(runArgs)
		c.result <- ClientResult{Id: runId, Data: result, Error: err}
		/*if c.Connected && !c.Retry && !c.Closed {
			c.result <- ClientResult{Id: runId, Data: result, Error: err}
		} else {
			time.Sleep(1 * time.Second)
		}*/
	}
	//close(c.result)
	//log.Println("processDo process channel has closed")
}

func ArrayAppendToFirst(src []interface{}, dst []interface{}) []interface{} {
	tmp := src
	tmp = append(tmp, dst...)
	return tmp
}

func (c *Client) Do(args ...interface{}) ([]string, error) {
	if c.Connected && !c.Retry && !c.Closed {
		runId := fmt.Sprintf("%d", time.Now().UnixNano())
		args = ArrayAppendToFirst([]interface{}{runId}, args)
		c.process <- args
		for result := range c.result {
			if result.Id == runId {
				return result.Data, result.Error
			} else {
				c.result <- result
			}
		}
	}
	return nil, fmt.Errorf("Connection has closed.")
}

func (c *Client) do(args []interface{}) ([]string, error) {
	if c.Connected {
		err := c.send(args)
		if err != nil {
			if debug {
				log.Printf("SSDB Client[%s] Do Send Error:%v Data:%v\n", c.Id, err, args)
			}
			c.CheckError(err)
			return nil, err
		}
		resp, err := c.recv()
		if err != nil {
			if debug {
				log.Printf("SSDB Client[%s] Do Receive Error:%v Data:%v\n", c.Id, err, args)
			}
			c.CheckError(err)
			return nil, err
		}
		return resp, nil
	}
	return nil, fmt.Errorf("lost connection")
}

func (c *Client) ProcessCmd(cmd string, args []interface{}) (interface{}, error) {
	if c.Connected {
		args = ArrayAppendToFirst([]interface{}{cmd}, args)
		runId := fmt.Sprintf("%d", time.Now().UnixNano())
		args = ArrayAppendToFirst([]interface{}{runId}, args)
		var err error
		c.process <- args
		var resResult ClientResult
		for result := range c.result {
			if result.Id == runId {
				resResult = result
				break
			} else {
				c.result <- result
			}
		}
		if resResult.Error != nil {
			return nil, resResult.Error
		}

		resp := resResult.Data
		if len(resp) == 2 && resp[0] == "ok" {
			switch cmd {
			case "set", "del":
				return true, nil
			case "expire", "setnx", "auth", "exists", "hexists":
				if resp[1] == "1" {
					return true, nil
				}
				return false, nil
			case "hsize":
				val, err := strconv.ParseInt(resp[1], 10, 64)
				return val, err
			default:
				return resp[1], nil
			}

		} else if len(resp) == 1 && resp[0] == "not_found" {
			return nil, fmt.Errorf("%v", resp[0])
		} else {
			if len(resp) >= 1 && resp[0] == "ok" {
				//fmt.Println("Process:",args,resp)
				switch cmd {
				case "hgetall", "hscan", "hrscan", "multi_hget", "scan", "rscan":
					list := make(map[string]string)
					length := len(resp[1:])
					data := resp[1:]
					for i := 0; i < length; i += 2 {
						list[data[i]] = data[i+1]
					}
					return list, nil
				default:
					return resp[1:], nil
				}
			}
		}
		if len(resp) == 2 && strings.Contains(resp[1], "connection") {
			c.sock.Close()
			go c.RetryConnect()
		}
		log.Printf("SSDB Client Error Response:%v args:%v Error:%v", resp, args, err)
		return nil, fmt.Errorf("bad response:%v args:%v", resp, args)
	} else {
		return nil, fmt.Errorf("lost connection")
	}
}

func (c *Client) Auth(pwd string) (interface{}, error) {
	return c.Do("auth", pwd)
	//return c.ProcessCmd("auth",params)
}

func (c *Client) Set(key string, val string) (interface{}, error) {
	params := []interface{}{key, val}
	return c.ProcessCmd("set", params)
}

func (c *Client) Get(key string) (interface{}, error) {
	params := []interface{}{key}
	return c.ProcessCmd("get", params)
}

func (c *Client) Del(key string) (interface{}, error) {
	params := []interface{}{key}
	return c.ProcessCmd("del", params)
}

func (c *Client) SetX(key string, val string, ttl int) (interface{}, error) {
	params := []interface{}{key, val, ttl}
	return c.ProcessCmd("setx", params)
}

func (c *Client) Scan(start string, end string, limit int) (interface{}, error) {
	params := []interface{}{start, end, limit}
	return c.ProcessCmd("scan", params)
}

func (c *Client) Expire(key string, ttl int) (interface{}, error) {
	params := []interface{}{key, ttl}
	return c.ProcessCmd("expire", params)
}

func (c *Client) KeyTTL(key string) (interface{}, error) {
	params := []interface{}{key}
	return c.ProcessCmd("ttl", params)
}

//set new key if key exists then ignore this operation
func (c *Client) SetNew(key string, val string) (interface{}, error) {
	params := []interface{}{key, val}
	return c.ProcessCmd("setnx", params)
}

//
func (c *Client) GetSet(key string, val string) (interface{}, error) {
	params := []interface{}{key, val}
	return c.ProcessCmd("getset", params)
}

//incr num to exist number value
func (c *Client) Incr(key string, val int) (interface{}, error) {
	params := []interface{}{key, val}
	return c.ProcessCmd("incr", params)
}

func (c *Client) Exists(key string) (interface{}, error) {
	params := []interface{}{key}
	return c.ProcessCmd("exists", params)
}

func (c *Client) HashSet(hash string, key string, val string) (interface{}, error) {
	params := []interface{}{hash, key, val}
	return c.ProcessCmd("hset", params)
}

// ------  added by Dixen for multi connections Hashset function

func conHelper(chunk []HashData, wg *sync.WaitGroup, c *Client, results []interface{}, errs []error) {
	defer wg.Done()
	fmt.Printf("go - %v\n", time.Now())
	for _, v := range chunk {
		params := []interface{}{v.HashName, v.Key, v.Value}
		res, err := c.ProcessCmd("hset", params)
		if err != nil {
			errs = append(errs, err)
			break
		}
		results = append(results, res)
	}
	fmt.Printf("so - %v\n", time.Now())
}

func (c *Client) MultiHashSet(parts []HashData, connNum int) (interface{}, error) {
	var privatePool []*Client
	for i := 0; i < connNum-1; i++ {
		innerClient, _ := Connect(c.Ip, c.Port, c.Password)
		privatePool = append(privatePool, innerClient)
	}
	privatePool = append(privatePool, c)
	var results []interface{}
	var errs []error
	var wg sync.WaitGroup
	wg.Add(connNum)
	p := len(parts) / connNum
	for i := 1; i <= connNum; i++ {
		if i == 1 {
			go conHelper(parts[:p*i], &wg, privatePool[i-1], results, errs)
		} else if i == connNum {
			go conHelper(parts[p*(i-1):], &wg, privatePool[i-1], results, errs)
		} else {
			go conHelper(parts[p*(i-1):p*i], &wg, privatePool[i-1], results, errs)
		}

	}
	wg.Wait()
	for _, c := range privatePool[:connNum-1] {
		c.Close()
	}
	if len(errs) > 0 {
		return nil, errs[0]
	}
	return results, nil
}

func (c *Client) MultiMode(args [][]interface{}) ([]string, error) {
	if c.Connected {
		for _, v := range args {
			err := c.send(v)
			if err != nil {
				log.Printf("SSDB Client[%s] Do Send Error:%v Data:%v\n", c.Id, err, args)
				c.CheckError(err)
				return nil, err
			}
		}
		var resps []string
		for i := 0; i < len(args); i++ {
			resp, err := c.recv()
			if err != nil {
				log.Printf("SSDB Client[%s] Do Receive Error:%v Data:%v\n", c.Id, err, args)
				c.CheckError(err)
				return nil, err
			}
			resps = append(resps, strings.Join(resp, ","))
		}
		return resps, nil
	}
	return nil, fmt.Errorf("lost connection")
}

func (c *Client) HashGet(hash string, key string) (interface{}, error) {
	params := []interface{}{hash, key}
	return c.ProcessCmd("hget", params)
}

func (c *Client) HashDel(hash string, key string) (interface{}, error) {
	params := []interface{}{hash, key}
	return c.ProcessCmd("hdel", params)
}

func (c *Client) HashIncr(hash string, key string, val int) (interface{}, error) {
	params := []interface{}{hash, key, val}
	return c.ProcessCmd("hincr", params)
}

func (c *Client) HashExists(hash string, key string) (interface{}, error) {
	params := []interface{}{hash, key}
	return c.ProcessCmd("hexists", params)
}

func (c *Client) HashSize(hash string) (interface{}, error) {
	params := []interface{}{hash}
	return c.ProcessCmd("hsize", params)
}

//search from start to end hashmap name or haskmap key name,except start word
func (c *Client) HashList(start string, end string, limit int) (interface{}, error) {
	params := []interface{}{start, end, limit}
	return c.ProcessCmd("hlist", params)
}

func (c *Client) HashKeys(hash string, start string, end string, limit int) (interface{}, error) {
	params := []interface{}{hash, start, end, limit}
	return c.ProcessCmd("hkeys", params)
}
func (c *Client) HashKeysAll(hash string) ([]string, error) {
	size, err := c.HashSize(hash)
	if err != nil {
		return nil, err
	}
	log.Printf("DB Hash Size:%d\n", size)
	hashSize := size.(int64)
	page_range := 15
	splitSize := math.Ceil(float64(hashSize) / float64(page_range))
	log.Printf("DB Hash Size:%d hashSize:%d splitSize:%f\n", size, hashSize, splitSize)
	var range_keys []string
	for i := 1; i <= int(splitSize); i++ {
		start := ""
		end := ""
		if len(range_keys) != 0 {
			start = range_keys[len(range_keys)-1]
			end = ""
		}

		val, err := c.HashKeys(hash, start, end, page_range)
		if err != nil {
			log.Println("HashGetAll Error:", err)
			continue
		}
		if val == nil {
			continue
		}
		//log.Println("HashGetAll type:",reflect.TypeOf(val))
		var data []string
		if fmt.Sprintf("%v", reflect.TypeOf(val)) == "string" {
			data = append(data, val.(string))
		} else {
			data = val.([]string)
		}

		if len(data) > 0 {
			range_keys = append(range_keys, data...)
		}

	}
	log.Printf("DB Hash Keys Size:%d\n", len(range_keys))
	return range_keys, nil
}

func (c *Client) HashGetAll(hash string) (map[string]string, error) {
	params := []interface{}{hash}
	val, err := c.ProcessCmd("hgetall", params)
	if err != nil {
		return nil, err
	} else {
		return val.(map[string]string), err
	}

	return nil, nil
}

func (c *Client) HashGetAllLite(hash string) (map[string]string, error) {
	size, err := c.HashSize(hash)
	if err != nil {
		return nil, err
	}
	//log.Printf("DB Hash Size:%d\n",size)
	hashSize := size.(int64)
	page_range := 20
	splitSize := math.Ceil(float64(hashSize) / float64(page_range))
	//log.Printf("DB Hash Size:%d hashSize:%d splitSize:%f\n",size,hashSize,splitSize)
	var range_keys []string
	GetResult := make(map[string]string)
	for i := 1; i <= int(splitSize); i++ {
		start := ""
		end := ""
		if len(range_keys) != 0 {
			start = range_keys[len(range_keys)-1]
			end = ""
		}

		val, err := c.HashKeys(hash, start, end, page_range)
		if err != nil {
			log.Println("HashGetAll Error:", err)
			continue
		}
		if val == nil {
			continue
		}
		//log.Println("HashGetAll type:",reflect.TypeOf(val))
		var data []string
		if fmt.Sprintf("%v", reflect.TypeOf(val)) == "string" {
			data = append(data, val.(string))
		} else {
			data = val.([]string)
		}
		range_keys = data
		if len(data) > 0 {
			result, err := c.HashMultiGet(hash, data)
			if err != nil {
				log.Println("HashGetAll Error:", err)
			}
			if result == nil {
				continue
			}
			for k, v := range result {
				GetResult[k] = v
			}
		}

	}

	return GetResult, nil
}

func (c *Client) HashScan(hash string, start string, end string, limit int) (map[string]string, error) {
	params := []interface{}{hash, start, end, limit}
	val, err := c.ProcessCmd("hscan", params)
	if err != nil {
		return nil, err
	} else {
		return val.(map[string]string), err
	}

	return nil, nil
}

func (c *Client) HashRScan(hash string, start string, end string, limit int) (map[string]string, error) {
	params := []interface{}{hash, start, end, limit}
	val, err := c.ProcessCmd("hrscan", params)
	if err != nil {
		return nil, err
	} else {
		return val.(map[string]string), err
	}
	return nil, nil
}

func (c *Client) HashMultiSet(hash string, data map[string]string) (interface{}, error) {
	params := []interface{}{hash}
	for k, v := range data {
		params = append(params, k)
		params = append(params, v)
	}
	return c.ProcessCmd("multi_hset", params)
}

func (c *Client) HashMultiGet(hash string, keys []string) (map[string]string, error) {
	params := []interface{}{hash}
	for _, v := range keys {
		params = append(params, v)
	}
	val, err := c.ProcessCmd("multi_hget", params)
	if err != nil {
		return nil, err
	} else {
		return val.(map[string]string), err
	}
	return nil, nil
}

func (c *Client) HashMultiDel(hash string, keys []string) (interface{}, error) {
	params := []interface{}{hash}
	for _, v := range keys {
		params = append(params, v)
	}
	return c.ProcessCmd("multi_hdel", params)
}

func (c *Client) HashClear(hash string) (interface{}, error) {
	params := []interface{}{hash}
	return c.ProcessCmd("hclear", params)
}

func (c *Client) Send(args ...interface{}) error {
	return c.send(args)
}

func (c *Client) send(args []interface{}) error {
	var buf bytes.Buffer
	for _, arg := range args {
		var s string
		switch arg := arg.(type) {
		case string:
			s = arg
		case []byte:
			s = string(arg)
		case []string:
			for _, s := range arg {
				buf.WriteString(fmt.Sprintf("%d", len(s)))
				buf.WriteByte('\n')
				buf.WriteString(s)
				buf.WriteByte('\n')
			}
			continue
		case int:
			s = fmt.Sprintf("%d", arg)
		case int64:
			s = fmt.Sprintf("%d", arg)
		case float64:
			s = fmt.Sprintf("%f", arg)
		case bool:
			if arg {
				s = "1"
			} else {
				s = "0"
			}
		case nil:
			s = ""
		default:
			return fmt.Errorf("bad arguments")
		}
		buf.WriteString(fmt.Sprintf("%d", len(s)))
		buf.WriteByte('\n')
		buf.WriteString(s)
		buf.WriteByte('\n')
	}
	buf.WriteByte('\n')
	_, err := c.sock.Write(buf.Bytes())
	return err
}

func (c *Client) Recv() ([]string, error) {
	return c.recv()
}

func (c *Client) recv() ([]string, error) {
	//tmp := make([]byte, 102400)
	var tmp [102400]byte
	for {
		resp := c.parse()
		if resp == nil || len(resp) > 0 {
			//log.Println("SSDB Receive:",resp)
			if len(resp) > 0 && resp[0] == "zip" {
				//log.Println("SSDB Receive Zip\n",resp)
				zipData, err := base64.StdEncoding.DecodeString(resp[1])
				if err != nil {
					return nil, err
				}
				resp = c.UnZip(zipData)
			}
			return resp, nil
		}
		n, err := c.sock.Read(tmp[0:])
		if err != nil {
			return nil, err
		}
		c.recv_buf.Write(tmp[0:n])
	}
}

func (c *Client) parse() []string {
	resp := []string{}
	buf := c.recv_buf.Bytes()
	var Idx, offset int
	Idx = 0
	offset = 0
	for {
		Idx = bytes.IndexByte(buf[offset:], '\n')
		if Idx == -1 {
			break
		}
		p := buf[offset : offset+Idx]
		offset += Idx + 1
		//fmt.Printf("> [%s]\n", p);
		if len(p) == 0 || (len(p) == 1 && p[0] == '\r') {
			if len(resp) == 0 {
				continue
			} else {
				c.recv_buf.Next(offset)
				return resp
			}
		}
		pIdx := strings.Replace(strconv.Quote(string(p)), `"`, ``, -1)
		size, err := strconv.Atoi(pIdx)
		if err != nil || size < 0 {
			//log.Printf("SSDB Parse Error:%v data:%v\n",err,pIdx)
			return nil
		}
		//fmt.Printf("packet size:%d\n",size);
		if offset+size >= c.recv_buf.Len() {
			//tmpLen := offset+size
			//fmt.Printf("buf size too big:%d > buf len:%d\n",tmpLen,c.recv_buf.Len());
			break
		}

		v := buf[offset : offset+size]
		resp = append(resp, string(v))
		offset += size + 1
	}

	//fmt.Printf("buf.size: %d packet not ready...\n", len(buf))
	return []string{}
}

func (c *Client) UnZip(data []byte) []string {
	var buf bytes.Buffer
	buf.Write(data)
	zipReader, err := gzip.NewReader(&buf)
	if err != nil {
		log.Println("[ERROR] New gzip reader:", err)
	}
	defer zipReader.Close()

	zipData, err := ioutil.ReadAll(zipReader)
	if err != nil {
		fmt.Println("[ERROR] ReadAll:", err)
		return nil
	}
	var resp []string

	if zipData != nil {
		Idx := 0
		offset := 0
		hiIdx := 0
		for {
			Idx = bytes.IndexByte(zipData, '\n')
			if Idx == -1 {
				break
			}
			p := string(zipData[:Idx])
			//fmt.Println("p:[",p,"]\n")
			size, err := strconv.Atoi(string(p))
			if err != nil || size < 0 {
				zipData = zipData[Idx+1:]
				continue
			} else {
				offset = Idx + 1 + size
				hiIdx = size + Idx + 1
				resp = append(resp, string(zipData[Idx+1:hiIdx]))
				//fmt.Printf("data:[%s] size:%d Idx:%d\n",str,size,Idx+1)
				zipData = zipData[offset:]
			}

		}
	}
	return resp
}

// Close The Client Connection
func (c *Client) Close() error {
	if !c.Closed {
		c.mu.Lock()
		c.Connected = false
		c.Closed = true
		c.mu.Unlock()
		close(c.process)
		c.sock.Close()
		c = nil
	}
	return nil
}
