package ssdb

import (
	"bytes"
	"fmt"
	"net"
	"strconv"
	"sync"
	"io"
	"time"
	"math"
	"reflect"
	_"syscall"
	"strings"
	"log"
)

type UnixClient struct {
	sock *net.UnixConn
	recv_buf bytes.Buffer
	reuse	bool
	id int64
	Ip string
	Port int
	Password string
	connected bool
	last_time int64
	success int64
	count int64
	mu	*sync.Mutex
}

type SSDBUnix struct {
	mu *sync.Mutex
	connect_pool []*UnixClient
	max_connect int
	ip string
	port int
	timeout int
}
var SSDBUnixM *SSDBUnix
var unixVersion string = "0.1.1"
func SSDBUnixInit(ip string, port int,max_connect int) *SSDBUnix {
	log.Println("SSDBUnix Client Version:",unixVersion)
	return &SSDBUnix{max_connect:max_connect,ip:ip,port:port,timeout:30,mu:&sync.Mutex{}}
}

func (db *SSDBUnix) Recycle() {
	go func() {
		for {
			now := time.Now().Unix()
			db.mu.Lock()
			remove_count := 0
			for i := len(db.connect_pool)-1;i >= 0;i-- {
				v := db.connect_pool[i]
				if !v.connected || now - v.last_time > int64(db.timeout) {
					v.Close()
					remove_count++
					if len(db.connect_pool) > 1 {
						db.connect_pool = append(db.connect_pool[:i], db.connect_pool[i+1:]...)
					} else {
						db.connect_pool = nil
					}	
				}
			}
			db.mu.Unlock()
			time.Sleep(10 * time.Second)
		}
	}()
}


func (db *SSDBUnix) Info() {
	var use,nouse int
	var count,success,close_count int64
	for _,v := range SSDBUnixM.connect_pool {
		//fmt.Printf("[%d][status]:%v\n",k,v.reuse)
		if v.reuse {
			nouse++
		} else {
			use++
		}
		
		if !v.connected {
			close_count++
		}
		count += v.count
		success += v.success
	}
	failed := count - success
	
	now_time:=time.Now().Format(layout)
	fmt.Printf("[%s] SSDBUnixM Info[IP]:%v [Port]:%v [Max]:%v [Pool]:%v [Use]:%v [NoUse]:%v [Close]:%v [Total]:%v [Success]:%v [Failed]:%v\n",now_time,db.ip,db.port,db.max_connect,len(SSDBUnixM.connect_pool),use,nouse,close_count,count,success,failed)
	
}

func UnixConnect(ip string, port int, auth string) (*UnixClient, error) {
	if SSDBUnixM == nil {
		SSDBUnixM = SSDBUnixInit(ip,port,100)
		//SSDBUnixM.Recycle()
	}
	/*SSDBUnixM.mu.Lock()
	for i,v := range SSDBUnixM.connect_pool {
		if v.reuse && v.connected {
			v.mu.Lock()
			v.reuse = true
			v.mu.Unlock()
			return v,nil
		} else if !v.connected {
			SSDBUnixM.connect_pool = append(SSDBUnixM.connect_pool[:i], SSDBUnixM.connect_pool[i+1:]...)
		}
	}
	SSDBUnixM.mu.Unlock()*/
	client,err := Unixconnect(ip,port,auth)
	if err != nil {
		go client.RetryConnect()
		return client,err
	}
	if client != nil {
		SSDBUnixM.connect_pool = append(SSDBUnixM.connect_pool,client)
		client.id = time.Now().UnixNano()
		return client,nil
	}
	return nil,nil
}

func Unixconnect(ip string, port int,auth string) (*UnixClient, error) {
	var c UnixClient
	c.Ip = ip
	c.Port = port
	c.Password = auth
	c.mu = &sync.Mutex{}
	err := c.Connect()
	return &c, err
}

func (c *UnixClient) Connect() error {
	types := "unix" // or "unixgram" or "unixpacket"
	//laddr := net.UnixAddr{"/tmp/ssdbcli", types}
	sock, err := net.DialUnix(types, nil,&net.UnixAddr{c.Ip, types})
	if err != nil {
	    log.Println("Client dial failed:",err)
		return err
	}   
	
	c.last_time = time.Now().Unix()
	c.sock = sock
	c.reuse = true
	c.connected = true
	if c.Password != "" {
    	c.Auth(c.Password)
    }
	//log.Println("Client connected to ",c.Ip, c.Port)
	return nil
}

func (cl *UnixClient) RetryConnect() {
	log.Println("Client retry connect to ",cl.Ip, cl.Port)
	for {
		if !cl.connected {
			cl.Connect()
			time.Sleep(10 * time.Second)
		} else {
			break
		}
	}
}

func (c *UnixClient) CheckError(err error) {
	 if err == io.EOF || strings.Contains(err.Error(), "connection") || strings.Contains(err.Error(), "timed out" ) || strings.Contains(err.Error(), "route" ) {
         c.Close()
         go c.RetryConnect()
     }
}

func (c *UnixClient) Do(args ...interface{}) ([]string, error) {
	 c.mu.Lock()
     defer c.mu.Unlock()
     c.reuse = false
     c.count++
     err := c.send(args)
     if err != nil {
         log.Println("SSDB Client Do send error:",err)
         c.CheckError(err)
         return nil, err
     }
     resp, err := c.recv()
     if err != nil {
           log.Println("SSDB Client Do recv error:",err)
           c.CheckError(err)
	      return nil, err
     }
     c.success++
     c.reuse = true
     /*if resp[0] == "error" {
     	err = fmt.Errorf("bad response:%v",resp[1])
     }*/
     if err != nil {
     	return nil,err
     }
     return resp, err
}


func (c *UnixClient) ProcessCmd(cmd string,args []interface{}) (interface{}, error) {
	if c.connected {
	    c.last_time = time.Now().Unix()
	    args = append(args,nil)
	    // Use copy to move the upper part of the slice out of the way and open a hole.
	    copy(args[1:], args[0:])
	    // Store the cmd to args
	    args[0] = cmd
	    c.mu.Lock()
		defer c.mu.Unlock()
		c.reuse = false
		c.count++
		err := c.send(args)
		if err != nil {
			log.Println("SSDB Client ProcessCmd send error:",err)
			c.CheckError(err)
			return nil, err
		}
		resp, err := c.recv()
		if err != nil {
			log.Println("SSDB Client ProcessCmd receive error:",err)
			c.CheckError(err)
			return nil, err
		}
		//log.Println("Process:",args,resp)
		c.success++
		c.reuse = true
		if len(resp) == 2 && resp[0] == "ok" {
			switch cmd {
				case "set","del":
					return true, nil
				case "expire","setnx","auth","exists","hexists":
					if resp[1] == "1" {
					 return true,nil
					}	
					return false,nil
				case "hsize":
					val,err := strconv.ParseInt(resp[1],10,64)
					return val,err
				default:
					return resp[1], nil
			}
			
		}else if resp[0] == "not_found" {
			return nil, nil
		} else {
			if resp[0] == "ok" {
				//fmt.Println("Process:",args,resp)
				switch cmd {
					case "hgetall","hscan","hrscan","multi_hget","scan","rscan":
						list := make(map[string]interface{})
						length := len(resp[1:])
						data := resp[1:]
						for i := 0; i < length; i += 2 {
							list[data[i]] = data[i+1]
						}
						return list,nil
					default:
						return resp[1:],nil
				}
			}
		}
		
		return nil, fmt.Errorf("bad response:%v",resp)
	} else {
		return nil, fmt.Errorf("lost connection")
	}
}

func (c *UnixClient) Auth(pwd string) (interface{}, error) {
	params := []interface{}{pwd}
	return c.ProcessCmd("auth",params)
}

func (c *UnixClient) Set(key string, val string) (interface{}, error) {
	params := []interface{}{key,val}
	return c.ProcessCmd("set",params)
}

func (c *UnixClient) Get(key string) (interface{}, error) {
	params := []interface{}{key}
	return c.ProcessCmd("get",params)
}

func (c *UnixClient) Del(key string) (interface{}, error) {
	params := []interface{}{key}
	return c.ProcessCmd("del",params)
}

func (c *UnixClient) SetX(key string,val string, ttl int) (interface{}, error) {
	params := []interface{}{key,val,ttl}
	return c.ProcessCmd("setx",params)
}

func (c *UnixClient) Scan(start string,end string,limit int) (interface{}, error) {
	params := []interface{}{start,end,limit}
	return c.ProcessCmd("scan",params)
}

func (c *UnixClient) Expire(key string,ttl int) (interface{}, error) {
	params := []interface{}{key,ttl}
	return c.ProcessCmd("expire",params)
}

func (c *UnixClient) KeyTTL(key string) (interface{}, error) {
	params := []interface{}{key}
	return c.ProcessCmd("ttl",params)
}

//set new key if key exists then ignore this operation
func (c *UnixClient) SetNew(key string,val string) (interface{}, error) {
	params := []interface{}{key,val}
	return c.ProcessCmd("setnx",params)
}

//
func (c *UnixClient) GetSet(key string,val string) (interface{}, error) {
	params := []interface{}{key,val}
	return c.ProcessCmd("getset",params)
}

//incr num to exist number value
func (c *UnixClient) Incr(key string,val int) (interface{}, error) {
	params := []interface{}{key,val}
	return c.ProcessCmd("incr",params)
}

func (c *UnixClient) Exists(key string) (interface{}, error) {
	params := []interface{}{key}
	return c.ProcessCmd("exists",params)
}

func (c *UnixClient) HashSet(hash string,key string,val string) (interface{}, error) {
	params := []interface{}{hash,key,val}
	return c.ProcessCmd("hset",params)
}

func (c *UnixClient) HashGet(hash string,key string) (interface{}, error) {
	params := []interface{}{hash,key}
	return c.ProcessCmd("hget",params)
}

func (c *UnixClient) HashDel(hash string,key string) (interface{}, error) {
	params := []interface{}{hash,key}
	return c.ProcessCmd("hdel",params)
}

func (c *UnixClient) HashIncr(hash string,key string,val int) (interface{}, error) {
	params := []interface{}{hash,key,val}
	return c.ProcessCmd("hincr",params)
}

func (c *UnixClient) HashExists(hash string,key string) (interface{}, error) {
	params := []interface{}{hash,key}
	return c.ProcessCmd("hexists",params)
}

func (c *UnixClient) HashSize(hash string) (interface{}, error) {
	params := []interface{}{hash}
	return c.ProcessCmd("hsize",params)
}

//search from start to end hashmap name or haskmap key name,except start word
func (c *UnixClient) HashList(start string,end string,limit int) (interface{}, error) {
	params := []interface{}{start,end,limit}
	return c.ProcessCmd("hlist",params)
}

func (c *UnixClient) HashKeys(hash string,start string,end string,limit int) (interface{}, error) {
	params := []interface{}{hash,start,end,limit}
	return c.ProcessCmd("hkeys",params)
}
func (c *UnixClient) HashKeysAll(hash string) ([]string, error) {
	size,err := c.HashSize(hash)
	if err != nil {
		return nil,err
	}
	log.Printf("DB Hash Size:%d\n",size)
	hashSize := size.(int64)
	page_range := 15
	splitSize := math.Ceil(float64(hashSize)/float64(page_range))
	log.Printf("DB Hash Size:%d hashSize:%d splitSize:%f\n",size,hashSize,splitSize)
	var range_keys []string
	for i := 1;i <= int(splitSize);i++ {
		start := ""
		end := ""
		if len(range_keys) != 0 {
			start = range_keys[len(range_keys)-1]
			end = ""
		}
		
		val, err := c.HashKeys(hash,start,end,page_range) 
		if err != nil {
			log.Println("HashGetAll Error:",err)
			continue
		} 
		if val == nil {
			continue
		}
		//log.Println("HashGetAll type:",reflect.TypeOf(val))
		var data []string
		if(fmt.Sprintf("%v",reflect.TypeOf(val)) == "string"){
			data = append(data,val.(string))
		}else{
			data = val.([]string)
		}
		
		if len(data) > 0 {
			range_keys = append(range_keys,data...)
		}
		
	}
	log.Printf("DB Hash Keys Size:%d\n",len(range_keys))
	return range_keys,nil
}

func (c *UnixClient) HashGetAllLite(hash string) (map[string]string, error) {
	params := []interface{}{hash}
	val,err := c.ProcessCmd("hgetall",params)
	if err != nil {
		return nil,err
	} else {
		return val.(map[string]string),err
	}
	
	return nil,nil
}

func (c *UnixClient) HashGetAll(hash string) (map[string]interface{}, error) {
	size,err := c.HashSize(hash)
	if err != nil {
		return nil,err
	}
	//log.Printf("DB Hash Size:%d\n",size)
	hashSize := size.(int64)
	page_range := 20
	splitSize := math.Ceil(float64(hashSize)/float64(page_range))
	//log.Printf("DB Hash Size:%d hashSize:%d splitSize:%f\n",size,hashSize,splitSize)
	var range_keys []string
	GetResult := make(map[string]interface{})
	for i := 1;i <= int(splitSize);i++ {
		start := ""
		end := ""
		if len(range_keys) != 0 {
			start = range_keys[len(range_keys)-1]
			end = ""
		}
		
		val, err := c.HashKeys(hash,start,end,page_range) 
		if err != nil {
			log.Println("HashGetAll Error:",err)
			continue
		} 
		if val == nil {
			continue
		}
		//log.Println("HashGetAll type:",reflect.TypeOf(val))
		var data []string
		if(fmt.Sprintf("%v",reflect.TypeOf(val)) == "string"){
			data = append(data,val.(string))
		}else{
			data = val.([]string)
		}
		range_keys = data
		if len(data) > 0 {
			result, err := c.HashMultiGet(hash,data)
			if err != nil {	
				log.Println("HashGetAll Error:",err)
			} 
			if result == nil {
				continue
			}
			for k,v := range result {
				GetResult[k] = v
			}	
		}
		
	}

	return GetResult,nil
}

func (c *UnixClient) HashScan(hash string,start string,end string,limit int) (map[string]interface{}, error) {
	params := []interface{}{hash,start,end,limit}
	val,err := c.ProcessCmd("hscan",params)
	if err != nil {
		return nil,err
	} else {
		return val.(map[string]interface{}),err
	}
	
	return nil,nil
}

func (c *UnixClient) HashRScan(hash string,start string,end string,limit int) (map[string]interface{}, error) {
	params := []interface{}{hash,start,end,limit}
	val,err := c.ProcessCmd("hrscan",params)
	if err != nil {
		return nil,err
	} else {
		return val.(map[string]interface{}),err
	}
	return nil,nil
}

func (c *UnixClient) HashMultiSet(hash string,data map[string]string) (interface{}, error) {
	params := []interface{}{hash}
	for k,v := range data {
		params = append(params,k)
		params = append(params,v)
	}
	return c.ProcessCmd("multi_hset",params)
}

func (c *UnixClient) HashMultiGet(hash string,keys []string) (map[string]interface{}, error) {
	params := []interface{}{hash}
	for _,v := range keys {
		params = append(params, v)
	}
	val,err := c.ProcessCmd("multi_hget",params)
	if err != nil {
		return nil,err
	} else {
		return val.(map[string]interface{}),err
	}
	return nil,nil
}

func (c *UnixClient) HashMultiDel(hash string,keys []string) (interface{}, error) {
	params := []interface{}{hash}
	for _,v := range keys {
		params = append(params, v)
	}
	return c.ProcessCmd("multi_hdel",params)
}


func (c *UnixClient) HashClear(hash string) (interface{}, error) {
	params := []interface{}{hash}
	return c.ProcessCmd("hclear",params)
}


func (c *UnixClient) Send(args ...interface{}) error {
	return c.send(args);
}

func (c *UnixClient) send(args []interface{}) error {
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

func (c *UnixClient) Recv() ([]string, error) {
	return c.recv();
}

func (c *UnixClient) recv() ([]string, error) {
	var tmp [1]byte
	for {
		resp := c.parse()
		if resp == nil || len(resp) > 0 {
			return resp, nil
		}
		n, err := c.sock.Read(tmp[0:])
		if err != nil {
			return nil, err
		}
		c.recv_buf.Write(tmp[0:n])
	}
}

func (c *UnixClient) parse() []string {
	resp := []string{}
	buf := c.recv_buf.Bytes()
	var idx, offset int
	idx = 0
	offset = 0

	for {
		idx = bytes.IndexByte(buf[offset:], '\n')
		if idx == -1 {
			break
		}
		p := buf[offset : offset+idx]
		offset += idx + 1
		//fmt.Printf("> [%s]\n", p);
		if len(p) == 0 || (len(p) == 1 && p[0] == '\r') {
			if len(resp) == 0 {
				continue
			} else {
				c.recv_buf.Next(offset)
				return resp
			}
		}

		size, err := strconv.Atoi(string(p))
		if err != nil || size < 0 {
			return nil
		}
		if offset+size >= c.recv_buf.Len() {
			break
		}

		v := buf[offset : offset+size]
		resp = append(resp, string(v))
		offset += size + 1
	}

	//fmt.Printf("buf.size: %d packet not ready...\n", len(buf))
	return []string{}
}


// Close The Client Connection
func (c *UnixClient) Close() error {
	c.connected = false
	c.reuse = false
	return c.sock.Close()
}