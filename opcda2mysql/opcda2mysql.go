package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode"

	"github.com/BurntSushi/toml"
	_ "github.com/go-sql-driver/mysql"
	"github.com/huskar-t/opcda"
	"github.com/huskar-t/opcda/com"
	"github.com/pkg/errors"
)

type Config struct {
	Opcda struct {
		Host   string
		ProgID string
	}
	Mysql struct {
		Host     string
		Port     int
		ID       string
		Username string
		Password string
	}
	Reconnect struct {
		Delay time.Duration
	}
}

var config Config

func trimInvisible(s string) string {
	start := 0
	for start < len(s) && unicode.IsSpace(rune(s[start])) {
		start++
	}
	end := len(s) - 1
	for end >= 0 && unicode.IsSpace(rune(s[end])) {
		end--
	}
	if end < start {
		return ""
	}
	return s[start : end+1]
}

// 从配置文件中读取
func loadConfig(configFile string) error {
	_, err := toml.DecodeFile(configFile, &config)
	if err != nil {
		return errors.Wrap(err, "failed to decode config file")
	}
	return nil
}

func readTagsFromCSV(filePath string) ([]string, error) {
	content, err := ioutil.ReadFile(filePath)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read CSV file")
	}

	tags := make([]string, 0)
	lines := strings.Split(string(content), "\n")
	for _, line := range lines {
		if line != "" {
			tags = append(tags, line)
		}
	}

	return tags, nil
}

// 定义一个数据队列
type DataQueue struct {
	data []string
	sync.Mutex
}

func (q *DataQueue) Enqueue(d string) {
	q.Mutex.Lock()
	q.data = append(q.data, d)
	q.Mutex.Unlock()
}

func (q *DataQueue) Dequeue() (string, bool) {
	q.Mutex.Lock()
	defer q.Mutex.Unlock()
	if len(q.data) == 0 {
		return "", false
	}
	val := q.data[0]
	q.data = q.data[1:]
	return val, true
}

var (
	stopChan  = make(chan struct{})
	dataQueue = DataQueue{}
)

// mqttPUB数据结构体
type PUBData struct {
	TagID     string `json:"tag_id"`
	Timestamp string `json:"data_timestamp"`
	Quality   uint8  `json:"data_quality"`
	Value     string `json:"value"`
}

// 读取OPCDA数据的函数
func readOPCDAData(config Config, tags []string) {
	com.Initialize()
	defer com.Uninitialize()
	host := config.Opcda.Host
	progID := config.Opcda.ProgID
	server, err := opcda.Connect(progID, host)
	if err != nil {
		log.Fatalf("connect to opc server failed: %s\n", err)
	}
	defer server.Disconnect()
	groups := server.GetOPCGroups()
	group, err := groups.Add("group1")
	if err != nil {
		log.Fatalf("add group failed: %s\n", err)
	}
	items := group.OPCItems()
	itemList, errs, err := items.AddItems(tags)
	if err != nil {
		log.Fatalf("add items failed: %s\n", err)
	}
	for i, err := range errs {
		if err != nil {
			log.Fatalf("add item %s failed: %s\n", tags[i], err)
		}
	}
	// Wait for the OPC server to be ready
	time.Sleep(time.Second * 2)
	ch := make(chan *opcda.DataChangeCallBackData, 100)
	go func() {
		for {
			select {

			case data := <-ch:
				fmt.Printf("data change received, transaction id: %d, group handle: %d, masterQuality: %d, masterError: %v\n", data.TransID, data.GroupHandle, data.MasterQuality, data.MasterErr)
				for i := 0; i < len(data.ItemClientHandles); i++ {
					tag := ""
					for _, item := range itemList {
						//fmt.Printf("%d, %d, %s\n", data.ItemClientHandles[i], item.GetClientHandle(), item.GetItemID())
						if item.GetClientHandle() == data.ItemClientHandles[i] {
							tag = trimInvisible(item.GetItemID())
						}
					}
					// 将 data.Values[i] 转换为字符串
					valueStr := fmt.Sprintf("%v", data.Values[i])
					timestampstr := data.TimeStamps[i].Format("2006-01-02 15:04:05")
					quality := uint8(data.Qualities[i])
					//fmt.Printf("json data:: %s, %s, %d, %s \n", item.GetItemID(), timestampstr, quality, valueStr)
					opcdaData := PUBData{
						TagID:     tag,
						Timestamp: timestampstr,
						Quality:   quality,
						Value:     valueStr,
					}
					jsonData, err := json.Marshal(opcdaData)
					if err != nil {
						fmt.Printf("Error marshaling Modbus data to JSON: %v\n", err)
					}

					dataQueue.Enqueue(string(jsonData))
					fmt.Printf("item: %s, timestamp: %s, quality: %d, value: %v\n", tag, data.TimeStamps[i], data.Qualities[i], data.Values[i])
				}

			}
		}
	}()
	err = group.RegisterDataChange(ch)
	if err != nil {
		log.Fatalf("register data change failed: %s\n", err)
	}
	log.Println("Registered data change in OPCDA")
	select {
	case <-stopChan:
		group.Release()
		log.Println("Received stop signal. Exiting readOPCDAData goroutine.")
		err := server.Disconnect()
		if err != nil {
			return
		} // 断开连接
		return
	}

}

// 从队列读取数据写入MySQL的函数
func writeMySQLData(config Config) {
	// 连接数据库
	fmt.Printf("%s:%s@tcp(%s:%d)/\n", config.Mysql.Username, config.Mysql.Password, config.Mysql.Host, config.Mysql.Port)
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/", config.Mysql.Username, config.Mysql.Password, config.Mysql.Host, config.Mysql.Port)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		fmt.Printf("failed to connect to database: %v", err)
		return
	}
	defer db.Close()

	// 创建数据库
	_, err = db.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", config.Mysql.ID))
	if err != nil {
		fmt.Printf("failed to create database: %v", err)
		return
	}

	// 选择数据库
	_, err = db.Exec(fmt.Sprintf("USE %s", config.Mysql.ID))
	if err != nil {
		log.Printf("failed to select database: %v", err)
		return
	}

	// 创建表
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS opcdata (
        id INT AUTO_INCREMENT PRIMARY KEY,
        tag_id VARCHAR(255) NOT NULL,
        data_timestamp DATETIME NOT NULL,
        data_quality INT NOT NULL,
        value VARCHAR(255) NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )`)
	if err != nil {
		log.Printf("failed to create table: %v", err)
		return
	}

	// 从队列中读取数据并写入数据库
	for {
		select {
		case <-stopChan:
			log.Println("Received stop signal. Exiting writeMySQLData goroutine.")
			return
		default:
			data, ok := dataQueue.Dequeue()
			if !ok {
				time.Sleep(100 * time.Millisecond)
				continue
			}
			// 解析JSON数据
			var pubData PUBData
			err := json.Unmarshal([]byte(data), &pubData)
			if err != nil {
				log.Printf("failed to unmarshal JSON: %v", err)
				continue
			}
			// 插入数据到数据库
			_, err = db.Exec("INSERT INTO opcdata (tag_id, data_timestamp, data_quality, value) VALUES (?,?,?,?)",
				pubData.TagID, pubData.Timestamp, pubData.Quality, pubData.Value)
			if err != nil {
				log.Printf("failed to insert data into database: %v", err)
			} else {
				fmt.Printf("Tagid %s data inserted  into table opcdata successfully\n", pubData.TagID)
			}
		}
	}
}

func main() {
	var wg sync.WaitGroup

	// 加载配置文件
	err := loadConfig("config.toml")
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	// 读取 tags 从 CSV 文件
	tags, err := readTagsFromCSV("opcdatags.csv")
	if err != nil {
		log.Fatalf("failed to read tags from CSV: %v", err)
	}

	// 启动 readOPCDAData 线程
	go func() {
		defer wg.Done()
		readOPCDAData(config, tags)
	}()

	// 启动MySQL数据写入线程
	go func() {
		defer wg.Done()
		writeMySQLData(config)
	}()

	// 等待子线程启动
	time.Sleep(time.Second * 2)

	// 捕获系统信号，用于优雅退出
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-signals
		fmt.Println()
		fmt.Println(sig)
		fmt.Println("Received termination, shutting down gracefully...")
		close(stopChan)
		wg.Wait()
		os.Exit(0)
	}()

	// 防止主线程退出
	select {}
}
