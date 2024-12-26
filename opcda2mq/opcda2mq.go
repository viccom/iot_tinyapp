package main

import (
	"encoding/json"
	"fmt"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/huskar-t/opcda"
	"github.com/huskar-t/opcda/com"
)

const (
	opcdaHost      = "localhost"
	opcdaprogID    = "Matrikon.OPC.Simulation.1"
	mqttBroker     = "mqbroker.metme.top"
	mqttPort       = 1883
	mqttID         = "mqttgo_client_id"
	mqttUsername   = "mqdevice"
	mqttPassword   = "device@metme"
	mqttTopic      = "7486E23133A9/Matrikon.OPC.Simulation.1"
	reconnectDelay = 5 * time.Second // 重连间隔时间
)

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
func readOPCDAData() {
	com.Initialize()
	defer com.Uninitialize()
	host := opcdaHost
	progID := opcdaprogID
	tags := []string{
		"Random.Real4",
		"Random.Real8",
		"Random.String",
		"Random.Time",
		"Random.UInt1",
		"Random.UInt2",
		"Random.UInt4",
		"Random.UInt8",
	}
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
						if item.GetClientHandle() == data.ItemClientHandles[i] {
							tag = item.GetItemID()
						}
					}
					// 将 data.Values[i] 转换为字符串
					valueStr := fmt.Sprintf("%v", data.Values[i])
					timestampstr := data.TimeStamps[i].Format("2006-01-02 15:04:05")
					quality := uint8(data.Qualities[i])
					//fmt.Printf("json data : %s %s %d %s \n", tag, timestampstr, quality, valueStr)
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
					fmt.Printf("item %s\ttimestamp: %s\tquality: %d\tvalue: %v\n", tag, data.TimeStamps[i], data.Qualities[i], data.Values[i])
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
		err := server.Disconnect()
		if err != nil {
			return
		} // 断开连接
		log.Println("Received stop signal. Exiting readOPCDAData goroutine.")
		return
	}

}

// 发布MQTT数据的函数
var f mqtt.MessageHandler = func(client mqtt.Client, msg mqtt.Message) {
	fmt.Printf("TOPIC: %s\n", msg.Topic())
	fmt.Printf("MSG: %s\n", msg.Payload())
}

func publishMQTTData() {
	log.Println("Starting publishMQTTData")
	opts := mqtt.NewClientOptions()
	opts.AddBroker(fmt.Sprintf("tcp://%s:%d", mqttBroker, mqttPort))
	opts.SetClientID(mqttID)
	opts.SetUsername(mqttUsername)
	opts.SetPassword(mqttPassword)
	opts.SetDefaultPublishHandler(f)

	client := mqtt.NewClient(opts)
	for {
		select {
		case <-stopChan:
			log.Println("Received stop signal in publishMQTTData goroutine. Disconnecting MQTT client.")
			client.Disconnect(250)
			log.Println("Exiting publishMQTTData goroutine.")
			return
		default:
			// 检查MQTT连接状态，如果未连接则尝试连接
			if !client.IsConnected() {
				for {
					log.Println("Attempting to connect to MQTT broker...")
					token := client.Connect()
					if token.Wait() && token.Error() == nil {
						log.Println("Connected to MQTT broker")
						break
					}
					fmt.Printf("Failed to connect to MQTT broker. Retrying... Error: %v\n", token.Error())
					time.Sleep(reconnectDelay)
					if _, ok := <-stopChan; ok {
						client.Disconnect(250)
						return
					}

				}
			}

			if data, ok := dataQueue.Dequeue(); ok {
				fmt.Printf("Publishing data to MQTT: %s\n", data)
				token := client.Publish(mqttTopic, 0, false, data)
				if token.Wait() && token.Error() != nil {
					log.Printf("Error publishing to MQTT: %v\n", token.Error())
				} else {
					fmt.Printf("Successfully published data to MQTT: %s\n", data)
				}
			}
			time.Sleep(1 * time.Second)
		}
	}
}

func main() {
	var wg sync.WaitGroup
	wg.Add(2)

	// 启动OPCDA读取线程
	go func() {
		defer wg.Done()
		readOPCDAData()
	}()

	// 启动MQTT发布线程
	go func() {
		defer wg.Done()
		publishMQTTData()
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
