package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/eclipse/paho.mqtt.golang"
	"github.com/simonvetter/modbus"
)

const (
	modbusHost     = "192.168.55.162"
	modbusPort     = 8899
	mqttBroker     = "mqbroker.metme.top"
	mqttPort       = 1883
	mqttID         = "mb2mqttgo_client_id"
	mqttUsername   = "mqdevice"
	mqttPassword   = "device@metme"
	mqttTopic      = "402A8F29260B/data"
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
	DeviceUnitID    uint8  `json:"device_unitid"`
	TagID           string `json:"tag_id"`
	RegisterAddress uint16 `json:"register_address"`
	Value           uint16 `json:"value"`
}

// 读取Modbus数据的函数
func readModbusData() {
	var client *modbus.ModbusClient
	var err error
	var mbConnected = false
	client, err = modbus.NewClient(&modbus.ClientConfiguration{
		URL:     "rtuovertcp://" + modbusHost + ":" + strconv.Itoa(modbusPort),
		Speed:   9600,
		Timeout: 1 * time.Second,
	})
	if err != nil {
		defer client.Close()
	}

	deviceUnitId := uint8(1)
	tagID := "i_Temperature\n"
	registerAddress := uint16(0)

	// Switch to unit ID
	err = client.SetUnitId(deviceUnitId)
	if err != nil {
		return
	}

	for {
		select {
		case <-stopChan:
			log.Println("Received stop signal. Exiting readModbusData function.")
			return
		default:
			if !mbConnected {
				for {
					err = client.Open()
					if err != nil {
						log.Printf("Failed to connect to Modbus TCP server at %s:%d. Retrying...\n", modbusHost, modbusPort)
						time.Sleep(reconnectDelay)
						continue
					}
					mbConnected = true
					break
				}
			}
			var reg16s uint16
			reg16s, err = client.ReadRegister(registerAddress, modbus.HOLDING_REGISTER)
			if err != nil {
				log.Printf("Error reading Modbus data: %v\n", err)
			} else {
				modbusData := PUBData{
					DeviceUnitID:    deviceUnitId,
					TagID:           tagID,
					RegisterAddress: registerAddress,
					Value:           reg16s,
				}
				jsonData, err := json.Marshal(modbusData)
				if err != nil {
					fmt.Printf("Error marshaling Modbus data to JSON: %v\n", err)
					continue
				}
				dataQueue.Enqueue(string(jsonData))
				fmt.Printf("Read data from Modbus: %s\n", jsonData)
			}
			time.Sleep(1 * time.Second)
		}
	}
}

// 发布MQTT数据的函数
var f mqtt.MessageHandler = func(client mqtt.Client, msg mqtt.Message) {
	fmt.Printf("TOPIC: %s\n", msg.Topic())
	fmt.Printf("MSG: %s\n", msg.Payload())
}

func publishMQTTData() {
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
			client.Disconnect(250)
			log.Println("Received stop signal. Exiting publishMQTTData function.")
			return
		default:
			// 检查MQTT连接状态，如果未连接则尝试连接
			if !client.IsConnected() {
				for {
					token := client.Connect()
					if token.Wait() && token.Error() == nil {
						break
					}
					log.Printf("Failed to connect to MQTT broker. Retrying... Error: %v\n", token.Error())
					time.Sleep(reconnectDelay)
					if _, ok := <-stopChan; ok {
						client.Disconnect(250)
						return
					}
				}
			}

			if data, ok := dataQueue.Dequeue(); ok {
				token := client.Publish(mqttTopic, 0, false, data)
				if token.Wait() && token.Error() != nil {
					log.Printf("Error publishing to MQTT: %v\n", token.Error())
				} else {
					fmt.Printf("Published data to MQTT: %s\n", data)
				}
			}
			time.Sleep(1 * time.Second)
		}
	}
}

func main() {
	var wg sync.WaitGroup
	wg.Add(2)

	// 启动Modbus读取线程
	go func() {
		defer wg.Done()
		readModbusData()
	}()

	// 启动MQTT发布线程
	go func() {
		defer wg.Done()
		publishMQTTData()
	}()

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
		time.Sleep(2 * time.Second)
		os.Exit(0)
	}()

	// 防止主线程退出
	select {}
}
