package main

import (
	"bufio"
	"crypto/tls"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"math"

	"github.com/IBM/sarama"
)

var (
	kafkaPropertiesFile = "client.properties"
	kafkaTopic          = "eduroam-log"
	logFile             = "/eduroam/log/eduroam-nro.log"
	batchSize           = 1000
)

// kafkaConfig เก็บการตั้งค่าการเชื่อมต่อ Kafka
type kafkaConfig struct {
	BootstrapServers string
	SecurityProtocol string
	SaslMechanism    string
	SaslUsername     string
	SaslPassword     string
}

// readProperties อ่านและแยกวิเคราะห์ไฟล์คุณสมบัติ Kafka
// รับพารามิเตอร์:
//   - filePath: พาธของไฟล์คุณสมบัติ
// คืนค่า:
//   - kafkaConfig: โครงสร้างที่มีการตั้งค่า Kafka
//   - error: ข้อผิดพลาดที่เกิดขึ้นระหว่างการอ่านไฟล์
func readProperties(filePath string) (kafkaConfig, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return kafkaConfig{}, err
	}
	defer file.Close()

	config := kafkaConfig{}
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "#") || line == "" {
			continue
		}
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}
		key, value := strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])
		switch key {
		case "bootstrap.servers":
			config.BootstrapServers = value
		case "security.protocol":
			config.SecurityProtocol = value
		case "sasl.mechanism":
			config.SaslMechanism = value
		case "sasl.jaas.config":
			if strings.Contains(value, "username=") && strings.Contains(value, "password=") {
				config.SaslUsername = strings.Split(strings.Split(value, "username=\"")[1], "\"")[0]
				config.SaslPassword = strings.Split(strings.Split(value, "password=\"")[1], "\"")[0]
			}
		}
	}
	return config, scanner.Err()
}

// createKafkaProducer สร้าง Kafka AsyncProducer จากการตั้งค่าที่กำหนด
// รับพารามิเตอร์:
//   - config: โครงสร้าง kafkaConfig ที่มีการตั้งค่า Kafka
// คืนค่า:
//   - sarama.AsyncProducer: อินสแตนซ์ของ AsyncProducer
//   - error: ข้อผิดพลาดที่เกิดขึ้นระหว่างการสร้าง producer
func createKafkaProducer(config kafkaConfig) (sarama.AsyncProducer, error) {
	kafkaConfig := sarama.NewConfig()
	kafkaConfig.Producer.Return.Successes = false
	kafkaConfig.Producer.Return.Errors = true
	kafkaConfig.Producer.Flush.Frequency = 500 * time.Millisecond
	kafkaConfig.Producer.Flush.Messages = batchSize

	if config.SecurityProtocol == "SASL_PLAINTEXT" || config.SecurityProtocol == "SASL_SSL" {
		kafkaConfig.Net.SASL.Enable = true
		kafkaConfig.Net.SASL.Mechanism = sarama.SASLMechanism(config.SaslMechanism)
		kafkaConfig.Net.SASL.User = config.SaslUsername
		kafkaConfig.Net.SASL.Password = config.SaslPassword
	}

	if config.SecurityProtocol == "SASL_SSL" {
		kafkaConfig.Net.TLS.Enable = true
		kafkaConfig.Net.TLS.Config = &tls.Config{InsecureSkipVerify: true}
	}

	producer, err := sarama.NewAsyncProducer(strings.Split(config.BootstrapServers, ","), kafkaConfig)
	if err != nil {
		return nil, err
	}
	return producer, nil
}

// processLines ประมวลผลบรรทัดจากช่องและส่งไปยัง Kafka
// รับพารามิเตอร์:
//   - lines: ช่องสำหรับรับบรรทัดที่จะประมวลผล
//   - producer: Kafka AsyncProducer สำหรับส่งข้อความ
//   - wg: WaitGroup สำหรับการซิงโครไนซ์ goroutine
//   - processedLines: ตัวนับบรรทัดที่ประมวลผลแล้ว
func processLines(lines <-chan string, producer sarama.AsyncProducer, wg *sync.WaitGroup, processedLines *int64) {
	defer wg.Done()
	for line := range lines {
		msg := &sarama.ProducerMessage{
			Topic: kafkaTopic,
			Value: sarama.StringEncoder(line),
		}
		producer.Input() <- msg
		atomic.AddInt64(processedLines, 1)
	}
}

// updateProgress แสดงและอัปเดตความคืบหน้าของการประมวลผล
// รับพารามิเตอร์:
//   - totalLines: จำนวนบรรทัดทั้งหมดที่ต้องประมวลผล
//   - processedLines: จำนวนบรรทัดที่ประมวลผลแล้ว
//   - doneChan: ช่องที่ส่งสัญญาณเมื่อการประมวลผลเสร็จสิ้น
func updateProgress(totalLines *int64, processedLines *int64, doneChan <-chan bool) {
    var lastProcessed int64
    startTime := time.Now()
    var lastProgressPercent float64 = -1  // เริ่มต้นที่ -1 เพื่อให้แน่ใจว่าจะแสดง 0%
    progressInterval := 10.0
    for {
        select {
        case <-time.After(100 * time.Millisecond):  // ตรวจสอบบ่อยขึ้นเพื่อให้เห็น 0%
            total := atomic.LoadInt64(totalLines)
            processed := atomic.LoadInt64(processedLines)
            
            if total > 0 {
                progress := float64(processed) / float64(total) * 100
                currentProgressPercent := math.Floor(progress/progressInterval) * progressInterval

                if currentProgressPercent > lastProgressPercent || (processed == total && lastProgressPercent < 100) {
                    speed := float64(processed-lastProcessed) / time.Since(startTime).Seconds()
                    fmt.Printf("\nProgress: %.0f%% (%d/%d) - %.2f lines/sec", 
                        currentProgressPercent, processed, total, speed)
                    lastProgressPercent = currentProgressPercent
                }
            }
            
            lastProcessed = processed
            startTime = time.Now()

        case <-doneChan:
            total := atomic.LoadInt64(totalLines)
            processed := atomic.LoadInt64(processedLines)
            
            if total > 0 {
                speed := float64(processed-lastProcessed) / time.Since(startTime).Seconds()
                fmt.Printf("\nProgress: 100%% (%d/%d) - %.2f lines/sec\n", processed, total, speed)
            }
            
            fmt.Println("Processing complete")
            return
        }
    }
}

// getUserInput ขอข้อมูลนำเข้าจากผู้ใช้พร้อมค่าเริ่มต้น
// รับพารามิเตอร์:
//   - prompt: ข้อความที่แสดงให้ผู้ใช้
//   - defaultValue: ค่าเริ่มต้นที่จะใช้ถ้าผู้ใช้ไม่ป้อนค่า
// คืนค่า:
//   - string: ค่าที่ผู้ใช้ป้อนหรือค่าเริ่มต้น
func getUserInput(prompt, defaultValue string) string {
	fmt.Printf("%s (default: %s): ", prompt, defaultValue)
	reader := bufio.NewReader(os.Stdin)
	input, _ := reader.ReadString('\n')
	input = strings.TrimSpace(input)
	if input == "" {
		return defaultValue
	}
	return input
}

// checkFile ตรวจสอบว่าไฟล์ที่ระบุมีอยู่จริง
// รับพารามิเตอร์:
//   - filePath: พาธของไฟล์ที่จะตรวจสอบ
// คืนค่า:
//   - error: nil ถ้าไฟล์มีอยู่, หรือข้อผิดพลาดถ้าไม่พบไฟล์
func checkFile(filePath string) error {
	_, err := os.Stat(filePath)
	return err
}

// getLogFiles รับรายการไฟล์บันทึกตามพาธที่ระบุ รองรับ wildcard
// รับพารามิเตอร์:
//   - logFilePath: พาธของไฟล์บันทึกหรือรูปแบบ wildcard
// คืนค่า:
//   - []string: สไลซ์ของพาธไฟล์ที่ตรงกับรูปแบบ
//   - error: ข้อผิดพลาดที่เกิดขึ้นระหว่างการค้นหาไฟล์
func getLogFiles(logFilePath string) ([]string, error) {
	if strings.Contains(logFilePath, "*") {
		return filepath.Glob(logFilePath)
	}
	if err := checkFile(logFilePath); err != nil {
		return nil, err
	}
	return []string{logFilePath}, nil
}

// processFile อ่านและประมวลผลไฟล์บันทึกแต่ละไฟล์
// รับพารามิเตอร์:
//   - filePath: พาธของไฟล์ที่จะประมวลผล
//   - producer: Kafka AsyncProducer สำหรับส่งข้อความ
//   - totalLines: ตัวนับจำนวนบรรทัดทั้งหมด
//   - processedLines: ตัวนับจำนวนบรรทัดที่ประมวลผลแล้ว
//   - linesChan: ช่องสำหรับส่งบรรทัดที่อ่านได้
// คืนค่า:
//   - error: ข้อผิดพลาดที่เกิดขึ้นระหว่างการประมวลผลไฟล์
func processFile(filePath string, producer sarama.AsyncProducer, totalLines, processedLines *int64, linesChan chan<- string) error {
    file, err := os.Open(filePath)
    if err != nil {
        return err
    }
    defer file.Close()

    scanner := bufio.NewScanner(file)
    var localTotal int64
    for scanner.Scan() {
        linesChan <- scanner.Text()
        localTotal++
    }
    atomic.AddInt64(totalLines, localTotal)

    return scanner.Err()
}

// main เป็นจุดเริ่มต้นของโปรแกรม
// ฟังก์ชันนี้จัดการการทำงานหลักของโปรแกรม รวมถึง:
// - การรับข้อมูลนำเข้าจากผู้ใช้
// - การตั้งค่าและสร้าง Kafka producer
// - การประมวลผลไฟล์บันทึก
// - การจัดการ goroutines สำหรับการประมวลผลแบบพร้อมกัน
// - การแสดงความคืบหน้าและการจัดการข้อผิดพลาด
func main() {
	kafkaPropertiesFile = getUserInput("Enter Kafka properties file", kafkaPropertiesFile)
	kafkaTopic = getUserInput("Enter Kafka topic", kafkaTopic)
	logFile = getUserInput("Enter log file or pattern", logFile)
	batchSizeStr := getUserInput("Enter batch size", strconv.Itoa(batchSize))
	batchSize, _ = strconv.Atoi(batchSizeStr)

	if err := checkFile(kafkaPropertiesFile); err != nil {
		log.Fatalf("Invalid Kafka properties file: %v", err)
	}

	config, err := readProperties(kafkaPropertiesFile)
	if err != nil {
		log.Fatalf("Failed to read properties: %v", err)
	}

	producer, err := createKafkaProducer(config)
	if err != nil {
		log.Fatalf("Failed to create Kafka producer: %v", err)
	}
	defer producer.Close()

	logFiles, err := getLogFiles(logFile)
	if err != nil {
		log.Fatalf("Failed to get log files: %v", err)
	}

	var totalLines int64
	var processedLines int64

	linesChan := make(chan string, 10000)
	var wg sync.WaitGroup
	doneChan := make(chan bool)

	numWorkers := runtime.NumCPU() * 2
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go processLines(linesChan, producer, &wg, &processedLines)
	}

	go updateProgress(&totalLines, &processedLines, doneChan)

    var fileWg sync.WaitGroup
    for _, file := range logFiles {
        fileWg.Add(1)
        go func(f string) {
            defer fileWg.Done()
            if err := processFile(f, producer, &totalLines, &processedLines, linesChan); err != nil {
                log.Printf("Error processing file %s: %v", f, err)
            }
        }(file)
    }
    fileWg.Wait()
    close(linesChan)

	wg.Wait()
	doneChan <- true

	fmt.Printf("Finished processing files\n")

	producer.AsyncClose()

	for err := range producer.Errors() {
		log.Printf("Failed to send message: %v\n", err)
	}
}