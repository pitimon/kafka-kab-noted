package main

import (
        "bufio"
        "crypto/tls"
        "crypto/x509"
        "encoding/json"
        "fmt"
        "io/ioutil"
        "log"
        "math"
        "os"
        "regexp"
        "sort"
        "strings"
        "time"

        "github.com/IBM/sarama"
        "github.com/spf13/viper"
)

type LogMessage struct {
        FileName  string  `json:"file_name"`
        Content   string  `json:"content"`
        Timestamp float64 `json:"timestamp"`
}

func loadProperties(filename string) (*viper.Viper, error) {
        v := viper.New()
        v.SetConfigFile(filename)
        v.SetConfigType("properties")
        err := v.ReadInConfig()
        return v, err
}

func extractIPAndDomain(logEntry string) (string, string) {
    ipPattern := `\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}`
    domainPattern := `\(([^)]+)\)`

    ipRegex := regexp.MustCompile(ipPattern)
    domainRegex := regexp.MustCompile(domainPattern)

    ipMatch := ipRegex.FindString(logEntry)
    domainMatches := domainRegex.FindStringSubmatch(logEntry)

    domain := ""
    if len(domainMatches) > 1 {
        domain = domainMatches[1]
    }

    return ipMatch, domain
}

func createTLSConfig(props *viper.Viper) (*tls.Config, error) {
        tlsConfig := &tls.Config{
                InsecureSkipVerify: true, // ใช้ความระมัดระวังเมื่อตั้งค่านี้เป็น true ในสภาพแวดล้อมการผลิต
        }

        certFile := props.GetString("ssl.truststore.location")
        if certFile != "" {
                caCert, err := ioutil.ReadFile(certFile)
                if err != nil {
                        return nil, fmt.Errorf("error reading SSL cert file: %v", err)
                }
                caCertPool := x509.NewCertPool()
                caCertPool.AppendCertsFromPEM(caCert)
                tlsConfig.RootCAs = caCertPool
        }

        tlsConfig.MinVersion = tls.VersionTLS12
        tlsConfig.MaxVersion = tls.VersionTLS13

        return tlsConfig, nil
}

func createKafkaConsumer(propertiesFile string, startFromBeginning bool) (sarama.Consumer, error) {
        props, err := loadProperties(propertiesFile)
        if err != nil {
                return nil, fmt.Errorf("failed to load properties: %v", err)
        }

        config := sarama.NewConfig()
        config.Consumer.Return.Errors = true

        if startFromBeginning {
                config.Consumer.Offsets.Initial = sarama.OffsetOldest
        } else {
                config.Consumer.Offsets.Initial = sarama.OffsetNewest
        }

        config.Net.SASL.Enable = true
        config.Net.SASL.Mechanism = sarama.SASLTypePlaintext
        jaasConfig := props.GetString("sasl.jaas.config")
        re := regexp.MustCompile(`username="(.+?)".*password="(.+?)"`)
        matches := re.FindStringSubmatch(jaasConfig)
        if len(matches) == 3 {
                config.Net.SASL.User = matches[1]
                config.Net.SASL.Password = matches[2]
        } else {
                return nil, fmt.Errorf("failed to extract username and password from JAAS config")
        }

        if props.GetString("security.protocol") == "SASL_SSL" {
                config.Net.TLS.Enable = true
                tlsConfig, err := createTLSConfig(props)
                if err != nil {
                        return nil, fmt.Errorf("failed to create TLS config: %v", err)
                }
                config.Net.TLS.Config = tlsConfig
        }

        brokers := strings.Split(props.GetString("bootstrap.servers"), ",")
        return sarama.NewConsumer(brokers, config)
}

func processMessage(rawMessage []byte) (*LogMessage, error) {
    var logMessage LogMessage
    err := json.Unmarshal(rawMessage, &logMessage)
    if err != nil {
        return nil, fmt.Errorf("failed to parse JSON: %v", err)
    }

    if logMessage.FileName != "security.log" {
        return nil, fmt.Errorf("unexpected file_name: %s", logMessage.FileName)
    }

    if logMessage.Timestamp == 0 {
        return nil, fmt.Errorf("invalid timestamp: %f", logMessage.Timestamp)
    }

    return &logMessage, nil
}

func min(a, b int) int {
        if a < b {
                return a
        }
        return b
}

func timestampToDatetime(timestamp float64) time.Time {
        sec, dec := math.Modf(timestamp)
        return time.Unix(int64(sec), int64(dec*(1e9))).In(time.Local)
}

func getEndDatetime() time.Time {
        reader := bufio.NewReader(os.Stdin)
        for {
                fmt.Println("Choose end time option:")
                fmt.Println("1. Current date and time")
                fmt.Println("2. Specify date and time")
                fmt.Print("Enter your choice (1 or 2): ")
                choice, _ := reader.ReadString('\n')
                choice = strings.TrimSpace(choice)

                if choice == "1" {
                        return time.Now()
                } else if choice == "2" {
                        for {
                                fmt.Print("Enter the end date and time (YYYY-MM-DD HH:MM:SS): ")
                                dateStr, _ := reader.ReadString('\n')
                                dateStr = strings.TrimSpace(dateStr)
                                endDatetime, err := time.ParseInLocation("2006-01-02 15:04:05", dateStr, time.Local)
                                if err == nil {
                                        return endDatetime
                                }
                                fmt.Println("Invalid date and time format. Please use YYYY-MM-DD HH:MM:SS.")
                        }
                }
                fmt.Println("Invalid choice. Please enter 1 or 2.")
        }
}

func processLogs(endDatetime time.Time, startFromBeginning bool) {
    consumer, err := createKafkaConsumer("k0100-client.properties", startFromBeginning)
    if err != nil {
        log.Fatalf("Failed to create consumer: %v", err)
    }
    defer consumer.Close()

    log.Printf("Connected to Kafka. Starting to consume messages.")
    log.Printf("Topic: %s", "logCentral")

    partitions, err := consumer.Partitions("logCentral")
    if err != nil {
        log.Fatalf("Failed to get partitions: %v", err)
    }

    var firstMessage, lastMessage *LogMessage
    ipCounter := make(map[string]int)
    domainCounter := make(map[string]int)
    totalDenied := 0
    processedCount := 0
    skippedCount := 0

    ticker := time.NewTicker(5 * time.Second)
    defer ticker.Stop()

    done := make(chan bool)

    go func() {
        for {
            select {
            case <-done:
                return
            case <-ticker.C:
                log.Printf("Processed messages: %d, Skipped: %d, Denied: %d", processedCount, skippedCount, totalDenied)
            }
        }
    }()

    for _, partition := range partitions {
        pc, err := consumer.ConsumePartition("logCentral", partition, sarama.OffsetOldest)
        if err != nil {
            log.Printf("Failed to start consumer for partition %d: %s", partition, err)
            continue
        }
        defer pc.Close()

        for msg := range pc.Messages() {
            logMessage, err := processMessage(msg.Value)
            if err != nil {
                skippedCount++
                continue
            }
            if logMessage == nil {
                skippedCount++
                continue
            }

            messageDateTime := timestampToDatetime(logMessage.Timestamp)
            if messageDateTime.After(endDatetime) {
                log.Printf("Reached message with datetime greater than %v. Stopping.", endDatetime)
                break
            }

            processedCount++
            if firstMessage == nil {
                firstMessage = logMessage
            }
            lastMessage = logMessage

            if strings.Contains(logMessage.Content, "denied") {
                totalDenied++
                ip, domain := extractIPAndDomain(logMessage.Content)
                if ip != "" {
                    ipCounter[ip]++
                }
                if domain != "" {
                    domainCounter[domain]++
                }
            }
        }
    }

    done <- true

    printSummary(ipCounter, domainCounter, processedCount, skippedCount, totalDenied, firstMessage, lastMessage)
}

func printSummary(ipCounter, domainCounter map[string]int, processedCount, skippedCount, totalDenied int, firstMessage, lastMessage *LogMessage) {
    fmt.Printf("\nProcessing Summary:\n")
    fmt.Printf("Total messages processed: %d\n", processedCount)
    fmt.Printf("Total messages skipped: %d\n", skippedCount)
    fmt.Printf("Total denied queries: %d\n", totalDenied)

    fmt.Printf("\nTop 10 IP Addresses Denied:\n")
    printTopN(ipCounter, 10)

    fmt.Printf("\nTop 10 Domains Denied:\n")
    printTopN(domainCounter, 10)

    if firstMessage != nil {
        fmt.Printf("\nFirst processed message:\n")
        fmt.Printf("Time: %v\n", timestampToDatetime(firstMessage.Timestamp))
        fmt.Printf("Content: %s\n", firstMessage.Content)
    }

    if lastMessage != nil {
        fmt.Printf("\nLast processed message:\n")
        fmt.Printf("Time: %v\n", timestampToDatetime(lastMessage.Timestamp))
        fmt.Printf("Content: %s\n", lastMessage.Content)
    }
}

func printTopN(counter map[string]int, n int) {
    type kv struct {
        Key   string
        Value int
    }

    var ss []kv
    for k, v := range counter {
        ss = append(ss, kv{k, v})
    }

    sort.Slice(ss, func(i, j int) bool {
        return ss[i].Value > ss[j].Value
    })

    for i := 0; i < n && i < len(ss); i++ {
        fmt.Printf("%s: %d\n", ss[i].Key, ss[i].Value)
    }
}


func main() {
        endDatetime := getEndDatetime()
        fmt.Print("Start from the beginning of the topic? (y/n): ")
        var startFromBeginning string
        fmt.Scanln(&startFromBeginning)

        log.Printf("Script will run until message datetime exceeds: %v", endDatetime)
        log.Printf("Starting from the beginning: %v", strings.ToLower(startFromBeginning) == "y")

        processLogs(endDatetime, strings.ToLower(startFromBeginning) == "y")