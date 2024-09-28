package main

import (
	"bufio"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/oschwald/geoip2-golang"
	"github.com/spf13/viper"
)

// LogMessage represents the structure of a log message
type LogMessage struct {
	FileName  string  `json:"file_name"`
	Content   string  `json:"content"`
	Timestamp float64 `json:"timestamp"`
}

// Config holds the configuration for the application
type Config struct {
	KafkaPropertiesFile string
	GeoIPDatabase       string
	GeoIPASNDatabase    string
	Topics              []string
	StartFromBeginning  bool
	EncryptionKey       string
}

var (
	geoIP   *geoip2.Reader
	geoIPASN *geoip2.Reader
	config  Config
)

// init initializes the application configuration and GeoIP databases
// This function is automatically called before the main function
// It sets up command-line flags, parses them, and initializes global variables
func init() {
	// Set up command-line flags
	flag.StringVar(&config.KafkaPropertiesFile, "kafka-properties", "k0100-client.properties", "Path to the Kafka properties file")
	flag.StringVar(&config.GeoIPDatabase, "geoip-db", "GeoLite2-Country.mmdb", "Path to the GeoIP country database file")
	flag.StringVar(&config.GeoIPASNDatabase, "geoip-asn-db", "GeoLite2-ASN.mmdb", "Path to the GeoIP ASN database file")
	flag.StringVar(&config.EncryptionKey, "encryption-key", "", "Key used for encrypting sensitive data in the output")
	flag.BoolVar(&config.StartFromBeginning, "from-beginning", false, "If true, start reading from the beginning of the Kafka topic")

	topicsFlag := flag.String("topics", "logCentral", "Comma-separated list of Kafka topics to consume from")

	// Parse the command-line flags
	flag.Parse()

	// Split the topics string into a slice
	config.Topics = strings.Split(*topicsFlag, ",")

	// Open GeoIP databases
	var err error
	geoIP, err = geoip2.Open(config.GeoIPDatabase)
	if err != nil {
		log.Fatalf("Error opening GeoIP country database: %v", err)
	}

	geoIPASN, err = geoip2.Open(config.GeoIPASNDatabase)
	if err != nil {
		log.Fatalf("Error opening GeoIP ASN database: %v", err)
	}
}

// loadProperties loads Kafka properties from a file
// Parameters:
//   - filename: string, the path to the properties file
// Returns:
//   - *viper.Viper: a Viper configuration object containing the loaded properties
//   - error: an error if the file couldn't be read or parsed
// This function uses the Viper library to read and parse the properties file
func loadProperties(filename string) (*viper.Viper, error) {
	v := viper.New()
	v.SetConfigFile(filename)
	v.SetConfigType("properties")
	err := v.ReadInConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %v", err)
	}
	return v, nil
}

// extractIPAndDomain extracts IP and domain from a log entry
// Parameters:
//   - logEntry: string, the log entry to parse
// Returns:
//   - string: the extracted IP address, or an empty string if not found
//   - string: the extracted domain name, or an empty string if not found
// This function uses regular expressions to find IP addresses and domain names in the log entry
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

// getCountryAndASNFromIP retrieves country and ASN information for an IP address
// Parameters:
//   - ipStr: string, the IP address to look up
// Returns:
//   - string: the country name associated with the IP address, or "Unknown" if not found
//   - uint: the Autonomous System Number (ASN) associated with the IP address, or 0 if not found
// This function uses the GeoIP2 databases to look up geographical and network information
func getCountryAndASNFromIP(ipStr string) (string, uint) {
	ip := net.ParseIP(ipStr)
	if ip == nil {
		return "Unknown", 0
	}

	record, err := geoIP.Country(ip)
	if err != nil {
		log.Printf("Error looking up country for IP %s: %v", ipStr, err)
		return "Unknown", 0
	}

	asnRecord, err := geoIPASN.ASN(ip)
	if err != nil {
		log.Printf("Error looking up ASN for IP %s: %v", ipStr, err)
		return record.Country.Names["en"], 0
	}

	return record.Country.Names["en"], asnRecord.AutonomousSystemNumber
}

// createTLSConfig creates a TLS configuration for Kafka
// Parameters:
//   - props: *viper.Viper, a Viper configuration object containing Kafka properties
// Returns:
//   - *tls.Config: a TLS configuration object
//   - error: an error if the TLS configuration couldn't be created
// This function sets up TLS for secure communication with Kafka
func createTLSConfig(props *viper.Viper) (*tls.Config, error) {
	tlsConfig := &tls.Config{
		InsecureSkipVerify: true, // Note: Use caution with this setting in production
	}

	certFile := props.GetString("ssl.truststore.location")
	if certFile != "" {
		caCert, err := os.ReadFile(certFile)
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

// createKafkaConsumer creates a Kafka consumer
// Parameters:
//   - propertiesFile: string, the path to the Kafka properties file
// Returns:
//   - sarama.Consumer: a Sarama Kafka consumer object
//   - error: an error if the consumer couldn't be created
// This function sets up and configures a Kafka consumer based on the provided properties
func createKafkaConsumer(propertiesFile string) (sarama.Consumer, error) {
	props, err := loadProperties(propertiesFile)
	if err != nil {
		return nil, fmt.Errorf("failed to load properties: %v", err)
	}

	kafkaConfig := sarama.NewConfig()
	kafkaConfig.Consumer.Return.Errors = true

	if config.StartFromBeginning {
		kafkaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	} else {
		kafkaConfig.Consumer.Offsets.Initial = sarama.OffsetNewest
	}

	// Configure SASL authentication
	kafkaConfig.Net.SASL.Enable = true
	kafkaConfig.Net.SASL.Mechanism = sarama.SASLTypePlaintext
	jaasConfig := props.GetString("sasl.jaas.config")
	re := regexp.MustCompile(`username="(.+?)".*password="(.+?)"`)
	matches := re.FindStringSubmatch(jaasConfig)
	if len(matches) == 3 {
		kafkaConfig.Net.SASL.User = matches[1]
		kafkaConfig.Net.SASL.Password = matches[2]
	} else {
		return nil, fmt.Errorf("failed to extract username and password from JAAS config")
	}

	// Configure TLS if needed
	if props.GetString("security.protocol") == "SASL_SSL" {
		kafkaConfig.Net.TLS.Enable = true
		tlsConfig, err := createTLSConfig(props)
		if err != nil {
			return nil, fmt.Errorf("failed to create TLS config: %v", err)
		}
		kafkaConfig.Net.TLS.Config = tlsConfig
	}

	brokers := strings.Split(props.GetString("bootstrap.servers"), ",")
	return sarama.NewConsumer(brokers, kafkaConfig)
}

// processMessage processes a single Kafka message
// Parameters:
//   - rawMessage: []byte, the raw message data from Kafka
// Returns:
//   - *LogMessage: a pointer to a LogMessage struct containing the parsed message data
//   - error: an error if the message couldn't be processed
// This function parses the JSON data in the message and performs some basic validations
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

// timestampToDatetime converts a timestamp to a datetime
// Parameters:
//   - timestamp: float64, the timestamp to convert
// Returns:
//   - time.Time: a Time object representing the timestamp
// This function converts a Unix timestamp (with fractional seconds) to a Go time.Time object
func timestampToDatetime(timestamp float64) time.Time {
	sec, dec := math.Modf(timestamp)
	return time.Unix(int64(sec), int64(dec*(1e9))).In(time.Local)
}

// getStartDatetime prompts the user to choose a start time
// Parameters:
//   - endDatetime: time.Time, the end time of the log analysis period
// Returns:
//   - time.Time: the chosen start time
//   - bool: true if the user chose to start from the beginning of available data, false otherwise
// This function interactively prompts the user to choose a start time for log analysis
func getStartDatetime(endDatetime time.Time) (time.Time, bool) {
	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Println("\nChoose start time option:")
		fmt.Println("1. Last 1 hour")
		fmt.Println("2. Last 6 hours")
		fmt.Println("3. Last 12 hours")
		fmt.Println("4. Last 1 day")
		fmt.Println("5. Last 7 days")
		fmt.Println("6. Last 30 days")
		fmt.Println("7. All available data")
		fmt.Println("8. Specify custom date and time")
		fmt.Print("Enter your choice (1-8): ")
		
		choice, _ := reader.ReadString('\n')
		choice = strings.TrimSpace(choice)

		switch choice {
		case "1":
			return endDatetime.Add(-1 * time.Hour), false
		case "2":
			return endDatetime.Add(-6 * time.Hour), false
		case "3":
			return endDatetime.Add(-12 * time.Hour), false
		case "4":
			return endDatetime.Add(-24 * time.Hour), false
		case "5":
			return endDatetime.Add(-7 * 24 * time.Hour), false
		case "6":
			return endDatetime.Add(-30 * 24 * time.Hour), false
		case "7":
			return time.Time{}, true // Return zero time and set StartFromBeginning to true
		case "8":
			for {
				fmt.Print("Enter the start date and time (YYYY-MM-DD HH:MM:SS): ")
				dateStr, _ := reader.ReadString('\n')
				dateStr = strings.TrimSpace(dateStr)
				startDatetime, err := time.ParseInLocation("2006-01-02 15:04:05", dateStr, time.Local)
				if err == nil {
					return startDatetime, false
				}
				fmt.Println("Invalid date and time format. Please use YYYY-MM-DD HH:MM:SS.")
			}
		default:
			fmt.Println("Invalid choice. Please enter a number between 1 and 8.")
		}
	}
}

// getEndDatetime prompts the user to choose an end time
// Returns:
//   - time.Time: the chosen end time
// This function interactively prompts the user to choose an end time for log analysis
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

// processLogs processes log messages from Kafka
// Parameters:
//   - startDatetime: time.Time, the start time for log processing
//   - endDatetime: time.Time, the end time for log processing
// Returns:
//   - map[string]map[string]int: a nested map of countries and their IP counts
//   - map[string]int: a map of domain counts
//   - int: total number of processed messages
//   - int: total number of skipped messages
//   - int: total number of denied queries
//   - *LogMessage: pointer to the first processed message
//   - *LogMessage: pointer to the last processed message
//   - time.Duration: duration of message consumption
//   - time.Duration: duration of message processing
// This function is the core of the log processing logic, consuming messages from Kafka and analyzing them
func processLogs(startDatetime, endDatetime time.Time) (map[string]map[string]int, map[string]int, int, int, int, *LogMessage, *LogMessage, time.Duration, time.Duration) {
	consumeStartTime := time.Now()

	consumer, err := createKafkaConsumer(config.KafkaPropertiesFile)
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}
	defer consumer.Close()

	log.Printf("Connected to Kafka. Starting to consume messages.")
	log.Printf("Topics: %v", config.Topics)

	ipCountryCounter := make(map[string]map[string]int)
	domainCounter := make(map[string]int)
	var firstMessage, lastMessage *LogMessage
	totalDenied := 0
	processedCount := 0
	skippedCount := 0

	var wg sync.WaitGroup
	resultChan := make(chan struct {
		ipCountry map[string]map[string]int
		domain    map[string]int
		denied    int
		processed int
		skipped   int
		first     *LogMessage
		last      *LogMessage
	})

	// Process messages from each partition of each topic
	for _, topic := range config.Topics {
		partitions, err := consumer.Partitions(topic)
		if err != nil {
			log.Printf("Failed to get partitions for topic %s: %v", topic, err)
			continue
		}

		for _, partition := range partitions {
			wg.Add(1)
			go func(topic string, partition int32) {
				defer wg.Done()

				pc, err := consumer.ConsumePartition(topic, partition, sarama.OffsetOldest)
				if err != nil {
					log.Printf("Failed to start consumer for partition %d: %s", partition, err)
					return
				}
				defer pc.Close()

				localIPCountryCounter := make(map[string]map[string]int)
				localDomainCounter := make(map[string]int)
				var localFirstMessage, localLastMessage *LogMessage
				localTotalDenied := 0
				localProcessedCount := 0
				localSkippedCount := 0

				for msg := range pc.Messages() {
					logMessage, err := processMessage(msg.Value)
					if err != nil {
						localSkippedCount++
						continue
					}
					if logMessage == nil {
						localSkippedCount++
						continue
					}

					messageDateTime := timestampToDatetime(logMessage.Timestamp)
					if !startDatetime.IsZero() && messageDateTime.Before(startDatetime) {
						continue
					}
					if messageDateTime.After(endDatetime) {
						break
					}

					localProcessedCount++
					if localFirstMessage == nil {
						localFirstMessage = logMessage
					}
					localLastMessage = logMessage

					if strings.Contains(logMessage.Content, "denied") {
						localTotalDenied++
						ip, domain := extractIPAndDomain(logMessage.Content)
						if ip != "" {
							country, asn := getCountryAndASNFromIP(ip)
							if localIPCountryCounter[country] == nil {
								localIPCountryCounter[country] = make(map[string]int)
							}
							localIPCountryCounter[country][fmt.Sprintf("%s (ASN: %d)", ip, asn)]++
						}
						if domain != "" {
							localDomainCounter[domain]++
						}
					}
				}

				resultChan <- struct {
					ipCountry map[string]map[string]int
					domain    map[string]int
					denied    int
					processed int
					skipped   int
					first     *LogMessage
					last      *LogMessage
				}{
					ipCountry: localIPCountryCounter,
					domain:    localDomainCounter,
					denied:    localTotalDenied,
					processed: localProcessedCount,
					skipped:   localSkippedCount,
					first:     localFirstMessage,
					last:      localLastMessage,
				}
			}(topic, partition)
		}
	}

	go func() {
		wg.Wait()
		close(resultChan)
	}()

	consumeDuration := time.Since(consumeStartTime)
	processStartTime := time.Now()

	// Aggregate results from all goroutines
	for result := range resultChan {
		for country, ips := range result.ipCountry {
			if ipCountryCounter[country] == nil {
				ipCountryCounter[country] = make(map[string]int)
			}
			for ip, count := range ips {
				ipCountryCounter[country][ip] += count
			}
		}
		for domain, count := range result.domain {
			domainCounter[domain] += count
		}
		totalDenied += result.denied
		processedCount += result.processed
		skippedCount += result.skipped
		if firstMessage == nil || (result.first != nil && timestampToDatetime(result.first.Timestamp).Before(timestampToDatetime(firstMessage.Timestamp))) {
			firstMessage = result.first
		}
		if lastMessage == nil || (result.last != nil && timestampToDatetime(result.last.Timestamp).After(timestampToDatetime(lastMessage.Timestamp))) {
			lastMessage = result.last
		}
	}

	processDuration := time.Since(processStartTime)

	return ipCountryCounter, domainCounter, processedCount, skippedCount, totalDenied, firstMessage, lastMessage, consumeDuration, processDuration
}

// generateSummary generates a summary of the processed logs
// Parameters:
//   - ipCountryCounter: map[string]map[string]int, a nested map of countries and their IP counts
//   - domainCounter: map[string]int, a map of domain counts
//   - processedCount: int, total number of processed messages
//   - skippedCount: int, total number of skipped messages
//   - totalDenied: int, total number of denied queries
//   - firstMessage: *LogMessage, pointer to the first processed message
//   - lastMessage: *LogMessage, pointer to the last processed message
//   - consumeDuration: time.Duration, duration of message consumption
//   - processDuration: time.Duration, duration of message processing
// Returns:
//   - string: a formatted summary of the log analysis
// This function generates a human-readable summary of the log analysis results
func generateSummary(ipCountryCounter map[string]map[string]int, domainCounter map[string]int, processedCount, skippedCount, totalDenied int, firstMessage, lastMessage *LogMessage, consumeDuration, processDuration time.Duration) string {
	var output strings.Builder

	fmt.Fprintf(&output, "\nProcessing Summary:\n")
	fmt.Fprintf(&output, "Total messages processed: %d\n", processedCount)
	fmt.Fprintf(&output, "Total messages skipped: %d\n", skippedCount)
	fmt.Fprintf(&output, "Total denied queries: %d\n", totalDenied)
	fmt.Fprintf(&output, "Time taken to consume messages: %v\n", consumeDuration)
	fmt.Fprintf(&output, "Time taken to process messages: %v\n", processDuration)

	fmt.Fprintf(&output, "\nTop 10 Countries with Denied IPs:\n")
	output.WriteString(getTopCountries(ipCountryCounter, 10))

	fmt.Fprintf(&output, "\nTop 10 Denied IPs per Country:\n")
	for country, ips := range ipCountryCounter {
		fmt.Fprintf(&output, "\n%s:\n", country)
		output.WriteString(getTopN(ips, 10))
	}

	fmt.Fprintf(&output, "\nTop 10 Domains Denied:\n")
	output.WriteString(getTopN(domainCounter, 10))

	if lastMessage != nil {
		fmt.Fprintf(&output, "\nLast processed message:\n")
		fmt.Fprintf(&output, "Time: %v\n", timestampToDatetime(lastMessage.Timestamp))
		fmt.Fprintf(&output, "Content: %s\n", lastMessage.Content)
	}

	return output.String()
}

// getTopCountries returns the top N countries with the most denied IPs
// Parameters:
//   - ipCountryCounter: map[string]map[string]int, a nested map of countries and their IP counts
//   - n: int, the number of top countries to return
// Returns:
//   - string: a formatted string containing the top N countries and their denied IP counts
// This function sorts countries by their total denied IP count and returns the top N
func getTopCountries(ipCountryCounter map[string]map[string]int, n int) string {
	var output strings.Builder
	type kv struct {
		Key   string
		Value int
	}

	var ss []kv
	for country, ips := range ipCountryCounter {
		total := 0
		for _, count := range ips {
			total += count
		}
		ss = append(ss, kv{country, total})
	}

	sort.Slice(ss, func(i, j int) bool {
		return ss[i].Value > ss[j].Value
	})

	for i := 0; i < n && i < len(ss); i++ {
		fmt.Fprintf(&output, "%s: %d\n", ss[i].Key, ss[i].Value)
	}
	return output.String()
}

// getTopN returns the top N items from a map
// Parameters:
//   - counter: map[string]int, a map of items and their counts
//   - n: int, the number of top items to return
// Returns:
//   - string: a formatted string containing the top N items and their counts
// This function sorts items by their count and returns the top N
func getTopN(counter map[string]int, n int) string {
	var output strings.Builder
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
		fmt.Fprintf(&output, "%s: %d\n", ss[i].Key, ss[i].Value)
	}
	return output.String()
}

// ensureResultDirectory ensures that the result directory exists
// Returns:
//   - error: an error if the directory couldn't be created, nil otherwise
// This function creates the 'result' directory if it doesn't exist
func ensureResultDirectory() error {
	if _, err := os.Stat("result"); os.IsNotExist(err) {
		return os.Mkdir("result", 0755)
	}
	return nil
}

// saveOutputToFile saves the output to a file
// Parameters:
//   - filename: string, the name of the file to save the output to
//   - content: string, the content to be saved
// Returns:
//   - error: an error if the file couldn't be created or written to, nil otherwise
// This function saves the given content to a file in the 'result' directory
func saveOutputToFile(filename string, content string) error {
	if err := ensureResultDirectory(); err != nil {
		return fmt.Errorf("failed to create result directory: %v", err)
	}

	fullPath := filepath.Join("result", filename)
	file, err := os.Create(fullPath)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.WriteString(content)
	return err
}

// exportToCSV exports the results to a CSV file
// Parameters:
//   - filename: string, the name of the CSV file to create
//   - ipCountryCounter: map[string]map[string]int, a nested map of countries and their IP counts
//   - domainCounter: map[string]int, a map of domain counts
// Returns:
//   - error: an error if the CSV file couldn't be created or written to, nil otherwise
// This function exports the analysis results to a CSV file in the 'result' directory
func exportToCSV(filename string, ipCountryCounter map[string]map[string]int, domainCounter map[string]int) error {
	if err := ensureResultDirectory(); err != nil {
		return fmt.Errorf("failed to create result directory: %v", err)
	}

	fullPath := filepath.Join("result", filename)
	file, err := os.Create(fullPath)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write headers
	writer.Write([]string{"Type", "Country", "IP/Domain", "Count"})

	// Write IP data
	for country, ips := range ipCountryCounter {
		for ip, count := range ips {
			writer.Write([]string{"IP", country, ip, strconv.Itoa(count)})
		}
	}

	// Write Domain data
	for domain, count := range domainCounter {
		writer.Write([]string{"Domain", "", domain, strconv.Itoa(count)})
	}

	return nil
}

// encryptSensitiveData encrypts sensitive data using AES encryption
// Parameters:
//   - data: string, the data to be encrypted
// Returns:
//   - string: the encrypted data as a base64-encoded string
//   - error: an error if encryption failed, nil otherwise
// This function encrypts the given data using AES encryption with the provided encryption key
func encryptSensitiveData(data string) (string, error) {
    if config.EncryptionKey == "" {
        return data, nil
    }

    block, err := aes.NewCipher([]byte(config.EncryptionKey))
    if err != nil {
        return "", fmt.Errorf("failed to create AES cipher: %v", err)
    }

    gcm, err := cipher.NewGCM(block)
    if err != nil {
        return "", fmt.Errorf("failed to create GCM: %v", err)
    }

    nonce := make([]byte, gcm.NonceSize())
    if _, err = io.ReadFull(rand.Reader, nonce); err != nil {
        return "", fmt.Errorf("failed to generate nonce: %v", err)
    }

    ciphertext := gcm.Seal(nonce, nonce, []byte(data), nil)
    return base64.StdEncoding.EncodeToString(ciphertext), nil
}

// main is the entry point of the application
// This function orchestrates the entire log analysis process
func main() {
	// Get end datetime from user input
	endDatetime := getEndDatetime()
	// Get start datetime and whether to start from beginning
	startDatetime, startFromBeginning := getStartDatetime(endDatetime)

	config.StartFromBeginning = startFromBeginning

	log.Printf("Script will process messages from %v to %v", startDatetime, endDatetime)
	log.Printf("Starting from the beginning: %v", config.StartFromBeginning)

	defer geoIP.Close()
	defer geoIPASN.Close()

	// Process logs
	ipCountryCounter, domainCounter, processedCount, skippedCount, totalDenied, firstMessage, lastMessage, consumeDuration, processDuration := processLogs(startDatetime, endDatetime)

	// Generate summary
	summary := generateSummary(ipCountryCounter, domainCounter, processedCount, skippedCount, totalDenied, firstMessage, lastMessage, consumeDuration, processDuration)

	// Print summary to console
	fmt.Print(summary)

	// Save summary to file
	outputFilename := fmt.Sprintf("log_analysis_summary_%s.txt", time.Now().Format("20060102_150405"))
	err := saveOutputToFile(outputFilename, summary)
	if err != nil {
		log.Printf("Failed to save summary to file: %v", err)
	} else {
		log.Printf("Summary saved to result/%s", outputFilename)
	}

	// Ask if user wants to export to CSV
	fmt.Print("Export results to CSV? (y/n): ")
	var exportCSV string
	fmt.Scanln(&exportCSV)
	if strings.ToLower(exportCSV) == "y" {
		csvFilename := fmt.Sprintf("log_analysis_%s.csv", time.Now().Format("20060102_150405"))

		// Encrypt sensitive data before exporting
		encryptedIPCountryCounter := make(map[string]map[string]int)
		for country, ips := range ipCountryCounter {
			encryptedIPCountryCounter[country] = make(map[string]int)
			for ip, count := range ips {
				encryptedIP, err := encryptSensitiveData(ip)
				if err != nil {
					log.Printf("Failed to encrypt IP: %v", err)
					encryptedIP = ip
				}
				encryptedIPCountryCounter[country][encryptedIP] = count
			}
		}

		encryptedDomainCounter := make(map[string]int)
		for domain, count := range domainCounter {
			encryptedDomain, err := encryptSensitiveData(domain)
			if err != nil {
				log.Printf("Failed to encrypt domain: %v", err)
				encryptedDomain = domain
			}
			encryptedDomainCounter[encryptedDomain] = count
		}

		err := exportToCSV(csvFilename, encryptedIPCountryCounter, encryptedDomainCounter)
		if err != nil {
			log.Printf("Failed to export to CSV: %v", err)
		} else {
			log.Printf("Results exported to result/%s", csvFilename)
		}
	}
}