# Kafka Ep1: Producer data to kafka broker (Go Language)
- [Loom clip](https://www.loom.com/share/c5e16d1102734e8c81048e418f9bf0a7?sid=afcb5064-80f3-4036-b78e-c6f2ac412fcd)
---
## Kafka Producer Program:

1. User Input Handling:
   - Added getUserInput function to get user input with default values for:
     - Kafka properties file
     - Kafka topic
     - Log file or pattern
     - Batch size

2. File Handling:
   - Added checkFile function to verify the existence of input files
   - Implemented getLogFiles function to support wildcard patterns for log files

3. Kafka Configuration:
   - Improved readProperties function to parse Kafka configuration from the properties file
   - Enhanced createKafkaProducer function to support SASL_PLAINTEXT and SASL_SSL security protocols

4. Performance Optimizations:
   - Using AsyncProducer instead of SyncProducer for better throughput
   - Implemented concurrent processing using goroutines and channels
   - Added batch processing with configurable batch size

5. Progress Tracking:
   - Implemented real-time progress updates using atomic operations and a separate goroutine

6. Error Handling:
   - Added error checking for file operations and Kafka message sending

7. Flexibility:
   - Support for processing multiple log files using wildcard patterns
   - Configurable batch size for fine-tuning performance

8. Resource Management:
   - Proper closing of file handles and Kafka producer
   - Using defer statements for cleanup operations

9. Concurrency:
   - Using sync.WaitGroup to manage goroutines
   - Implementing worker pool pattern for processing log lines

10. Memory Efficiency:
    - Using buffered channels to manage backpressure

11. Logging:
    - Improved error logging with more context

---

## Main Algorithm of the Program:

1. Program Initialization:
   - Get user input (Kafka properties file, topic, log file, batch size)
   - Validate input files

2. Kafka Setup:
   - Read and parse Kafka properties file
   - Create Kafka AsyncProducer with specified settings

3. File Processing Preparation:
   - Find log files matching the specified pattern (supporting wildcards)
   - Create counters for total lines and processed lines

4. Concurrent Processing Setup:
   - Create channels for passing lines between goroutines
   - Set up worker pool for line processing
   - Start goroutine for progress updates

5. File Processing:
   - Loop through each log file:
     a. Open file
     b. Read file line by line
     c. Send each line to channel for processing
     d. Increment total line count
   - Close channel after processing all files

6. Line Processing (Concurrent via worker pool):
   - Receive line from channel
   - Create Kafka message from line
   - Send message to Kafka producer
   - Increment processed line count

7. Progress Updates:
   - Periodically display progress percentage
   - Update display in real-time

8. Finalization:
   - Wait for all workers to complete
   - Close Kafka producer connection
   - Display completion status and processing statistics

9. Error Handling:
   - Catch and log errors occurring during processing
   - Display alerts for critical errors

This algorithm is designed for high efficiency using concurrent processing and efficient memory management. The use of AsyncProducer and worker pool allows for fast processing of large files and sending data to Kafka. Simultaneously, it includes error handling and progress reporting to allow users to monitor the processing status.

---
### sample data
```
du -hs /eduroam/kafka-log/eduroam-nro.log
```
```result
31G     /eduroam/kafka-log/eduroam-nro.log
```
```
wc -l /eduroam/kafka-log/eduroam-nro.log
```
```result
245062297 /eduroam/kafka-log/eduroam-nro.log
```
## run program
```
$ ./eduroam-2-kafka 
```
```result
Enter Kafka properties file (default: client.properties): 
Enter Kafka topic (default: eduroam-log): 
Enter log file or pattern (default: /eduroam/kafka-log/eduroam-nro.log): 
Enter batch size (default: 1000): 10000
Finished processing files

Progress: 100% (245062297/245062297) - 635758.49 lines/sec
Processing complete
```