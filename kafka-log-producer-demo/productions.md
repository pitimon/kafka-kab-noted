## produce to kafka 
> log's 2023

```
$ ls -l /eduroam/kafka-log/eduroam-nro.log
-rw-r--r-- 1 root root 25507944666 Sep 29 06:57 /eduroam/kafka-log/eduroam-nro.log
```
```
$ du -hs /eduroam/kafka-log/eduroam-nro.log
24G     /eduroam/kafka-log/eduroam-nro.log
```
```
$ wc -l /eduroam/kafka-log/eduroam-nro.log
193710301 /eduroam/kafka-log/eduroam-nro.log
```
```
$ date ; ./eduroam-2-kafka ; date
```
```result
Sun Sep 29 07:07:52 AM +07 2024
Enter Kafka properties file (default: client.properties): 
Enter Kafka topic (default: eduroam-log): eduroam-log-2023
Enter log file or pattern (default: /eduroam/kafka-log/eduroam-nro.log): 
Enter batch size (default: 1000): 10000
Finished processing files

Progress: 100% (193710301/193710301) - 738722.42 lines/sec
Processing complete
Sun Sep 29 07:31:09 AM +07 2024
```
---
> log's 2022
```
$ ls -l /eduroam/kafka-log/eduroam-nro.log 
-rw-r--r-- 1 root root 18726795936 Sep 29 07:36 /eduroam/kafka-log/eduroam-nro.log
```
```
$ du -hs /eduroam/kafka-log/eduroam-nro.log 
18G     /eduroam/kafka-log/eduroam-nro.log
```
```
$ wc -l /eduroam/kafka-log/eduroam-nro.log 
143723564 /eduroam/kafka-log/eduroam-nro.log
```
```
$ date ; ./eduroam-2-kafka ; date
```
```result
Sun Sep 29 07:39:17 AM +07 2024
Enter Kafka properties file (default: client.properties): 
Enter Kafka topic (default: eduroam-log): eduroam-log-2022
Enter log file or pattern (default: /eduroam/kafka-log/eduroam-nro.log): 
Enter batch size (default: 1000): 10000
Finished processing files

Progress: 100% (143723564/143723564) - 407924.92 lines/sec
Processing complete
Sun Sep 29 07:56:38 AM +07 2024
```
---
> log's 2021
```
$ ls -l /eduroam/kafka-log/eduroam-nro.log 
-rw-r--r-- 1 root root 11876568255 Sep 29 07:59 /eduroam/kafka-log/eduroam-nro.log
```
```
$ du -hs /eduroam/kafka-log/eduroam-nro.log 
12G     /eduroam/kafka-log/eduroam-nro.log
```
```
$ wc -l /eduroam/kafka-log/eduroam-nro.log 
92888811 /eduroam/kafka-log/eduroam-nro.log
```
```
$ date ; ./eduroam-2-kafka ; date
```
```result
Sun Sep 29 07:59:55 AM +07 2024
Enter Kafka properties file (default: client.properties): 
Enter Kafka topic (default: eduroam-log): eduroam-log-2021
Enter log file or pattern (default: /eduroam/kafka-log/eduroam-nro.log): 
Enter batch size (default: 1000): 10000

Progress: 90% (92888229/92888811) - 513394.47 lines/secFinished processing files

Progress: 100% (92888811/92888811) - 640636.15 lines/sec
Processing complete
Sun Sep 29 08:10:01 AM +07 2024
```
---