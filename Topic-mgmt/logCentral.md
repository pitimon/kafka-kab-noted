---
- สร้าง topic
```
./bin/kafka-topics.sh --create --topic logCentral   --bootstrap-server kafka.ipv9.me:9092,kafka.ipv9.me:9093,kafka.ipv9.me:9094   --command-config admin-client.properties   --partitions 8   --replication-factor 3   --config compression.type=producer   --config retention.ms=10368000000
```
- ปรับจำนวน partition
```
./bin/kafka-topics.sh --alter \
  --topic logCentral \
  --bootstrap-server kafka.ipv9.me:9092,kafka.ipv9.me:9093,kafka.ipv9.me:9094 \
  --command-config admin-client.properties \
  --partitions 16
  
./bin/kafka-topics.sh --describe \
  --topic logCentral \
  --bootstrap-server kafka.ipv9.me:9092,kafka.ipv9.me:9093,kafka.ipv9.me:9094 \
  --command-config admin-client.properties
```