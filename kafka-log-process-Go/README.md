>> environment
- Kafka Bootstap: kafka.ipv9.me:9092,kafka.ipv9.me:9093,kafka.ipv9.me:9094
- Topic: logCental
- security: SASL_SSL

- source /var/log/bind9/security.log => kafka topic

---

>> [Summary of Functions](./details/1.funcSumm.md)
  
---
>> Go Build

- [Go download](https://go.dev/dl/)
```
cp k0100-client.properties-master k0100-client.properties
```
- change Username, Password , keystore password before use.
  
```
go mod init kafka-log-processor

go get github.com/IBM/sarama
go get github.com/oschwald/geoip2-golang
go get github.com/spf13/viper

go build -o kafka-log-processor
```

---

>> [ตัวอย่างการใช้งาน](./details/run_program.md)
>> [Results](./result/)
