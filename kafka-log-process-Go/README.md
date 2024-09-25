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