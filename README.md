# [Apache kafka](https://kafka.apache.org/)
## bootstrap.servers
- kafka.ipv9.me:9092,kafka.ipv9.me:9093,kafka.ipv9.me:9094
## security.protocol
- SASL_SSL
## [download Server certificate](http://sp06.ipv9.xyz/keys/kafka.truststore.jks)
- truststore.password = '   '
  

- [1.ACL](./ACL-k0100-test.md)
- [2.usecase](./LogCentral-usercase/1.Create-Topic.md)

---
```
$ keytool -list -v -keystore kafka.truststore.jks
Enter keystore password:  
```
```result
Keystore type: PKCS12
Keystore provider: SUN

Your keystore contains 1 entry

Alias name: caroot
Creation date: Sep 23, 2024
Entry type: trustedCertEntry

Owner: C=TH, ST=Pathumthani, L=CPE-K6, O=xOps2024, OU=DevOps, CN=Kafka-Security-CA
Issuer: C=TH, ST=Pathumthani, L=CPE-K6, O=xOps2024, OU=DevOps, CN=Kafka-Security-CA
Serial number: 10b6b42d668160d72f1b2cc520e031f66e9a7781
Valid from: Tue Sep 10 05:58:06 ICT 2024 until: Fri Sep 08 05:58:06 ICT 2034
Certificate fingerprints:
         SHA1: AB:8E:E3:8E:0D:4D:C4:ED:08:CD:74:E6:72:D1:28:FA:FE:67:94:77
         SHA256: C2:C0:BE:E7:B6:FB:71:2A:BE:60:24:C2:0E:3B:5D:54:E0:16:8F:08:4A:52:0C:08:78:AA:50:61:49:AB:B8:2F
Signature algorithm name: SHA256withRSA
Subject Public Key Algorithm: 2048-bit RSA key
Version: 3

Extensions: 

#1: ObjectId: 2.5.29.35 Criticality=false
AuthorityKeyIdentifier [
KeyIdentifier [
0000: 22 7C B3 2E 9B 54 97 5E   C8 C7 3C FB F6 9B 4D 5D  "....T.^..<...M]
0010: 9D E1 0C C1                                        ....
]
]

#2: ObjectId: 2.5.29.19 Criticality=true
BasicConstraints:[
  CA:true
  PathLen:2147483647
]

#3: ObjectId: 2.5.29.14 Criticality=false
SubjectKeyIdentifier [
KeyIdentifier [
0000: 22 7C B3 2E 9B 54 97 5E   C8 C7 3C FB F6 9B 4D 5D  "....T.^..<...M]
0010: 9D E1 0C C1                                        ....
]
]



*******************************************
*******************************************

```