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

- ตัวอย่างการใช้งาน
```
% ./kafka-log-processor                                    
Choose end time option:
1. Current date and time
2. Specify date and time
Enter your choice (1 or 2): 1

Choose start time option:
1. Last 1 hour
2. Last 6 hours
3. Last 12 hours
4. Last 1 day
5. Last 7 days
6. Last 30 days
7. All available data
8. Specify custom date and time
Enter your choice (1-8): 7
2024/09/25 07:32:03 Script will process messages from 0001-01-01 00:00:00 +0000 UTC to 2024-09-25 07:32:01.089591 +0700 +07 m=+2.048205543
2024/09/25 07:32:03 Starting from the beginning: false
2024/09/25 07:32:03 Connected to Kafka. Starting to consume messages.
2024/09/25 07:32:03 Topics: [logCentral]

Processing Summary:
Total messages processed: 566070
Total messages skipped: 1006
Total denied queries: 566070
Time taken to consume messages: 194.991083ms
Time taken to process messages: 39.364433583s

Top 10 Countries with Denied IPs:
Brazil: 483001
Chile: 28299
Argentina: 18117
Colombia: 16385
Venezuela: 6742
Bolivia: 5229
Paraguay: 4639
Peru: 2062
United States: 1320
The Netherlands: 120

Top 10 Denied IPs per Country:

Peru:
191.97.49.156 (ASN: 28032): 5
190.108.82.29 (ASN: 28032): 3
200.233.46.15 (ASN: 28032): 3
190.108.94.76 (ASN: 28032): 3
38.255.110.194 (ASN: 272836): 3
190.108.94.121 (ASN: 28032): 3
191.97.49.96 (ASN: 28032): 3
177.126.50.213 (ASN: 271862): 3
38.56.220.176 (ASN: 272836): 3
191.97.50.26 (ASN: 28032): 3

Argentina:
201.217.246.159 (ASN: 52373): 5
201.219.185.223 (ASN: 28015): 4
186.148.242.221 (ASN: 52329): 4
45.227.166.54 (ASN: 265883): 4
190.107.126.212 (ASN: 28015): 4
200.47.50.134 (ASN: 7908): 4
170.78.192.232 (ASN: 20207): 4
45.7.249.197 (ASN: 265738): 4
190.112.217.116 (ASN: 52465): 4
45.230.64.254 (ASN: 266702): 4

United States:
149.78.3.131 (ASN: 268314): 3
38.183.186.4 (ASN: 269656): 3
38.196.225.108 (ASN: 273312): 3
38.196.224.192 (ASN: 273312): 3
38.183.186.236 (ASN: 269656): 3
38.183.186.198 (ASN: 269656): 3
38.183.186.214 (ASN: 269656): 3
38.196.224.144 (ASN: 273312): 3
38.183.186.111 (ASN: 269656): 3
38.196.225.115 (ASN: 273312): 3

Seychelles:
156.235.77.219 (ASN: 262167): 1
156.235.77.178 (ASN: 262167): 1
156.235.77.137 (ASN: 262167): 1
154.213.187.178 (ASN: 51396): 1

China:
112.31.213.26 (ASN: 9808): 3

United Arab Emirates:
181.214.172.96 (ASN: 211439): 1
181.214.172.211 (ASN: 211439): 1
181.214.172.219 (ASN: 211439): 1
181.214.172.38 (ASN: 211439): 1

Brazil:
179.43.51.4 (ASN: 270854): 5
181.189.31.61 (ASN: 52660): 5
168.195.138.229 (ASN: 265421): 5
45.167.246.109 (ASN: 273756): 5
201.219.249.3 (ASN: 61951): 5
191.242.39.84 (ASN: 263469): 5
45.225.244.77 (ASN: 270417): 5
200.192.159.177 (ASN: 28572): 5
168.195.138.68 (ASN: 265421): 5
45.164.164.174 (ASN: 268627): 5

Luxembourg:
92.223.98.111 (ASN: 199524): 2
92.223.98.218 (ASN: 199524): 1
92.223.98.135 (ASN: 199524): 1
92.223.98.255 (ASN: 199524): 1
92.223.98.246 (ASN: 199524): 1
92.223.98.192 (ASN: 199524): 1
92.223.98.189 (ASN: 199524): 1
92.223.98.210 (ASN: 199524): 1
92.223.98.56 (ASN: 199524): 1
92.223.98.177 (ASN: 199524): 1

Germany:
104.248.244.129 (ASN: 14061): 1

Bolivia:
170.82.244.245 (ASN: 264858): 4
170.82.244.94 (ASN: 264858): 4
181.41.159.121 (ASN: 27882): 4
181.41.159.243 (ASN: 27882): 4
181.188.171.14 (ASN: 27882): 4
181.114.123.97 (ASN: 27882): 3
181.41.147.24 (ASN: 27882): 3
170.82.244.247 (ASN: 264858): 3
170.82.244.255 (ASN: 264858): 3
181.188.171.70 (ASN: 27882): 3

Paraguay:
200.108.135.215 (ASN: 27669): 4
200.108.131.241 (ASN: 27669): 4
168.227.211.59 (ASN: 61614): 4
45.238.39.54 (ASN: 266831): 4
200.50.157.216 (ASN: 273095): 4
190.93.179.50 (ASN: 262183): 4
200.50.156.115 (ASN: 273095): 3
200.108.135.54 (ASN: 27669): 3
200.50.157.236 (ASN: 273095): 3
200.108.131.187 (ASN: 27669): 3

Venezuela:
181.208.45.138 (ASN: 21826): 4
190.97.246.171 (ASN: 263703): 4
38.159.57.142 (ASN: 269946): 3
190.97.238.110 (ASN: 263703): 3
181.208.60.185 (ASN: 21826): 3
190.97.233.153 (ASN: 263703): 3
190.97.233.164 (ASN: 263703): 3
181.208.60.226 (ASN: 21826): 3
190.142.229.236 (ASN: 21826): 3
190.97.238.201 (ASN: 263703): 3

Ecuador:
38.255.29.66 (ASN: 28158): 1
146.75.208.24 (ASN: 54113): 1
38.255.29.34 (ASN: 28158): 1
38.255.29.193 (ASN: 28158): 1
38.255.29.227 (ASN: 28158): 1

Trinidad and Tobago:
190.93.64.220 (ASN: 262361): 3
190.93.64.174 (ASN: 262361): 2
190.93.64.221 (ASN: 262361): 2
190.93.64.12 (ASN: 262361): 2
190.93.64.151 (ASN: 262361): 2
190.93.64.17 (ASN: 262361): 2
190.93.64.49 (ASN: 262361): 2
190.93.64.196 (ASN: 262361): 2
190.93.64.97 (ASN: 262361): 2
190.93.64.67 (ASN: 262361): 2

Canada:
159.203.41.210 (ASN: 14061): 1

Colombia:
179.1.79.192 (ASN: 262589): 5
45.162.79.203 (ASN: 262589): 5
45.183.198.9 (ASN: 265696): 5
168.227.105.140 (ASN: 52330): 4
45.183.198.67 (ASN: 265696): 4
45.162.79.180 (ASN: 262589): 4
179.1.64.61 (ASN: 262589): 4
45.162.79.26 (ASN: 262589): 4
45.162.79.37 (ASN: 262589): 4
179.1.79.30 (ASN: 262589): 4

Chile:
146.83.220.6 (ASN: 11340): 6
190.13.130.177 (ASN: 14117): 5
152.230.223.140 (ASN: 18822): 4
190.215.151.138 (ASN: 18822): 4
200.14.107.255 (ASN: 27746): 4
200.124.55.47 (ASN: 18822): 4
201.220.102.182 (ASN: 14117): 4
190.215.9.155 (ASN: 18822): 4
190.153.152.85 (ASN: 18822): 4
190.215.63.239 (ASN: 18822): 4

The Netherlands:
193.0.14.156 (ASN: 25152): 3
193.0.15.48 (ASN: 25152): 2
193.0.15.164 (ASN: 25152): 2
193.0.14.91 (ASN: 25152): 2
193.0.14.216 (ASN: 25152): 2
193.0.14.243 (ASN: 25152): 2
193.0.14.224 (ASN: 25152): 2
193.0.15.218 (ASN: 25152): 2
193.0.14.217 (ASN: 25152): 2
193.0.14.74 (ASN: 25152): 2

Costa Rica:
146.75.208.8 (ASN: 54113): 1
146.75.208.10 (ASN: 54113): 1

Top 10 Domains Denied:
globo.com: 49386
vmware.com: 49326
adobe.com: 48894
spotify.com: 48311
ebay.com: 47731
zendesk.com: 47006
cisco.com: 46505
atlassian.com: 46359
zoom.com: 46326
microsoft.com: 45706

Last processed message:
Time: 2024-09-25 07:32:00.860504627 +0700 +07
Content: 23-Sep-2024 13:26:14.067 security: info: client @0x7f6b83dcd168 191.253.33.21#44742 (ebay.com): query (cache) 'ebay.com/TXT/IN' denied (allow-query-cache did not match)
2024/09/25 07:32:43 Summary saved to result/log_analysis_summary_20240925_073243.txt
Export results to CSV? (y/n): y
2024/09/25 07:35:50 Results exported to result/log_analysis_20240925_073550.csv
```