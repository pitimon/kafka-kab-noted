ขั้นตอนการลบสิทธิ์ทั้งหมดของ k0100 และวิธีการทดสอบ ACL command :

1. ลบสิทธิ์ทั้งหมดของ k0100:

```bash
./bin/kafka-acls.sh --bootstrap-server kafka.ipv9.me:9092,kafka.ipv9.me:9093,kafka.ipv9.me:9094 \
  --command-config admin-client.properties \
  --remove \
  --allow-principal User:k0100 \
  --operation All \
  --topic '*' \
  --group '*' \
  --cluster \
  --force
```

2. ตรวจสอบว่าสิทธิ์ถูกลบหมดแล้ว:

```bash
./bin/kafka-acls.sh --bootstrap-server kafka.ipv9.me:9092,kafka.ipv9.me:9093,kafka.ipv9.me:9094 \
  --command-config admin-client.properties \
  --list \
  --principal User:k0100
```
---
3. เพิ่มสิทธิ์ใหม่ตาม ACL command ที่แนะนำ:

```
./bin/kafka-acls.sh --bootstrap-server kafka.ipv9.me:9092,kafka.ipv9.me:9093,kafka.ipv9.me:9094 \
  --command-config admin-client.properties \
  --add --allow-principal User:k0100 \
  --operation Write --topic eduroam-log

./bin/kafka-acls.sh --bootstrap-server kafka.ipv9.me:9092,kafka.ipv9.me:9093,kafka.ipv9.me:9094 \
  --command-config admin-client.properties \
  --add --allow-principal User:k0100 \
  --operation Read --group '*'
```

---
4. ทดสอบการทำงานกับ topic eduroam-log:

   a. ทดสอบการอ่านข้อมูล:
   ```bash
   ./bin/kafka-console-consumer.sh --bootstrap-server kafka.ipv9.me:9092,kafka.ipv9.me:9093,kafka.ipv9.me:9094 \
     --topic eduroam-log \
     --from-beginning \
     --max-messages 1 \
     --consumer.config k0100-client.properties
   ```

   b. ทดสอบการเขียนข้อมูล:
   ```bash
   echo "Test message from k0100" | ./bin/kafka-console-producer.sh \
     --broker-list kafka.ipv9.me:9092,kafka.ipv9.me:9093,kafka.ipv9.me:9094 \
     --topic eduroam-log \
     --producer.config k0100-client.properties
   ```

   c. ทดสอบการใช้ consumer group:
   ```bash
   ./bin/kafka-console-consumer.sh --bootstrap-server kafka.ipv9.me:9092,kafka.ipv9.me:9093,kafka.ipv9.me:9094 \
     --topic eduroam-log \
     --group k0100-test-group \
     --from-beginning \
     --max-messages 1 \
     --consumer.config k0100-client.properties
   ```

5. ตรวจสอบ consumer group:
```bash
./bin/kafka-consumer-groups.sh --bootstrap-server kafka.ipv9.me:9092,kafka.ipv9.me:9093,kafka.ipv9.me:9094 \
  --describe --group k0100-test-group \
  --command-config k0100-client.properties
```

หากทุกขั้นตอนสามารถทำงานได้โดยไม่มีข้อผิดพลาด แสดงว่า ACL command ที่แนะนำสามารถให้สิทธิ์ที่ถูกต้องแก่ k0100 ในการทำงานกับ topic eduroam-log ได้

