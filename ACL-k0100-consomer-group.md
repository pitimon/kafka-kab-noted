## Consumer Groups คืออะไร?
Consumer Group เป็นกลุ่มของ consumers ที่ทำงานร่วมกันเพื่อบริโภคข้อมูลจาก Kafka topics. แต่ละ consumer ใน group จะรับผิดชอบการอ่านข้อมูลจาก partitions ที่แตกต่างกันของ topic

ประโยชน์และการใช้งานของ Consumer Groups:

1. การกระจายภาระงาน (Load Balancing):
   - Consumer groups ช่วยกระจายภาระงานในการอ่านข้อมูลระหว่าง consumers หลายตัว
   - แต่ละ consumer จะรับผิดชอบ partitions ที่แตกต่างกัน ทำให้สามารถประมวลผลข้อมูลได้เร็วขึ้น

2. การขยายระบบ (Scalability):
   - สามารถเพิ่มจำนวน consumers ใน group เพื่อรองรับปริมาณข้อมูลที่เพิ่มขึ้น
   - Kafka จะจัดสรร partitions ใหม่โดยอัตโนมัติเมื่อมีการเพิ่มหรือลด consumers

3. ความทนทานต่อความล้มเหลว (Fault Tolerance):
   - หาก consumer ตัวใดตัวหนึ่งล้มเหลว Kafka จะกระจาย partitions ให้กับ consumers ที่เหลือโดยอัตโนมัติ
   - ช่วยให้ระบบทำงานต่อเนื่องแม้เกิดปัญหากับ consumer บางตัว

4. การประมวลผลแบบขนาน (Parallel Processing):
   - หลาย consumers สามารถประมวลผลข้อมูลจากหลาย partitions พร้อมกัน
   - เพิ่มประสิทธิภาพในการประมวลผลข้อมูลปริมาณมาก

5. การจัดการ Offset:
   - Consumer group จัดการ offset (ตำแหน่งการอ่านข้อมูล) ให้อัตโนมัติ
   - ช่วยให้มั่นใจว่าข้อมูลทุกส่วนถูกประมวลผลและไม่มีการซ้ำซ้อน

6. การแยกการประมวลผลตามประเภทงาน:
   - สามารถมีหลาย consumer groups สำหรับ topic เดียวกัน
   - แต่ละ group อาจทำหน้าที่ประมวลผลข้อมูลแตกต่างกัน เช่น group หนึ่งสำหรับการวิเคราะห์ อีก group สำหรับการเก็บข้อมูลลง database

7. การรับประกันลำดับข้อมูล:
   - ภายใน partition เดียวกัน ข้อมูลจะถูกส่งให้ consumer ตามลำดับ
   - เหมาะสำหรับงานที่ต้องการความต่อเนื่องของข้อมูล

8. การจัดการ Rebalancing:
   - เมื่อมีการเปลี่ยนแปลงใน group (เช่น consumer เพิ่มหรือลด) Kafka จะทำ rebalancing โดยอัตโนมัติ
   - ช่วยรักษาประสิทธิภาพการทำงานของระบบ

## ในกรณีของ k0100 การให้สิทธิ์เกี่ยวกับ consumer groups ทำให้สามารถสร้างและจัดการ consumers ที่ทำงานร่วมกันเพื่อประมวลผลข้อมูลจาก topic eduroam-log ได้อย่างมีประสิทธิภาพและยืดหยุ่น


- สำหรับการให้สิทธิ์ในการสร้างและใช้งาน consumer groups ให้กับ k0100 เราสามารถใช้ operation "All" แทน ซึ่งจะครอบคลุมทุกการดำเนินการที่เกี่ยวข้องกับ consumer groups รวมถึงการสร้างด้วย:

```bash
./bin/kafka-acls.sh --bootstrap-server kafka.ipv9.me:9092,kafka.ipv9.me:9093,kafka.ipv9.me:9094 \
  --command-config admin-client.properties \
  --add --allow-principal User:k0100 \
  --operation All --group '*'
```

คำสั่งนี้จะให้สิทธิ์ทั้งหมดที่เกี่ยวข้องกับ consumer groups แก่ k0100 ซึ่งรวมถึง:
- การสร้าง consumer groups ใหม่
- การอ่านข้อมูลจาก consumer groups
- การลบ consumer groups
- การดูรายละเอียดของ consumer groups

อย่างไรก็ตาม หากคุณต้องการจำกัดสิทธิ์ให้น้อยลง คุณสามารถใช้เฉพาะ operations ที่จำเป็น เช่น:

```bash
./bin/kafka-acls.sh --bootstrap-server kafka.ipv9.me:9092,kafka.ipv9.me:9093,kafka.ipv9.me:9094 \
  --command-config admin-client.properties \
  --add --allow-principal User:k0100 \
  --operation Read,Describe --group '*'
```

คำสั่งนี้จะให้สิทธิ์เฉพาะการอ่านและดูรายละเอียดของ consumer groups แต่จะไม่อนุญาตให้สร้างหรือลบ groups

