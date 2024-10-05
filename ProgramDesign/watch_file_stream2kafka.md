# โปรแกรม Kafka Producer และ File Watcher
--- 
> copy โค๊ดด้านล่างของรูป ขยายที่ [plantUML render](https://plantuml.ipv9.me) เพื่อตวามชัดเจนของรูปได้
---
### Kafka Producer and File watcher

คำอธิบาย:
โปรแกรม Kafka Producer นี้เขียนด้วยภาษา Python เป็นระบบติดตามการเปลี่ยนแปลงของไฟล์ log แบบ real-time โดยใช้ pyinotify และส่งข้อมูลไปยัง Kafka cluster โปรแกรมรองรับการเชื่อมต่อแบบ SASL/SSL กับ Kafka และมีการจัดการ timestamp ของข้อมูลอย่างเหมาะสม

Features:

1. การติดตามไฟล์แบบ Real-time:
   ใช้ pyinotify เพื่อติดตามการเปลี่ยนแปลงของไฟล์ log แบบ real-time สามารถตรวจจับทั้งการแก้ไขไฟล์และการสร้างไฟล์ใหม่ ทำให้สามารถประมวลผลข้อมูลได้ทันทีที่มีการเปลี่ยนแปลง

2. การรักษาความปลอดภัยและการกำหนดค่าแบบยืดหยุ่น:
   รองรับการเชื่อมต่อ Kafka แบบ SASL/SSL และสามารถอ่านการตั้งค่าจากไฟล์ properties ทำให้สามารถปรับแต่งการเชื่อมต่อและการรักษาความปลอดภัยได้โดยไม่ต้องแก้ไขโค้ด

3. การจัดการ Timestamp และโครงสร้างข้อมูล:
   มีการแยก timestamp จากข้อมูล log และสร้างโครงสร้างข้อมูลที่เหมาะสมก่อนส่งไปยัง Kafka โดยรวมชื่อไฟล์, เนื้อหา, และ timestamp ไว้ในข้อความเดียวกัน ช่วยให้การวิเคราะห์และประมวลผลข้อมูลในภายหลังทำได้สะดวกยิ่งขึ้น

---
### Sequence Diagram 
- Diagram นี้แสดงให้เห็นการทำงานแบบต่อเนื่องของระบบในการติดตามการเปลี่ยนแปลงของไฟล์และส่งข้อมูลไปยัง Kafka โดยเฉพาะ:
  - การสร้างและกำหนดค่า KafkaProducer
  - การติดตามการเปลี่ยนแปลงของไฟล์ด้วย pyinotify
  - การจัดการกับเหตุการณ์การแก้ไขไฟล์และการสร้างไฟล์ใหม่
  - กระบวนการอ่านไฟล์และส่งข้อมูลไปยัง Kafka
![seq](./img/wf2k-seq.png)

```plantuml
@startuml
skinparam sequenceMessageAlign center

actor User
participant "Main" as Main
participant "FileHandler" as FileHandler
participant "WatchManager" as WatchManager
participant "Notifier" as Notifier
participant "KafkaProducer" as KafkaProducer
participant "File System" as FileSystem
database "Kafka" as Kafka

User -> Main: Start program
activate Main

Main -> FileHandler: Create
activate FileHandler

FileHandler -> KafkaProducer: create_kafka_producer()
activate KafkaProducer
KafkaProducer -> KafkaProducer: read_properties()
KafkaProducer -> KafkaProducer: Configure SSL (if needed)
KafkaProducer --> FileHandler: Return producer
deactivate KafkaProducer

Main -> WatchManager: Create
activate WatchManager

Main -> Notifier: Create
activate Notifier

Main -> WatchManager: add_watch()
WatchManager -> FileSystem: Monitor directory

Main -> Notifier: loop()
Notifier -> WatchManager: Start monitoring

loop File events
    FileSystem -> WatchManager: File modified/created
    WatchManager -> Notifier: Notify event
    Notifier -> FileHandler: process_IN_MODIFY() / process_IN_CREATE()
    activate FileHandler
    
    alt File modified
        FileHandler -> FileSystem: Open and read file
        FileSystem --> FileHandler: Return file content
        loop For each new line
            FileHandler -> FileHandler: extract_timestamp()
            FileHandler -> KafkaProducer: send()
            KafkaProducer -> Kafka: Send message
            Kafka --> KafkaProducer: Acknowledge
            KafkaProducer --> FileHandler: Return result
        end
    else New file created
        FileHandler -> FileHandler: Reset file position
        FileHandler -> FileSystem: Open and read file
        FileSystem --> FileHandler: Return file content
        loop For each line
            FileHandler -> FileHandler: extract_timestamp()
            FileHandler -> KafkaProducer: send()
            KafkaProducer -> Kafka: Send message
            Kafka --> KafkaProducer: Acknowledge
            KafkaProducer --> FileHandler: Return result
        end
    end
    deactivate FileHandler
end

@enduml

```

คำอธิบายสำหรับ Sequence Diagram นี้:

1. User เริ่มโปรแกรม

2. Main สร้าง FileHandler:
   - FileHandler สร้าง KafkaProducer โดยเรียกใช้ create_kafka_producer()
   - KafkaProducer อ่านค่าจากไฟล์ properties และกำหนดค่า SSL ถ้าจำเป็น

3. Main สร้าง WatchManager และ Notifier

4. Main เพิ่มการติดตามไดเรกทอรีที่ต้องการผ่าน WatchManager

5. Main เริ่ม loop ของ Notifier เพื่อเริ่มการติดตามไฟล์

6. เมื่อมีเหตุการณ์เกิดขึ้นกับไฟล์ (แก้ไขหรือสร้างใหม่):
   - FileSystem แจ้ง WatchManager
   - WatchManager แจ้ง Notifier
   - Notifier เรียก process_IN_MODIFY() หรือ process_IN_CREATE() ของ FileHandler

7. FileHandler ดำเนินการตามประเภทของเหตุการณ์:
   - ถ้าไฟล์ถูกแก้ไข: อ่านเฉพาะส่วนใหม่ของไฟล์
   - ถ้าไฟล์ถูกสร้างใหม่: รีเซ็ตตำแหน่งการอ่านและอ่านไฟล์ทั้งหมด

8. สำหรับแต่ละบรรทัดที่อ่านได้:
   - FileHandler แยกข้อมูล timestamp
   - FileHandler ส่งข้อความไปยัง KafkaProducer
   - KafkaProducer ส่งข้อความไปยัง Kafka
   - Kafka ตอบกลับเพื่อยืนยันการรับข้อความ

---

### Use Case Diagram, Class Diagram, และ Activity Diagram 

#### Use Case Diagram:
- diagrams เหล่านี้ช่วยให้เห็นภาพรวมของระบบในมุมมองที่แตกต่างกัน ทั้งด้านฟังก์ชันการทำงาน, โครงสร้าง, และลำดับการทำงาน
![usercase](./img/wf2k-usecase.png)

```plantuml
@startuml
left to right direction
actor "User" as user
actor "File System" as fs
actor "Kafka Cluster" as kafka

rectangle "Kafka Producer and File Watcher System" {
  usecase "Start File Monitoring" as UC1
  usecase "Configure Kafka Connection" as UC2
  usecase "Monitor File Changes" as UC3
  usecase "Process Modified File" as UC4
  usecase "Process New File" as UC5
  usecase "Extract Timestamp" as UC6
  usecase "Send Message to Kafka" as UC7
  usecase "Log System Activities" as UC8
}

user --> UC1
user --> UC2
fs --> UC3
UC3 ..> UC4 : include
UC3 ..> UC5 : include
UC4 ..> UC6 : include
UC5 ..> UC6 : include
UC4 ..> UC7 : include
UC5 ..> UC7 : include
UC7 --> kafka
UC1 ..> UC8 : include
UC3 ..> UC8 : include
UC7 ..> UC8 : include

@enduml

```

#### Class Diagram:
![class](./img/wf2k-class.png)


```plantuml
@startuml
skinparam classAttributeIconSize 0

class Main {
  + main()
}

class FileHandler {
  - producer: KafkaProducer
  - current_file: str
  - file_position: int
  + __init__()
  + process_IN_MODIFY(event: Event)
  + process_IN_CREATE(event: Event)
  - process_file()
  - send_line_to_kafka(line: str)
  - extract_timestamp(line: str): float
}

class KafkaProducer {
  + send(topic: str, value: dict): Future
}

class WatchManager {
  + add_watch(path: str, mask: int)
}

class Notifier {
  + loop()
}

Main --> FileHandler : creates
Main --> WatchManager : creates
Main --> Notifier : creates
FileHandler --> KafkaProducer : uses
Notifier --> WatchManager : uses
Notifier --> FileHandler : notifies

@enduml

```

#### Activity Diagram:
![act](./img/wf2k-act.png)

```plantuml
@startuml
start

:Initialize Logging;
:Read Kafka Properties;
:Create Kafka Producer;
:Create File Handler;
:Create Watch Manager;
:Create Notifier;

:Add Watch to Directory;

fork
  :Start File Monitoring Loop;
  while (Monitoring Active?) is (yes)
    if (File Event Detected?) then (yes)
      if (File Modified?) then (yes)
        :Process Modified File;
      else (no)
        :Process New File;
      endif
      :Extract Timestamp;
      :Send Message to Kafka;
      :Log Activity;
    else (no)
      :Wait for Event;
    endif
  endwhile (no)
fork again
  :Handle User Interrupts;
end fork

:Close Kafka Producer;
:Stop Monitoring;

stop

@enduml

```

คำอธิบายสำหรับแต่ละ diagram:

1. Use Case Diagram:
   - แสดงการปฏิสัมพันธ์ระหว่างผู้ใช้, ระบบไฟล์, และ Kafka Cluster กับระบบ
   - ระบุ use cases หลัก เช่น การเริ่มการติดตามไฟล์, การกำหนดค่าการเชื่อมต่อ Kafka, และการประมวลผลไฟล์

2. Class Diagram:
   - แสดงโครงสร้างของคลาสหลักในระบบ ได้แก่ Main, FileHandler, KafkaProducer, WatchManager, และ Notifier
   - แสดงความสัมพันธ์ระหว่างคลาส เช่น การสร้างและการใช้งาน

3. Activity Diagram:
   - แสดงลำดับขั้นตอนการทำงานของระบบ ตั้งแต่การเริ่มต้นจนถึงการสิ้นสุดการทำงาน
   - รวมถึงการทำงานแบบขนาน (parallel) สำหรับการติดตามไฟล์และการจัดการการหยุดทำงานจากผู้ใช้

---