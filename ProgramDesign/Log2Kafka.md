# โปรแกรม Kafka producer

## [plantUML render here..](https://plantuml.ipv9.me)

- Sequence Diagram: Diagram นี้แสดงการโต้ตอบระหว่างส่วนประกอบต่าง ๆ ของโปรแกรมอย่างละเอียด รวมถึงการทำงานแบบ concurrent ผ่าน goroutines และ channels 

![SeqDiagram](./img/l2k-seq.png)

```plantuml
@startuml
skinparam sequenceMessageAlign center

actor User
participant "Main" as Main
participant "KafkaConfig" as Config
participant "AsyncProducer" as Producer
participant "FileProcessor" as FileProc
participant "LineProcessor" as LineProc
participant "ProgressTracker" as Progress
database "Kafka" as Kafka
database "LogFiles" as LogFiles

User -> Main : Start program
activate Main

Main -> User : Request input (properties file, topic, log file, batch size)
User -> Main : Provide input

Main -> Config : readProperties(kafkaPropertiesFile)
activate Config
Config -> Config : Parse properties file
Config --> Main : Return KafkaConfig
deactivate Config

Main -> Producer : createKafkaProducer(config)
activate Producer
Producer -> Producer : Configure SASL/SSL and batching
Producer --> Main : Return AsyncProducer
deactivate Producer

Main -> FileProc : getLogFiles(logFile)
activate FileProc
FileProc -> FileProc : Check for wildcards and expand
FileProc --> Main : Return list of log files
deactivate FileProc

Main -> Progress : Start updateProgress goroutine
activate Progress

Main -> FileProc : Start processFile goroutines
activate FileProc

loop For each log file
    FileProc -> LogFiles : Open and read file
    activate LogFiles
    loop For each line in file
        LogFiles --> FileProc : Return line
        FileProc -> LineProc : Send line to linesChan
        activate LineProc
        LineProc -> Producer : Create and send ProducerMessage
        Producer -> Kafka : Send message
        LineProc -> Progress : Update processedLines count
        deactivate LineProc
    end
    LogFiles --> FileProc : End of file
    deactivate LogFiles
end

FileProc --> Main : All files processed
deactivate FileProc

Main -> LineProc : Close linesChan
deactivate LineProc

Main -> Producer : AsyncClose()
Producer -> Kafka : Flush remaining messages

Main -> Producer : Wait for Errors channel
Producer --> Main : Return any errors

Main -> Progress : Send done signal
deactivate Progress

Main -> User : Display final progress and completion message
deactivate Main

@enduml

```

คำอธิบายสำหรับ Sequence Diagram นี้:

1. User เริ่มโปรแกรมและให้ข้อมูลนำเข้าที่จำเป็น

2. Main อ่านไฟล์ properties ผ่าน KafkaConfig

3. Main สร้าง AsyncProducer โดยใช้การตั้งค่าที่อ่านมา

4. Main ค้นหาไฟล์ log ที่ต้องประมวลผลผ่าน FileProcessor

5. Main เริ่ม goroutine สำหรับ ProgressTracker เพื่อติดตามความคืบหน้า

6. Main เริ่ม goroutines สำหรับ FileProcessor เพื่อประมวลผลไฟล์:
   - FileProcessor อ่านแต่ละไฟล์
   - สำหรับแต่ละบรรทัด, ส่งไปยัง LineProcessor ผ่าน channel
   - LineProcessor สร้างและส่ง ProducerMessage ไปยัง AsyncProducer
   - AsyncProducer ส่งข้อความไปยัง Kafka
   - LineProcessor อัปเดตจำนวนบรรทัดที่ประมวลผลใน ProgressTracker

7. เมื่อประมวลผลไฟล์ทั้งหมดเสร็จ, Main ปิด linesChan

8. Main เรียก AsyncClose บน Producer เพื่อส่งข้อความที่เหลือ

9. Main รอและจัดการข้อผิดพลาดจาก Producer

10. Main ส่งสัญญาณ "done" ไปยัง ProgressTracker

11. Main แสดงข้อความสรุปและเสร็จสิ้นโปรแกรม



  
---
- Activity Diagram: Diagram นี้แสดงรายละเอียดการทำงานในระดับที่ลึกขึ้น รวมถึงการใช้ goroutines, channels, และการประมวลผลแบบขนาน ซึ่งเป็นลักษณะสำคัญของโปรแกรม Go 

![act](./img/l2k-act.png)

```plantuml
@startuml
title Kafka Producer Program - Low-Level Activity Diagram

start

:Get user input for:
- Kafka properties file
- Kafka topic
- Log file pattern
- Batch size;

:Read Kafka properties file;

:Create Kafka AsyncProducer:
- Set SASL/SSL if required
- Configure batch settings;

:Get log files based on pattern;

fork
  :Start updateProgress goroutine;
fork again
  partition "Process Files" {
    :Initialize totalLines and processedLines counters;
    :Create linesChan;
    :Start worker goroutines (NumCPU * 2);
    
    while (For each log file) is (more files)
      :Open file;
      while (Scan lines) is (more lines)
        :Send line to linesChan;
        :Increment totalLines;
      endwhile (no more lines)
      :Close file;
    endwhile (no more files)
    
    :Close linesChan;
    
    while (Process lines from linesChan) is (more lines)
      :Create Kafka ProducerMessage;
      :Send message to producer;
      :Increment processedLines;
    endwhile (no more lines)
  }
end fork

:Wait for all goroutines to complete;

:Close Kafka producer;

:Handle any remaining producer errors;

stop

@enduml

```

คำอธิบาย:

1. เริ่มต้นด้วยการรับข้อมูลจากผู้ใช้สำหรับการตั้งค่าต่าง ๆ
2. อ่านไฟล์ properties ของ Kafka
3. สร้าง Kafka AsyncProducer พร้อมกับตั้งค่า SASL/SSL และการ batch
4. ค้นหาไฟล์ log ตามรูปแบบที่ระบุ
5. แยกการทำงานเป็นสองส่วนแบบขนาน:
   a. เริ่ม goroutine สำหรับอัปเดตความคืบหน้า
   b. ประมวลผลไฟล์:
      - เตรียมตัวนับและช่องสำหรับส่งข้อมูล
      - เริ่ม worker goroutines
      - วนลูปอ่านแต่ละไฟล์และส่งแต่ละบรรทัดไปยังช่อง
      - ประมวลผลบรรทัดจากช่องและส่งไปยัง Kafka producer
6. รอให้ goroutines ทั้งหมดทำงานเสร็จ
7. ปิด Kafka producer
8. จัดการข้อผิดพลาดที่อาจเหลืออยู่จาก producer



---
- Use Case Diagram: Diagram นี้ให้ภาพรวมที่ละเอียดมากขึ้นของระบบ Kafka Producer โดยแสดงให้เห็นถึง:
  - กรณีการใช้งานย่อยที่ประกอบกันเป็นฟังก์ชันการทำงานหลัก
  - ความสัมพันธ์ที่ซับซ้อนระหว่าง Use Cases ต่าง ๆ
  - การปฏิสัมพันธ์กับระบบภายนอก (Kafka Cluster และ File System)
  - บทบาทที่แตกต่างกันของ User และ System Administrator 

![usecase](./img/l2k-usecase.png)

```plantuml
@startuml
left to right direction
skinparam actorStyle awesome
skinparam usecaseBackgroundColor LightBlue
skinparam usecaseBorderColor DarkBlue

actor "User" as user
actor "System Administrator" as admin
actor "Kafka Cluster" as kafka
actor "File System" as fs

rectangle "Kafka Producer System" {
  usecase "Configure Kafka Connection" as UC1
  usecase "Read Kafka Properties File" as UC1_1
  usecase "Set SASL/SSL Configuration" as UC1_2
  usecase "Configure Producer Settings" as UC1_3

  usecase "Specify Log File(s)" as UC2
  usecase "Support Wildcard Patterns" as UC2_1
  usecase "Validate File Existence" as UC2_2

  usecase "Set Batch Size" as UC3

  usecase "Process Log Files" as UC4
  usecase "Read Log Files" as UC4_1
  usecase "Parse Log Entries" as UC4_2

  usecase "Send Messages to Kafka" as UC5
  usecase "Create Kafka Producer" as UC5_1
  usecase "Batch Messages" as UC5_2
  usecase "Handle Retries" as UC5_3

  usecase "Monitor Progress" as UC6
  usecase "Display Real-time Statistics" as UC6_1
  usecase "Estimate Remaining Time" as UC6_2

  usecase "Handle Errors" as UC7
  usecase "Log Errors" as UC7_1
  usecase "Implement Error Recovery" as UC7_2

  usecase "Manage System Resources" as UC8
  usecase "Control Concurrency" as UC8_1
  usecase "Optimize Memory Usage" as UC8_2
}

user --> UC1
user --> UC2
user --> UC3
user --> UC4
user --> UC6

admin --> UC1
admin --> UC8

UC1 ..> UC1_1 : include
UC1 ..> UC1_2 : include
UC1 ..> UC1_3 : include

UC2 ..> UC2_1 : include
UC2 ..> UC2_2 : include

UC4 ..> UC4_1 : include
UC4 ..> UC4_2 : include
UC4 ..> UC5 : include
UC4 ..> UC6 : include
UC4 ..> UC7 : include

UC5 ..> UC5_1 : include
UC5 ..> UC5_2 : include
UC5 ..> UC5_3 : include

UC6 ..> UC6_1 : include
UC6 ..> UC6_2 : include

UC7 ..> UC7_1 : include
UC7 ..> UC7_2 : include

UC8 ..> UC8_1 : include
UC8 ..> UC8_2 : include

UC5 --> kafka
UC4_1 --> fs
UC1_1 --> fs

@enduml

```

คำอธิบายสำหรับ Use Case Diagram:

1. Actors (ผู้กระทำ):
   - User: ผู้ใช้งานทั่วไปของโปรแกรม
   - System Administrator: ผู้ดูแลระบบที่มีสิทธิ์ในการกำหนดค่าขั้นสูง
   - Kafka Cluster: ระบบ Kafka ที่รับข้อความ
   - File System: ระบบไฟล์ที่เก็บ log files และ configuration files

2. Use Cases หลัก (กรณีการใช้งานหลัก):
   - Configure Kafka Connection (UC1)
   - Specify Log File(s) (UC2)
   - Set Batch Size (UC3)
   - Process Log Files (UC4)
   - Send Messages to Kafka (UC5)
   - Monitor Progress (UC6)
   - Handle Errors (UC7)
   - Manage System Resources (UC8)

3. Use Cases ย่อย (กรณีการใช้งานย่อย):
   สำหรับแต่ละ Use Case หลัก มีการแยกย่อยเป็น Use Cases ที่เฉพาะเจาะจงมากขึ้น เช่น:
   - UC1 แยกเป็น Read Kafka Properties File, Set SASL/SSL Configuration, Configure Producer Settings
   - UC4 แยกเป็น Read Log Files, Parse Log Entries
   - UC5 แยกเป็น Create Kafka Producer, Batch Messages, Handle Retries
   - และอื่น ๆ

4. ความสัมพันธ์:
   - ใช้ความสัมพันธ์แบบ "include" เพื่อแสดงว่า Use Case หลักประกอบด้วย Use Cases ย่อย
   - แสดงการปฏิสัมพันธ์ระหว่าง Use Cases กับ Actors ภายนอกระบบ เช่น Kafka Cluster และ File System

5. รายละเอียดเพิ่มเติม:
   - แสดงให้เห็นว่า System Administrator มีบทบาทในการกำหนดค่าการเชื่อมต่อ Kafka และจัดการทรัพยากรระบบ
   - เพิ่ม Use Case สำหรับการจัดการทรัพยากรระบบ (UC8) ซึ่งรวมถึงการควบคุม concurrency และการใช้หน่วยความจำ



---
- Class Diagram: Diagram นี้แสดงให้เห็นโครงสร้างและความสัมพันธ์ระหว่างคลาสต่าง ๆ ในโปรแกรม ช่วยให้เข้าใจการออกแบบและการแบ่งความรับผิดชอบของแต่ละส่วนในระบบ แสดงโครงสร้างของคลาสและความสัมพันธ์ระหว่างคลาสต่าง ๆ ที่สำคัญในโปรแกรม

![class](./img/l2k-class.png)

```plantuml
@startuml
skinparam classAttributeIconSize 0

class Main {
  - kafkaPropertiesFile: string
  - kafkaTopic: string
  - logFile: string
  - batchSize: int
  + main()
}

class KafkaConfig {
  - BootstrapServers: string
  - SecurityProtocol: string
  - SaslMechanism: string
  - SaslUsername: string
  - SaslPassword: string
}

class AsyncProducer {
  + Input() chan<- *ProducerMessage
  + Errors() <-chan *ProducerError
  + AsyncClose()
}

class FileProcessor {
  - totalLines: *int64
  - processedLines: *int64
  - linesChan: chan<- string
  + processFile(filePath: string, producer: AsyncProducer)
}

class LineProcessor {
  - producer: AsyncProducer
  - processedLines: *int64
  + processLines(lines: <-chan string)
}

class ProgressTracker {
  - totalLines: *int64
  - processedLines: *int64
  - doneChan: <-chan bool
  + updateProgress()
}

Main --> KafkaConfig : uses
Main --> AsyncProducer : creates and uses
Main --> FileProcessor : creates and uses
Main --> LineProcessor : creates and uses
Main --> ProgressTracker : creates and uses

FileProcessor --> LineProcessor : sends lines to
LineProcessor --> AsyncProducer : sends messages to
ProgressTracker ..> FileProcessor : monitors
ProgressTracker ..> LineProcessor : monitors

@enduml

```

คำอธิบายสำหรับ Class Diagram นี้:

1. Main: คลาสหลักที่ควบคุมการทำงานของโปรแกรม
   - มีฟิลด์สำหรับเก็บค่าการตั้งค่าต่าง ๆ
   - มีเมธอด main() ที่เป็นจุดเริ่มต้นของโปรแกรม

2. KafkaConfig: คลาสที่เก็บการตั้งค่าสำหรับการเชื่อมต่อ Kafka
   - มีฟิลด์สำหรับเก็บค่าการตั้งค่าต่าง ๆ ของ Kafka

3. AsyncProducer: อินเตอร์เฟซที่แทน Kafka AsyncProducer
   - มีเมธอดสำหรับส่งข้อความและจัดการข้อผิดพลาด

4. FileProcessor: คลาสที่จัดการการอ่านไฟล์
   - มีฟิลด์สำหรับเก็บค่าตัวนับและช่องสำหรับส่งบรรทัด
   - มีเมธอด processFile() สำหรับประมวลผลไฟล์

5. LineProcessor: คลาสที่จัดการการประมวลผลแต่ละบรรทัด
   - มีฟิลด์สำหรับ producer และตัวนับบรรทัดที่ประมวลผลแล้ว
   - มีเมธอด processLines() สำหรับประมวลผลบรรทัดและส่งไปยัง Kafka

6. ProgressTracker: คลาสที่ติดตามและแสดงความคืบหน้า
   - มีฟิลด์สำหรับตัวนับและช่องสัญญาณ
   - มีเมธอด updateProgress() สำหรับอัปเดตและแสดงความคืบหน้า

ความสัมพันธ์ระหว่างคลาส:
- Main ใช้งานคลาสอื่น ๆ ทั้งหมด
- FileProcessor ส่งข้อมูลไปยัง LineProcessor
- LineProcessor ส่งข้อความไปยัง AsyncProducer
- ProgressTracker ติดตามความคืบหน้าของ FileProcessor และ LineProcessor



---
