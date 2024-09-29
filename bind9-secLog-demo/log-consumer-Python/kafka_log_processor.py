import ssl
from confluent_kafka import Consumer, KafkaError, TopicPartition
import json
from collections import defaultdict
import re
import logging
import datetime
import pytz
import uuid
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

try:
    import lz4
except ImportError:
    logging.warning("lz4 library not found. If your Kafka messages are compressed with lz4, please install it using 'pip install lz4'")

def load_properties(filename):
    properties = {}
    with open(filename, 'r') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#'):
                key, value = line.split('=', 1)
                properties[key.strip()] = value.strip()
    return properties

# Compile regular expressions
ip_pattern = re.compile(r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}')
domain_pattern = re.compile(r'\((.*?)\)')

def extract_ip_and_domain(log_entry):
    ip_match = ip_pattern.search(log_entry)
    domain_match = domain_pattern.search(log_entry)
    return (ip_match.group(0) if ip_match else None, 
            domain_match.group(1) if domain_match else None)

def create_kafka_consumer(properties_file, start_from_beginning=False):
    props = load_properties(properties_file)
    
    consumer_config = {
        'bootstrap.servers': props['bootstrap.servers'],
        'group.id': f'log-processor-{uuid.uuid4()}',
        'auto.offset.reset': 'earliest' if start_from_beginning else 'latest',
        'enable.auto.commit': 'false',  # Changed to false for manual offset management
        'security.protocol': props['security.protocol'],
        'sasl.mechanisms': props['sasl.mechanism'],
        'sasl.username': props['sasl.jaas.config'].split('username="')[1].split('"')[0],
        'sasl.password': props['sasl.jaas.config'].split('password="')[1].split('"')[0],
    }
    
    logging.debug(f"Consumer config: {consumer_config}")
    
    if props['security.protocol'] in ['SSL', 'SASL_SSL']:
        context = ssl.create_default_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        consumer_config['ssl.ca.location'] = None
        consumer_config['enable.ssl.certificate.verification'] = 'false'
        logging.warning("SSL verification is automatically skipped. This is not recommended for production use.")
    
    return Consumer(consumer_config)

def process_message_batch(messages):
    local_ip_counter = defaultdict(int)
    local_domain_counter = defaultdict(int)
    processed_count = 0
    skipped_count = 0
    
    for msg in messages:
        try:
            json_message = json.loads(msg.value().decode('utf-8'))
            if json_message.get('file_name') == 'security.log':
                log_entry = json_message.get('content', '')
                if 'denied' in log_entry:
                    processed_count += 1
                    ip, domain = extract_ip_and_domain(log_entry)
                    if ip:
                        local_ip_counter[ip] += 1
                    if domain:
                        local_domain_counter[domain] += 1
                else:
                    skipped_count += 1
            else:
                skipped_count += 1
        except (json.JSONDecodeError, UnicodeDecodeError):
            skipped_count += 1
    
    return local_ip_counter, local_domain_counter, processed_count, skipped_count

def save_results_to_file(ip_counter, domain_counter, processing_summary, first_message, last_message):
    results = []
    for i, (ip, count) in enumerate(sorted(ip_counter.items(), key=lambda x: x[1], reverse=True)):
        results.append({"type": "ip", "value": ip, "count": count})
        if i == 9:  # Only save top 10
            break
    
    for i, (domain, count) in enumerate(sorted(domain_counter.items(), key=lambda x: x[1], reverse=True)):
        results.append({"type": "domain", "value": domain, "count": count})
        if i == 9:  # Only save top 10
            break
    
    results.append({
        "type": "summary",
        "processing_summary": processing_summary,
        "first_processed_message": {
            "time": str(first_message[0]) if first_message else None,
            "raw_message": first_message[1] if first_message else None
        },
        "last_processed_message": {
            "time": str(last_message[0]) if last_message else None,
            "raw_message": last_message[1] if last_message else None
        }
    })
    
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"log_processing_results_{timestamp}.json"
    
    with open(filename, 'w') as f:
        json.dump(results, f, indent=2)
    
    logging.info(f"Results saved to {filename}")

def process_logs():
    consumer = create_kafka_consumer('k0100-client.properties', start_from_beginning=True)
    
    ip_counter = defaultdict(int)
    domain_counter = defaultdict(int)
    processed_count = 0
    skipped_count = 0
    first_message = None
    last_message = None
    start_time = time.time()
    total_messages = 0
    batch_size = 10000  # เพิ่มขนาด batch
    message_batch = []
    max_messages = 5000000  # จำกัดจำนวนข้อความที่จะประมวลผล

    try:
        consumer.subscribe(['logCentral'])
        logging.info("Subscribed to topic: logCentral")
        
        # Get partition information
        partitions = consumer.list_topics('logCentral').topics['logCentral'].partitions
        logging.info(f"Number of partitions: {len(partitions)}")
        
        for partition_id, partition_info in partitions.items():
            low_offset, high_offset = consumer.get_watermark_offsets(TopicPartition('logCentral', partition_id))
            logging.info(f"Partition {partition_id}: Low offset = {low_offset}, High offset = {high_offset}")
        
        with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
            future_to_batch = {}
            
            while total_messages < max_messages:
                msg = consumer.poll(0.1)  # ลดเวลารอ
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        break
                    else:
                        logging.error(f"Consumer error: {msg.error()}")
                    continue
                
                total_messages += 1
                message_batch.append(msg)
                
                if len(message_batch) >= batch_size:
                    future = executor.submit(process_message_batch, message_batch)
                    future_to_batch[future] = message_batch
                    message_batch = []
                
                # ประมวลผล futures ที่เสร็จสิ้นแล้ว
                for future in list(as_completed(future_to_batch)):
                    batch = future_to_batch[future]
                    local_ip_counter, local_domain_counter, local_processed, local_skipped = future.result()
                    
                    for ip, count in local_ip_counter.items():
                        ip_counter[ip] += count
                    for domain, count in local_domain_counter.items():
                        domain_counter[domain] += count
                    
                    processed_count += local_processed
                    skipped_count += local_skipped
                    
                    if first_message is None and local_processed > 0:
                        first_message = (datetime.datetime.fromtimestamp(batch[0].timestamp()[1] / 1000, pytz.timezone('Asia/Bangkok')), 
                                         batch[0].value().decode('utf-8'))
                    last_message = (datetime.datetime.fromtimestamp(batch[-1].timestamp()[1] / 1000, pytz.timezone('Asia/Bangkok')), 
                                    batch[-1].value().decode('utf-8'))
                    
                    del future_to_batch[future]
                
                if total_messages % 100000 == 0:
                    logging.info(f"Processed: {processed_count}, Skipped: {skipped_count}, Total: {total_messages}")

            # ประมวลผล batch สุดท้าย
            if message_batch:
                future = executor.submit(process_message_batch, message_batch)
                local_ip_counter, local_domain_counter, local_processed, local_skipped = future.result()
                for ip, count in local_ip_counter.items():
                    ip_counter[ip] += count
                for domain, count in local_domain_counter.items():
                    domain_counter[domain] += count
                processed_count += local_processed
                skipped_count += local_skipped

    except KeyboardInterrupt:
        logging.info("Interrupted by user. Closing consumer...")
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        import traceback
        logging.error(traceback.format_exc())
    finally:
        consumer.close()
        logging.info("Consumer closed")

    end_time = time.time()
    total_time = end_time - start_time
    average_processing_rate = processed_count / total_time if total_time > 0 else 0
    average_consumption_rate = total_messages / total_time if total_time > 0 else 0

    processing_summary = {
        "total_messages_consumed": total_messages,
        "total_messages_processed": processed_count,
        "total_messages_skipped": skipped_count,
        "total_unique_ip_addresses": len(ip_counter),
        "total_unique_domains": len(domain_counter),
        "average_processing_rate": average_processing_rate,
        "average_consumption_rate": average_consumption_rate,
        "total_processing_time": total_time
    }

    save_results_to_file(ip_counter, domain_counter, processing_summary, first_message, last_message)

    logging.info(f"Total processed: {processed_count}, Total skipped: {skipped_count}, "
                 f"Average processing rate: {average_processing_rate:.2f} messages/second, "
                 f"Average consumption rate: {average_consumption_rate:.2f} messages/second")

    print("\nTop 10 IP Addresses Denied:")
    for ip, count in sorted(ip_counter.items(), key=lambda x: x[1], reverse=True)[:10]:
        print(f"{ip}: {count}")

    print("\nTop 10 Domains Denied:")
    for domain, count in sorted(domain_counter.items(), key=lambda x: x[1], reverse=True)[:10]:
        print(f"{domain}: {count}")

    print("\nProcessing Summary:")
    print(f"Total messages consumed: {total_messages}")
    print(f"Total messages processed: {processed_count}")
    print(f"Total messages skipped: {skipped_count}")
    print(f"Total unique IP addresses: {len(ip_counter)}")
    print(f"Total unique domains: {len(domain_counter)}")
    print(f"Average processing rate: {average_processing_rate:.2f} messages/second")
    print(f"Average consumption rate: {average_consumption_rate:.2f} messages/second")

    if first_message:
        print(f"\nFirst processed message:")
        print(f"Time: {first_message[0]}")
        print(f"Raw message: {first_message[1]}")
    
    if last_message:
        print(f"\nLast processed message:")
        print(f"Time: {last_message[0]}")
        print(f"Raw message: {last_message[1]}")

if __name__ == "__main__":
    logging.info("Starting to process messages from the topic")
    process_logs()