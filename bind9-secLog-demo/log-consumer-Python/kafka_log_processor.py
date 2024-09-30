import ssl
from confluent_kafka import Consumer, KafkaError, TopicPartition
import json
from collections import Counter
import re
import logging
import datetime
import pytz
import uuid
import os
import time
import multiprocessing as mp
from multiprocessing import Manager, Pool, Value, Lock, Queue

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

def timestamp_to_datetime(timestamp):
    dt_utc = datetime.datetime.fromtimestamp(timestamp, datetime.UTC)
    local_tz = pytz.timezone('Asia/Bangkok')  # Adjust this to your local timezone
    dt_local = dt_utc.astimezone(local_tz)
    return dt_local

def extract_message_data(msg):
    return {
        'topic': msg.topic(),
        'partition': msg.partition(),
        'offset': msg.offset(),
        'timestamp': msg.timestamp(),
        'value': msg.value().decode('utf-8') if msg.value() else None,
    }

def get_end_datetime():
    while True:
        choice = input("Choose end time option:\n1. Current date and time\n2. Specify date and time\nEnter your choice (1 or 2): ")
        if choice == '1':
            return datetime.datetime.now(pytz.timezone('Asia/Bangkok'))
        elif choice == '2':
            while True:
                try:
                    end_datetime_str = input("Enter the end date and time (YYYY-MM-DD HH:MM:SS): ")
                    end_datetime = datetime.datetime.strptime(end_datetime_str, "%Y-%m-%d %H:%M:%S")
                    return pytz.timezone('Asia/Bangkok').localize(end_datetime)
                except ValueError:
                    print("Invalid date and time format. Please use YYYY-MM-DD HH:MM:SS.")
        else:
            print("Invalid choice. Please enter 1 or 2.")

def process_message_batch(batch_data, result_queue, end_datetime):
    local_ip_counter = Counter()
    local_domain_counter = Counter()
    local_processed = 0
    local_skipped = 0
    reached_end = False
    last_processed_time = 0

    for msg_data in batch_data:
        try:
            json_message = json.loads(msg_data['value'])
            if json_message.get('file_name') == 'security.log':
                timestamp = json_message.get('timestamp')
                message_datetime = timestamp_to_datetime(timestamp)
                
                if message_datetime > end_datetime:
                    reached_end = True
                    break

                local_processed += 1
                last_processed_time = max(last_processed_time, timestamp)

                log_entry = json_message.get('content', '')
                if 'denied' in log_entry:
                    ip, domain = extract_ip_and_domain(log_entry)
                    if ip:
                        local_ip_counter[ip] += 1
                    if domain:
                        local_domain_counter[domain] += 1
            else:
                local_skipped += 1
        except json.JSONDecodeError:
            local_skipped += 1

    result_queue.put((dict(local_ip_counter), dict(local_domain_counter), local_processed, local_skipped, reached_end, last_processed_time))

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

def signal_handler(signum, frame):
    global should_stop
    should_stop = True
    logging.info("Received signal to stop. Finishing current batch...")

def process_logs(end_datetime, start_from_beginning):
    manager = Manager()
    ip_counter = manager.dict()
    domain_counter = manager.dict()
    processed_count = Value('i', 0)
    skipped_count = Value('i', 0)
    consumed_count = Value('i', 0)
    last_processed_time = Value('d', 0)
    should_stop = Value('b', False)

    consumer = create_kafka_consumer('k0100-client.properties', start_from_beginning)
    
    first_message = manager.list([None, None])
    last_message = manager.list([None, None])
    start_time = time.time()

    try:
        consumer.subscribe(['logCentral'])
        
        with Pool(processes=os.cpu_count()) as pool:
            batch = []
            batch_size = 1000
            result_queue = Queue()

            while not should_stop.value:
                msg = consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logging.info('Reached end of partition')
                    else:
                        logging.error(f"Consumer error: {msg.error()}")
                    continue
                
                with consumed_count.get_lock():
                    consumed_count.value += 1
                batch.append(extract_message_data(msg))

                if len(batch) >= batch_size:
                    pool.apply_async(process_message_batch, (batch, result_queue, end_datetime))
                    batch = []

                while not result_queue.empty():
                    local_ip_counter, local_domain_counter, local_processed, local_skipped, reached_end, local_last_processed_time = result_queue.get()
                    
                    for ip, count in local_ip_counter.items():
                        ip_counter[ip] = ip_counter.get(ip, 0) + count
                    for domain, count in local_domain_counter.items():
                        domain_counter[domain] = domain_counter.get(domain, 0) + count
                    
                    with processed_count.get_lock():
                        processed_count.value += local_processed
                    with skipped_count.get_lock():
                        skipped_count.value += local_skipped
                    with last_processed_time.get_lock():
                        last_processed_time.value = max(last_processed_time.value, local_last_processed_time)
                    
                    if reached_end:
                        should_stop.value = True
                        break

                if consumed_count.value % 10000 == 0:
                    current_time = time.time()
                    elapsed_time = current_time - start_time
                    processing_rate = consumed_count.value / elapsed_time if elapsed_time > 0 else 0
                    logging.info(f"Consumed: {consumed_count.value}, Processed: {processed_count.value}, Skipped: {skipped_count.value}, Rate: {processing_rate:.2f} messages/second")

            # Process any remaining messages
            if batch:
                pool.apply_async(process_message_batch, (batch, result_queue, end_datetime))

            # Wait for all remaining results
            pool.close()
            pool.join()

            # Process any remaining results in the queue
            while not result_queue.empty():
                local_ip_counter, local_domain_counter, local_processed, local_skipped, _, local_last_processed_time = result_queue.get()
                for ip, count in local_ip_counter.items():
                    ip_counter[ip] = ip_counter.get(ip, 0) + count
                for domain, count in local_domain_counter.items():
                    domain_counter[domain] = domain_counter.get(domain, 0) + count
                with processed_count.get_lock():
                    processed_count.value += local_processed
                with skipped_count.get_lock():
                    skipped_count.value += local_skipped
                with last_processed_time.get_lock():
                    last_processed_time.value = max(last_processed_time.value, local_last_processed_time)

    except KeyboardInterrupt:
        logging.info("Interrupted by user. Closing consumer...")
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        import traceback
        logging.error(traceback.format_exc())
    finally:
        consumer.close()

    end_time = time.time()
    total_time = end_time - start_time
    average_rate = consumed_count.value / total_time if total_time > 0 else 0

    logging.info(f"Total consumed: {consumed_count.value}, Total processed: {processed_count.value}, Total skipped: {skipped_count.value}")
    logging.info(f"Average processing rate: {average_rate:.2f} messages/second")
    logging.info(f"Last processed message time: {timestamp_to_datetime(last_processed_time.value)}")
    
    print("\nTop 10 IP Addresses Denied:")
    for ip, count in Counter(ip_counter).most_common(10):
        print(f"{ip}: {count}")

    print("\nTop 10 Domains Denied:")
    for domain, count in Counter(domain_counter).most_common(10):
        print(f"{domain}: {count}")

    print("\nProcessing Summary:")
    print(f"Total messages consumed: {consumed_count.value}")
    print(f"Total messages processed: {processed_count.value}")
    print(f"Total messages skipped: {skipped_count.value}")
    print(f"Total unique IP addresses: {len(ip_counter)}")
    print(f"Total unique domains: {len(domain_counter)}")
    print(f"Average processing rate: {average_rate:.2f} messages/second")
    print(f"Last processed message time: {timestamp_to_datetime(last_processed_time.value)}")

    # if first_message[0]:
    #     print(f"\nFirst processed message:")
    #     print(f"Time: {first_message[0]}")
    #     print(f"Raw message: {first_message[1]}")
    
    if last_message[0]:
        print(f"\nLast processed message:")
        print(f"Time: {last_message[0]}")
        print(f"Raw message: {last_message[1]}")

if __name__ == "__main__":
    end_datetime = get_end_datetime()
    start_from_beginning = input("Start from the beginning of the topic? (y/n): ").lower() == 'y'

    logging.info(f"Script will run until message datetime exceeds or equals: {end_datetime}")
    logging.info(f"Starting from the beginning: {start_from_beginning}")
    process_logs(end_datetime, start_from_beginning)
