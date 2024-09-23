import ssl
from kafka import KafkaConsumer, TopicPartition
import json
from collections import Counter
import re
import logging
import datetime
import pytz
import uuid

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

def extract_ip_and_domain(log_entry):
    ip_pattern = r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}'
    domain_pattern = r'\((.*?)\)'
    
    ip_match = re.search(ip_pattern, log_entry)
    domain_match = re.search(domain_pattern, log_entry)
    
    ip = ip_match.group(0) if ip_match else None
    domain = domain_match.group(1) if domain_match else None
    
    return ip, domain

def create_kafka_consumer(properties_file, start_from_beginning=False):
    props = load_properties(properties_file)
    
    consumer_config = {
        'bootstrap_servers': props['bootstrap.servers'].split(','),
        'auto_offset_reset': 'earliest' if start_from_beginning else 'latest',
        'enable_auto_commit': True,
        'group_id': f'log-processor-{uuid.uuid4()}',  # Generate a unique group ID
        'value_deserializer': lambda x: x,  # Return raw bytes
        'security_protocol': props['security.protocol'],
        'sasl_mechanism': props['sasl.mechanism'],
        'sasl_plain_username': props['sasl.jaas.config'].split('username="')[1].split('"')[0],
        'sasl_plain_password': props['sasl.jaas.config'].split('password="')[1].split('"')[0],
    }
    
    if props['security.protocol'] == 'SASL_SSL':
        context = ssl.create_default_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        
        consumer_config.update({
            'ssl_context': context,
        })
    
    # Add consumer-specific configs
    int_props = ['fetch_max_bytes', 'max_partition_fetch_bytes', 'max_poll_records', 
                 'fetch_min_bytes', 'fetch_max_wait_ms', 'max_poll_interval_ms']
    
    for prop in int_props:
        kafka_prop = prop.replace('_', '.')
        if kafka_prop in props:
            consumer_config[prop] = int(props[kafka_prop])
    
    return KafkaConsumer(**consumer_config)

def process_message(raw_message):
    try:
        decoded_message = raw_message.decode('utf-8')
        json_message = json.loads(decoded_message)
        if json_message.get('file_name') == 'security.log':
            return json_message, decoded_message
    except json.JSONDecodeError:
        logging.error(f"Failed to decode JSON: {decoded_message}")
    except UnicodeDecodeError:
        logging.error(f"Failed to decode message: {raw_message}")
    return None, None

def timestamp_to_datetime(timestamp):
    dt_utc = datetime.datetime.utcfromtimestamp(timestamp)
    local_tz = pytz.timezone('Asia/Bangkok')  # Adjust this to your local timezone
    dt_local = dt_utc.replace(tzinfo=pytz.utc).astimezone(local_tz)
    return dt_local

def get_end_datetime():
    while True:
        choice = input("Choose end time option:\n1. Current date and time\n2. Specify date and time\nEnter your choice (1 or 2): ")
        if choice == '1':
            local_tz = pytz.timezone('Asia/Bangkok')  # ปรับให้ตรงกับเขตเวลาของคุณ
            return datetime.datetime.now(local_tz)
        elif choice == '2':
            while True:
                try:
                    end_datetime_str = input("Enter the end date and time (YYYY-MM-DD HH:MM:SS): ")
                    end_datetime = datetime.datetime.strptime(end_datetime_str, "%Y-%m-%d %H:%M:%S")
                    local_tz = pytz.timezone('Asia/Bangkok')  # ปรับให้ตรงกับเขตเวลาของคุณ
                    return local_tz.localize(end_datetime)
                except ValueError:
                    print("Invalid date and time format. Please use YYYY-MM-DD HH:MM:SS.")
        else:
            print("Invalid choice. Please enter 1 or 2.")

def process_logs(end_datetime, start_from_beginning):
    consumer = create_kafka_consumer('k0100-client.properties', start_from_beginning)

    ip_counter = Counter()
    domain_counter = Counter()
    processed_count = 0
    skipped_count = 0
    first_message = None
    last_message = None

    try:
        # Check if the topic exists
        topics = consumer.topics()
        if 'logCentral' not in topics:
            logging.error("Topic 'logCentral' does not exist.")
            return

        # Get partitions for the topic
        partitions = consumer.partitions_for_topic('logCentral')
        if not partitions:
            logging.error("No partitions found for topic 'logCentral'.")
            return

        # Assign all partitions
        topic_partitions = [TopicPartition('logCentral', p) for p in partitions]
        consumer.assign(topic_partitions)

        # Seek to beginning if requested
        if start_from_beginning:
            consumer.seek_to_beginning(*topic_partitions)
        
        # Get beginning and current offsets
        beginning_offsets = consumer.beginning_offsets(topic_partitions)
        current_offsets = {tp: consumer.position(tp) for tp in topic_partitions}
        
        logging.info(f"Starting offsets: {beginning_offsets}")
        logging.info(f"Current offsets: {current_offsets}")

        for message in consumer:
            json_message, raw_message = process_message(message.value)
            if json_message:
                timestamp = json_message.get('timestamp')
                message_datetime = timestamp_to_datetime(timestamp)
                
                if message_datetime > end_datetime:
                    logging.info(f"Reached message with datetime greater than {end_datetime}. Stopping.")
                    break

                processed_count += 1
                if first_message is None:
                    first_message = (message_datetime, raw_message)
                last_message = (message_datetime, raw_message)

                log_entry = json_message.get('content', '')
                if 'denied' in log_entry:
                    ip, domain = extract_ip_and_domain(log_entry)
                    if ip:
                        ip_counter[ip] += 1
                    if domain:
                        domain_counter[domain] += 1
                
                if processed_count % 1000 == 0:
                    logging.info(f"Processed: {processed_count}, Current message time: {message_datetime}")
            else:
                skipped_count += 1
            
            if (processed_count + skipped_count) % 1000 == 0:
                logging.info(f"Processed: {processed_count}, Skipped: {skipped_count}")

    except KeyboardInterrupt:
        logging.info("Interrupted by user. Closing consumer...")
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
    finally:
        consumer.close()

    logging.info(f"Total processed: {processed_count}, Total skipped: {skipped_count}")
    
    print("\nIP Addresses Denied:")
    for ip, count in ip_counter.most_common():
        print(f"{ip}: {count}")

    print("\nDomains Denied:")
    for domain, count in domain_counter.most_common():
        print(f"{domain}: {count}")

    print("\nProcessing Summary:")
    print(f"Total messages processed: {processed_count}")

    if first_message:
        print(f"\nFirst processed message:")
        print(f"Time: {first_message[0]}")
        print(f"Raw message: {first_message[1]}")
    
    if last_message:
        print(f"\nLast processed message:")
        print(f"Time: {last_message[0]}")
        print(f"Raw message: {last_message[1]}")

if __name__ == "__main__":
    end_datetime = get_end_datetime()
    start_from_beginning = input("Start from the beginning of the topic? (y/n): ").lower() == 'y'

    logging.info(f"Script will run until message datetime exceeds: {end_datetime}")
    logging.info(f"Starting from the beginning: {start_from_beginning}")
    process_logs(end_datetime, start_from_beginning)