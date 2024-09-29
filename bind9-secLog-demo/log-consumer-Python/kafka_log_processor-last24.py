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

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

try:
    import lz4
except ImportError:
    logging.warning("lz4 library not found. If your Kafka messages are compressed with lz4, please install it using 'pip install lz4'")

def load_properties(filename):
    """
    Load properties from a file and return them as a dictionary.

    Args:
        filename (str): The name of the file to load properties from.

    Returns:
        dict: A dictionary containing the loaded properties.

    Raises:
        FileNotFoundError: If the file specified by `filename` does not exist.

    Description:
        This function reads a file containing properties in the format "key=value"
        and returns them as a dictionary. Comments (lines starting with '#') are
        ignored. The function opens the file, reads each line, strips leading
        and trailing whitespace, and splits the line into a key-value pair using
        the '=' delimiter. The key and value are then trimmed of any leading or
        trailing whitespace before being added to the dictionary.

        If the file specified by `filename` does not exist, a `FileNotFoundError`
        is raised.
    """
    properties = {}
    with open(filename, 'r') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#'):
                key, value = line.split('=', 1)
                properties[key.strip()] = value.strip()
    return properties

def extract_ip_and_domain(log_entry):
    """
    Extracts IP address and domain from a log entry.

    Args:
        log_entry (str): The log entry to extract IP and domain from.

    Returns:
        tuple: A tuple containing the extracted IP address and domain. 
               If no IP or domain is found, the corresponding value will be None.
    """
    ip_pattern = r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}'
    domain_pattern = r'\((.*?)\)'
    
    ip_match = re.search(ip_pattern, log_entry)
    domain_match = re.search(domain_pattern, log_entry)
    
    ip = ip_match.group(0) if ip_match else None
    domain = domain_match.group(1) if domain_match else None
    
    return ip, domain

def create_kafka_consumer(properties_file, start_from_beginning=False):
    """
    Creates a Kafka consumer based on the provided properties file.

    Args:
        properties_file (str): The path to the properties file containing Kafka configuration.
        start_from_beginning (bool): Whether to start consuming from the beginning of the topic. Defaults to False.

    Returns:
        Consumer: A Kafka consumer instance configured with the provided properties.

    Notes:
        This function loads properties from the specified file, constructs a consumer configuration, and returns a Kafka consumer instance.
        If the security protocol is set to SSL or SASL_SSL, SSL verification is skipped by default.
    """
    props = load_properties(properties_file)
    
    consumer_config = {
        'bootstrap.servers': props['bootstrap.servers'],
        'group.id': f'log-processor-{uuid.uuid4()}',
        'auto.offset.reset': 'earliest' if start_from_beginning else 'latest',
        'enable.auto.commit': 'true',
        'security.protocol': props['security.protocol'],
        'sasl.mechanisms': props['sasl.mechanism'],
        'sasl.username': props['sasl.jaas.config'].split('username="')[1].split('"')[0],
        'sasl.password': props['sasl.jaas.config'].split('password="')[1].split('"')[0],
    }
    
    if props['security.protocol'] in ['SSL', 'SASL_SSL']:
        # ข้าม SSL verification โดยอัตโนมัติ
        context = ssl.create_default_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        consumer_config['ssl.ca.location'] = None
        consumer_config['enable.ssl.certificate.verification'] = 'false'
        logging.warning("SSL verification is automatically skipped. This is not recommended for production use.")
    
    return Consumer(consumer_config)

def process_message(raw_message):
    try:
        decoded_message = raw_message.decode('utf-8')
        json_message = json.loads(decoded_message)
        if json_message.get('file_name') == 'security.log':
            return json_message, decoded_message
    except json.JSONDecodeError:
        # Silently ignore JSON decoding errors
        pass
    except UnicodeDecodeError:
        # Silently ignore Unicode decoding errors
        pass
    return None, None

def timestamp_to_datetime(timestamp):
    dt_utc = datetime.datetime.fromtimestamp(timestamp, datetime.UTC)
    local_tz = pytz.timezone('Asia/Bangkok')  # Adjust this to your local timezone
    dt_local = dt_utc.astimezone(local_tz)
    return dt_local

def get_end_datetime():
    local_tz = pytz.timezone('Asia/Bangkok')  # Adjust this to your local timezone
    return datetime.datetime.now(local_tz)

def get_start_datetime(end_datetime):
    return end_datetime - datetime.timedelta(hours=24)

def process_logs(end_datetime):
    consumer = create_kafka_consumer('k0100-client.properties', start_from_beginning=True)
    
    start_datetime = get_start_datetime(end_datetime)

    ip_counter = Counter()
    domain_counter = Counter()
    processed_count = 0
    skipped_count = 0
    first_message = None
    last_message = None
    start_time = time.time()
    total_messages = 0

    try:
        consumer.subscribe(['logCentral'])
        
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logging.info('Reached end of partition')
                else:
                    logging.error(f"Consumer error: {msg.error()}")
                continue
            
            total_messages += 1
            json_message, raw_message = process_message(msg.value())
            if json_message:
                timestamp = json_message.get('timestamp')
                message_datetime = timestamp_to_datetime(timestamp)
                
                if message_datetime > end_datetime:
                    logging.info(f"Reached message with datetime greater than {end_datetime}. Stopping.")
                    break

                if message_datetime < start_datetime:
                    skipped_count += 1
                    continue

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
            else:
                skipped_count += 1

    except KeyboardInterrupt:
        logging.info("Interrupted by user. Closing consumer...")
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
    finally:
        consumer.close()

    end_time = time.time()
    total_time = end_time - start_time
    average_processing_rate = processed_count / total_time if total_time > 0 else 0
    average_consumption_rate = total_messages / total_time if total_time > 0 else 0

    logging.info(f"Total processed: {processed_count}, Total skipped: {skipped_count}, "
                 f"Average processing rate: {average_processing_rate:.2f} messages/second, "
                 f"Average consumption rate: {average_consumption_rate:.2f} messages/second")
    
    print("\nTop 10 IP Addresses Denied:")
    for ip, count in ip_counter.most_common(10):
        print(f"{ip}: {count}")

    print("\nTop 10 Domains Denied:")
    for domain, count in domain_counter.most_common(10):
        print(f"{domain}: {count}")

    print("\nProcessing Summary:")
    print(f"Total messages consumed: {total_messages}")
    print(f"Total messages processed: {processed_count}")
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
    end_datetime = get_end_datetime()
    start_datetime = get_start_datetime(end_datetime)
    logging.info(f"Script will process messages from {start_datetime} to {end_datetime}")
    logging.info("Starting from the beginning of the topic")
    process_logs(end_datetime)