"""
Kafka Configuration Module

This module provides utilities for reading Kafka configuration properties
and creating a Kafka producer with the specified settings.

Functions:
- read_properties(file_path): Read configuration properties from a file
- create_kafka_producer(properties_file): Create and configure a KafkaProducer instance

Dependencies:
- kafka-python
- ssl (Python standard library)
- json (Python standard library)
"""

import ssl
from kafka import KafkaProducer
import json

def read_properties(file_path):
    """
    Read configuration properties from a file.

    Args:
        file_path (str): Path to the properties file.

    Returns:
        dict: A dictionary containing the key-value pairs from the properties file.
    """
    properties = {}
    with open(file_path, 'r') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#'):
                key, value = line.split('=', 1)
                properties[key.strip()] = value.strip()
    return properties

def create_kafka_producer(properties_file):
    """
    Create and configure a KafkaProducer instance based on the provided properties file.

    This function reads the configuration from the specified properties file,
    sets up the necessary security protocols (if applicable), and creates a
    KafkaProducer with the appropriate settings.

    Args:
        properties_file (str): Path to the Kafka properties file.

    Returns:
        KafkaProducer: A configured KafkaProducer instance.

    Note:
        This function supports both SASL_PLAINTEXT and SASL_SSL security protocols.
        For SASL_SSL, it creates a default SSL context with hostname checking disabled
        and certificate verification set to CERT_NONE.
    """
    props = read_properties(properties_file)
    
    producer_config = {
        'bootstrap_servers': props['bootstrap.servers'].split(','),
        'security_protocol': props['security.protocol'],
        'sasl_mechanism': props['sasl.mechanism'],
        'sasl_plain_username': props['sasl.jaas.config'].split('username="')[1].split('"')[0],
        'sasl_plain_password': props['sasl.jaas.config'].split('password="')[1].split('"')[0],
        'value_serializer': lambda v: json.dumps(v).encode('utf-8')
    }
    
    if props['security.protocol'] == 'SASL_SSL':
        context = ssl.create_default_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        
        producer_config.update({
            'ssl_context': context,
        })
    
    # Add producer-specific configs
    producer_config.update({
        'max_request_size': int(props.get('fetch.max.bytes', 1048576)),
        'batch_size': int(props.get('max.partition.fetch.bytes', 1048576)),
        'linger_ms': int(props.get('fetch.max.wait.ms', 500)),
    })
    
    return KafkaProducer(**producer_config)