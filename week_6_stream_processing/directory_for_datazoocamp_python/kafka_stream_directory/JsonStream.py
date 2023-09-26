#!/usr/bin/env python


import csv
import json
from typing import List, Dict
import sys
from random import choice
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Producer
from confluent_kafka import Consumer

from week_6_stream_processing.directory_for_datazoocamp_python.producer_client_3.ride import Ride


def read_records(resource_path: str) -> List[Ride]:
    records = []
    with open(resource_path, 'r') as f:
        reader = csv.reader(f)
        header = next(reader)  # skip the header row
        for row in reader:
            records.append(Ride(arr=row))
    return records


if __name__ == '__main__':
    # Parse the command line.
    parser = ArgumentParser()
    parser.add_argument('config_file', type=FileType('r'))
    args = parser.parse_args()

    INPUT_DATA_PATH = "../../data/yellow_tripdata/yellow_tridata_head.csv"

    # Parse the configuration.
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser['default'])


    # Create Producer instance
    producer = Producer(config)


    # Optional per-message delivery callback (triggered by poll() or flush())
    # when a message has been successfully delivered or permanently
    # failed delivery (after retries).
    def delivery_callback(err, msg):
        if err:
            print('ERROR: Message failed delivery: {}'.format(err))
        else:
            print("Produced event to topic {topic}: key = {key:12} value = {value:12}".format(
                topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))

    # Produce data by selecting random values from these lists.
    topic_purchase = "purchases"
    FHV_TAXI_TOPIC = "fhv_taxi_rides"
    user_ids = ['eabara', 'jsmith', 'sgarcia', 'jbernard', 'htanaka', 'awalther']
    products = ['book', 'alarm clock', 't-shirts', 'gift card', 'batteries']
    rides = read_records(INPUT_DATA_PATH)

    count2 = 0
    for ride in rides:
        producer.produce(topic=FHV_TAXI_TOPIC,value=ride.__repr__(),key=str(ride.pu_location_id),callback=delivery_callback)
        count2 += 1

    # Block until the messages are sent.
    producer.poll(10000)
    producer.flush()