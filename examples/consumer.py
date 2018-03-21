#!/usr/bin/env python
#
# Copyright 2016 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

#
# Example high-level Kafka 0.9 balanced Consumer
#
from confluent_kafka import Consumer, KafkaException, KafkaError
import sys
import getopt
import json
from pprint import pformat


def stats_cb(stats_json_str):
    stats_json = json.loads(stats_json_str)
    print('\nKAFKA Stats: {}\n'.format(pformat(stats_json)))


def print_usage_and_exit(program_name):
    sys.stderr.write('Usage: %s [options..] <bootstrap-brokers> <group> <topic1> <topic2> ..\n' % program_name)
    options = '''
 Options:
  -T <intvl>   Enable client statistics at specified interval (ms)
'''
    sys.stderr.write(options)
    sys.exit(1)


if __name__ == '__main__':
    group = "g-f0cb4a31-fbf9-4409-8c37-225f26928d67"
    topics = ["k-47586193d451437fa0d8301b13b906dd-a7035b5b-039d-4637-a8a4-1259c86406fc", ]
    # Consumer configuration
    kafka_huaweiyun_conf = {
        'bootstrap.servers': 'dms-kafka.cn-north-1.myhuaweicloud.com',
        'sasl.project.id': '47586193d451437fa0d8301b13b906dd',
        'sasl.access.key': '39S4TYVNPRSJHOUCHJTK',
        'sasl.security.key': 'GIyQzOw180xThZDapMbgnvcZq5RQj7DoyvMsHzz8',
        'sasl.mechanisms': 'DMS',
        'security.protocol': 'SASL_SSL',
        'ssl.ca.location': 'examples/ca.crt',
    }
    conf = {}
    conf["group.id"] = group
    conf["session.timeout.ms"] = 6000
    conf["default.topic.config"] = {"auto.offset.reset": "earliest"}

    conf['sasl.project.id'] = 'fdc60cfe407a4b2a96a498efda55c785'
    conf['sasl.access.key'] = 'G6DNZF3TVBQIRENFOW4L'
    conf['sasl.security.key'] = 'WoIgoldytZ5OWWTE7czQ7V5VNo57qKnFeVcRxP5P'
    conf["sasl.mechanisms"] = "DMS"
    conf["security.protocol"] = "SASL_SSL"
    conf['ssl.ca.location'] = '/home/sysops/chen/xzb/ca.crt'
    
    conf.update(kafka_huaweiyun_conf)

    # Create Consumer instance
    c = Consumer(**conf)

    def print_assignment(consumer, partitions):
        print('Assignment:', partitions)

    # Subscribe to topics
    c.subscribe(topics, on_assign=print_assignment)

    # Read messages from Kafka, print to stdout
    try:
        while True:
            msg = c.poll(timeout=1.0)
            if msg is None:
                print "no message~"
                continue
            if msg.error():
                # Error or event
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    # Error
                    raise KafkaException(msg.error())
            else:
                # Proper message
                sys.stderr.write('%% %s [%d] at offset %d with key %s:\n' %
                                 (msg.topic(), msg.partition(), msg.offset(),
                                  str(msg.key())))
                print(msg.value())

    except KeyboardInterrupt:
        sys.stderr.write('%% Aborted by user\n')

    # Close down consumer to commit final offsets.
    c.close()

