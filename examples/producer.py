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
# Example Kafka Producer.
# Reads lines from stdin and sends to Kafka.
#

from confluent_kafka import Producer
import sys

if __name__ == '__main__':

    topic = "k-47586193d451437fa0d8301b13b906dd-a7035b5b-039d-4637-a8a4-1259c86406fc"

    # Producer configuration
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
    conf['sasl.project.id'] = 'fdc60cfe407a4b2a96a498efda55c785'
    conf['sasl.access.key'] = 'G6DNZF3TVBQIRENFOW4L'
    conf['sasl.security.key'] = 'WoIgoldytZ5OWWTE7czQ7V5VNo57qKnFeVcRxP5P'
    conf['sasl.mechanisms'] = 'DMS'
    conf['security.protocol'] = 'SASL_SSL'
    conf['ssl.ca.location'] = '/home/sysops/chen/xzb/ca.crt'
    conf['batch.size'] = 900000000
    conf['buffer.memory'] = 1000000000

    # Create Producer instance
    p = Producer(**kafka_huaweiyun_conf)

    # Optional per-message delivery callback (triggered by poll() or flush())
    # when a message has been successfully delivered or permanently
    # failed delivery (after retries).
    def delivery_callback(err, msg):
        if err:
            sys.stderr.write('%% Message failed delivery: %s\n' % err)
        else:
            sys.stderr.write('%% Message delivered to %s [%d] @ %o\n' %
                             (msg.topic(), msg.partition(), msg.offset()))

    # Read lines from stdin, produce each line to Kafka
    file1 = open('./10M.txt')
    msg02 = file1.readline()
    sys.stderr.write('%% the size of mesg02 is: %d\n' % len(msg02))
    file1.close()
#    sys.stderr.write('%% msg read from file is: %s\n' % msg02)
    #lines=["Hello!1111111111111111111111111111111111111111111111111111111111111111111111111111","python~~~~~~~~~~~~!","world~~2222222222222222222222222~~~~~~~~~~~~!"]
    lines=[msg02,"python~~~~~~~~~~~~!"]
    for line in lines:
        try:
            print line
            # Produce line (without newline)
            p.produce(topic, line.rstrip(), callback=delivery_callback)
    
        except BufferError as e:
            sys.stderr.write('%% Local producer queue is full (%d messages awaiting delivery): try again\n' %
                             len(p))

    # Serve delivery callback queue.
    # NOTE: Since produce() is an asynchronous API this poll() call
    #       will most likely not serve the delivery callback for the
    #       last produce()d message.
    p.poll(0)

    # Wait until all messages have been delivered
    sys.stderr.write('%% Waiting for %d deliveries\n' % len(p))
    p.flush()
