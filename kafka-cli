#!/usr/bin/env python
# wu.chen 2019.08
# install kafka module for python, cent-os 7.0
# sudo yum -y install epel-release
# sudo yum -y install python-pip
# sudo pip install kafka

import sys
from kafka import KafkaConsumer, TopicPartition
from datetime import datetime
import time
import re
import struct

def usage():
    print("""usage:
    kafka-cli <bootstrap-server>  -- list topics
    kafka-cli <bootstrap-server> <topic> -- describe topic
    kafka-cli <bootstrap-server> <topic> <partition> <offset> <limit: default=1> -- query message from offset
    kafka-cli <bootstrap-server> <topic> <partition> <begin-time> <limit: default=1> -- query message from time
    kafka-cli <bootstrap-server> <topic> <key-value-type> send <key> <value> -- send message

    key-value-type: 0 none, 1 string, 2 int,  key-type * 10 + value-type (11 string key and value, 1 only string value, 20 only int key)
    
examples:
    $>kafka-cli 192.168.20.211:9092 test01               
    topic: test01, partition: 0, offset: [0, 3)
    
    $>kafka-cli 192.168.20.211:9092 test01 0 1 
    2019-08-27 19:13:12.573000 ConsumerRecord(topic='test01', partition=0, offset=1, timestamp=1566904392573, timestamp_type=0, key=None, value=b'hihi', checksum=-1132288920, serialized_key_size=-1, serialized_value_size=4)
    
    $>kafka-cli 192.168.20.211:9092 test01 0 "2019-08-27 18:00:00" 2
    2019-08-27 19:08:36.798000 ConsumerRecord(topic='test01', partition=0, offset=0, timestamp=1566904116798, timestamp_type=0, key=None, value=b'hello', checksum=-367915336, serialized_key_size=-1, serialized_value_size=5)
    2019-08-27 19:13:12.573000 ConsumerRecord(topic='test01', partition=0, offset=1, timestamp=1566904392573, timestamp_type=0, key=None, value=b'hihi', checksum=-1132288920, serialized_key_size=-1, serialized_value_size=4)
    """)

def list_topics(bootstrap_server):
    consumer = KafkaConsumer(bootstrap_servers=[bootstrap_server])
    print("\n".join(consumer.topics()))

def describe_topic(bootstrap_server, topic):
    consumer = KafkaConsumer(bootstrap_servers=[bootstrap_server])
    partitions = consumer.partitions_for_topic(topic)
    if not partitions:
        print("topic: {}, no partition.".format(topic))
        return
    for partition in partitions:
        begin = consumer.beginning_offsets([TopicPartition(topic, partition)])[TopicPartition(topic, partition)]
        end  = consumer.end_offsets([TopicPartition(topic, partition)])[TopicPartition(topic, partition)]
        print("topic: {}, partition: {}, offset: [{}, {})".format(topic, partition,  begin, end))

def query_offset(bootstrap_server, topic, partition, offset, number=1):
    consumer = KafkaConsumer(bootstrap_servers=[bootstrap_server])
    consumer.assign([TopicPartition(topic, int(partition))])
    consumer.seek(TopicPartition(topic, int(partition)), int(offset))
    for _ in range(int(number)):
        msg = next(consumer)
        t = "{}".format(datetime.fromtimestamp(msg.timestamp/1000))
        print(t, msg)

def query_time(bootstrap_server, topic, partition, begin_time, number=1):
    consumer = KafkaConsumer(bootstrap_servers=[bootstrap_server])
    consumer.assign([TopicPartition(topic, partition)])
    begin_search = {}
    begin_search[TopicPartition(topic, partition)] = str_to_timestamp(begin_time)
    tp_offset = consumer.offsets_for_times(begin_search)
    if not tp_offset or not tp_offset[TopicPartition(topic, partition)]:
        print("no record after timestamp [{}] found.".format(begin_time))
        return
    offset = tp_offset[TopicPartition(topic, partition)].offset
    query_offset(bootstrap_server, topic, partition, offset, number)

def int_encode(n):
    return struct.pack("i", n)

def send_message(bootstrap_server, topic, key_type,  key, val_type,  value):
    # print("send message, topic: {}, key: {}, value: {}".format(topic, key, value))
    key_s = None
    val_s = None
    if key_type == 1:
        key_s = str.encode
    if val_type == 1:
        val_s = str.encode
    if key_type == 2:
        key_s = int_encode
        key = int(key)
    if val_type == 2:
        val_s = int_encode
        value = int(value)
    from kafka import KafkaProducer
    producer = KafkaProducer(bootstrap_servers=bootstrap_server, key_serializer=key_s, value_serializer=val_s)
    f = producer.send(topic, value, key)
    print(f.get(1000))
    producer.close()



def str_to_timestamp(str_time, format_type='%Y-%m-%d %H:%M:%S'):
    time_array = time.strptime(str_time, format_type)
    return int(time.mktime(time_array)) * 1000

def is_offset(begin):
    if re.match("^\d+$", begin):
        return True
    return False

def main():
    if len(sys.argv) > 1:
        bootstrap_server = sys.argv[1]
    if len(sys.argv) > 2:
        topic = sys.argv[2]
    if len(sys.argv) > 3:
        partition = int(sys.argv[3])
    if len(sys.argv) > 4:
        begin = sys.argv[4]
    if len(sys.argv) > 5:
        if not "send" == sys.argv[4]:
            number = int(sys.argv[5])
    args_number = len(sys.argv) - 1
    if args_number == 0:
        usage()
    elif args_number == 1:
        list_topics(bootstrap_server)
    elif args_number == 2:
        bootstrap_server = sys.argv[1]
        describe_topic(bootstrap_server, topic)
    elif args_number == 4:
        if is_offset(begin):
            query_offset(bootstrap_server, topic, partition, int(begin))
        else:
            query_time(bootstrap_server, topic, partition, begin)
    elif args_number == 5:
        if "send" == sys.argv[4]:
            # print("send a message")
            send_mode = partition
            key_type = send_mode / 10
            val_type = send_mode % 10
            key, value = None, None
            if key_type == 0:
                value = sys.argv[5]
            elif val_type == 0:
                key = sys.argv[5]
            else:
                print("error: only one argument, key type or value type must has one zero.")
                exit(1)
            send_message(bootstrap_server, topic, key_type, key, val_type, value)
        elif is_offset(begin):
            query_offset(bootstrap_server, topic, partition, int(begin), number)
        else:
            query_time(bootstrap_server, topic, partition, begin, number)
    elif args_number == 6:
        if "send" == sys.argv[4]:
            send_mode = partition
            key = sys.argv[5]
            value = sys.argv[6]
            key_type = send_mode / 10
            val_type = send_mode % 10
            send_message(bootstrap_server, topic, key_type,  key, val_type, value)
    else:
        print("too many arguments.")
        usage()

if __name__ == '__main__':
    main()