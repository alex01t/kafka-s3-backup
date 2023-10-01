"""
    this is helm chart to backup and restore kafka topic partition keeping exactly the same offsets

    k run cli --image=debian:11 --restart=Never -- sleep inf
    apt update; apt install -y python3-pip && pip3 install ipython kafka-python boto3

"""

from kafka import KafkaConsumer, TopicPartition

def get_offsets(b, t, p):
    c = KafkaConsumer(bootstrap_servers=b, enable_auto_commit=False)
    tp = TopicPartition(t, p)
    c.assign([tp])
    start_offset = c.beginning_offsets([tp]).get(tp)
    end_offset = c.end_offsets([tp]).get(tp)
    print(f'topic {t} is {start_offset}..{end_offset}')

def get_one(b, t, p, offset):
    c = KafkaConsumer(bootstrap_servers=b, enable_auto_commit=False, consumer_timeout_ms=100,)
    tp = TopicPartition(t, p)
    c.assign([tp])
    c.seek(offset=(offset), partition=tp)
    for m in c:
        print(m)
        break

def get_last(b, t, p):
    c = KafkaConsumer(bootstrap_servers=b, enable_auto_commit=False, consumer_timeout_ms=100,)
    tp = TopicPartition(t, p)
    c.assign([tp])
    x = c.position(tp)
    c.seek(offset=(x-1), partition=tp)
    for m in c:
        print(f"message = {m}")
        x = c.position(tp)
        print(f"new position = {x}")

def put_some():
    from kafka import KafkaProducer
    producer = KafkaProducer(bootstrap_servers=['my-cluster-1-kafka-bootstrap:9092'])
    for i in range(1000000):
        b = f'{i} - raw_bytes_raw_bytes_raw_bytes_raw_bytes_raw_bytesraw_bytes_raw_bytes'.encode('ascii')
        f = producer.send('events-1', b)
        if i % 100000 == 0:
            try:
                m = f.get(timeout=10)
                print(m)
            except Exception as e:
                print(e)
                break

def compare_partitions(b1, b2, t1, t2, p1, p2, from_offset):
    from kafka import KafkaConsumer, TopicPartition
    c1 = KafkaConsumer(bootstrap_servers=b1, enable_auto_commit=False, consumer_timeout_ms=100,)
    c2 = KafkaConsumer(bootstrap_servers=b2, enable_auto_commit=False, consumer_timeout_ms=100,)
    tp1 = TopicPartition(t1, p1)
    tp2 = TopicPartition(t2, p2)
    c1.assign([tp1])
    c2.assign([tp2])
    c1.seek(offset=(from_offset), partition=tp1)
    c2.seek(offset=(from_offset), partition=tp2)
    get_offsets(b1, t1, p1)
    get_offsets(b2, t2, p2)
    diff = []
    while True:
        m1, m2 = (c1.next_v2(), c2.next_v2())
        if m1.offset % 100000 == 0:
            print(f'{m1.offset}')
        if m1.value != m2.value:
            diff.append((m1,m2))
            print(f'{m1.offset} - {m1.value} vs. {m2.value}')