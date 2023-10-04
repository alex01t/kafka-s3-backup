#!/usr/bin/env python3
from kafka import KafkaConsumer, KafkaProducer, TopicPartition
import os, sys, datetime, time, base64, json, boto3
import functools

print = functools.partial(print, flush=True)
#
ACTION = os.getenv('ACTION')
DUMP_BROKER = os.getenv('DUMP_BROKER')
DUMP_TOPIC = os.getenv('DUMP_TOPIC')
DUMP_PARTITION = int(os.getenv('DUMP_PARTITION', 0))
RESTORE_MODE = os.getenv('RESTORE_MODE')
RESTORE_BROKER = os.getenv('RESTORE_BROKER')
RESTORE_TOPIC = os.getenv('RESTORE_TOPIC')
RESTORE_PARTITION = int(os.getenv('RESTORE_PARTITION', 0))
DUMP_FLUSH_SIZE = int(os.getenv('DUMP_FLUSH_SIZE', 100_000))
DUMP_FLUSH_PERIOD_MS = int(os.getenv('DUMP_FLUSH_PERIOD_MS', 60_000))
DUMP_CONSUMER_TIMEOUT_MS = int(os.getenv('DUMP_CONSUMER_TIMEOUT_MS', 30_000))  # consumer breaks if there is no messages this long
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
S3_REGION = os.getenv('S3_REGION')
S3_BUCKET = os.getenv('S3_BUCKET')
S3_PATH = os.getenv('S3_PATH')
#
LAST_SAVED_OFFSET_S3KEY = f'{S3_PATH}/{DUMP_TOPIC}/partition={DUMP_PARTITION}-last-saved-offset'
s3 = boto3.client('s3', region_name=S3_REGION, aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)


def batch2json(batch):
    return json.dumps([base64.b64encode(m.value).decode('ascii') for m in batch], indent=2)


def json2batch(s):
    return [base64.b64decode(x.encode("ascii")) for x in json.loads(s)]


def get_topic_offsets(b, t, p):
    c = KafkaConsumer(bootstrap_servers=b, enable_auto_commit=False)
    tp = TopicPartition(t, p)
    c.assign([tp])
    start_offset = c.beginning_offsets([tp]).get(tp)
    end_offset = c.end_offsets([tp]).get(tp)
    c.close()
    print(f'topic {t} is {start_offset}..{end_offset}')
    return start_offset, end_offset


def restore(dry_run=True):
    keys = []
    kwargs = {'Bucket': S3_BUCKET, 'Prefix': f'{S3_PATH}/{DUMP_TOPIC}/partition={DUMP_PARTITION}/'}
    while True:
        resp = s3.list_objects_v2(**kwargs)
        if resp['KeyCount'] == 0 or 'Contents' not in resp:
            print(f'error: nothing found under {kwargs}. response is {resp}')
            sys.exit(4)
        for obj in resp['Contents']:
            keys.append(obj['Key'])
        try:
            kwargs['ContinuationToken'] = resp['NextContinuationToken']
        except KeyError:
            break
    ranges = []
    for k in keys:
        x = k.split('/')[-1].split('.')[0].split('-')
        d = {
            'key': k,
            'from': int(x[0]),
            'to': int(x[1]),
        }
        ranges.append(d)
    ranges.sort(key=lambda d: d['from'])
    #
    print('check if backup chunks are all sequential')
    prev = {}
    for cur in ranges:
        if not prev:
            pass  # skip 1st
        else:
            assert prev['to'] + 1 == cur['from'], f'keys {prev} and {cur} are not connected'
        prev = cur
    #
    print('check last_saved_offset')
    x = s3.get_object(Bucket=f'{S3_BUCKET}', Key=LAST_SAVED_OFFSET_S3KEY)
    last_saved_offset = int(x.get('Body').read().decode('ascii'))
    assert ranges[-1]['to'] == last_saved_offset, f'last range {ranges[-1]} doesnt end on last_saved_offset = {last_saved_offset}'
    #
    backup_start_offset = ranges[0]["from"]
    backup_end_offset = ranges[-1]["to"]
    backup_total_messages = backup_end_offset - backup_start_offset + 1
    print(f'got {len(ranges)} chunks with offsets from {backup_start_offset} to {backup_end_offset}, total {backup_total_messages} messages')

    producer = KafkaProducer(bootstrap_servers=[RESTORE_BROKER], retries=0, acks=1)

    restore_topic_start_offset, restore_topic_end_offset = get_topic_offsets(RESTORE_BROKER, RESTORE_TOPIC, RESTORE_PARTITION)

    if restore_topic_end_offset > 0:
        print(f'restore topic {RESTORE_TOPIC} is non-empty')
        if RESTORE_MODE == 'full':
            print(f'error: restore topic {RESTORE_TOPIC} is non-empty while RESTORE_MODE = full')
            sys.exit(3)
    else:
        print(f'restore topic {RESTORE_TOPIC} is empty')
        if backup_start_offset > 0:
            print(f'filling {RESTORE_TOPIC} with empty messages up until backup_start_offset = {backup_start_offset}')
            future = None
            for i in range(0, backup_start_offset):
                future = producer.send(RESTORE_TOPIC, partition=RESTORE_PARTITION, value=b'')
            try:
                record_metadata = future.get(timeout=10)
                offset = record_metadata.offset
                print(f'last empty message offset was {offset}')
            except Exception as e:
                print(f'error: failed to send empty messages to {RESTORE_TOPIC}: {e}')
                sys.exit(8)
            restore_topic_start_offset, restore_topic_end_offset = get_topic_offsets(RESTORE_BROKER, RESTORE_TOPIC, RESTORE_PARTITION)

    # find chunk that starts as restore_topic_end_offset and restore from it and further
    restore_list = []
    for (i, r) in enumerate(ranges):
        if r['from'] < restore_topic_end_offset:
            print(f'{r["key"]} is already in, next')
        elif r['from'] == restore_topic_end_offset:
            print(f'{r["key"]} is the next batch to restore')
            restore_list = ranges[i:]
            break
        elif r['from'] > restore_topic_end_offset:
            print(f'{r["key"]} cannot be restored on top of restore_topic_end_offset = {restore_topic_end_offset}')
            sys.exit(5)
        else:
            print('not possible')
            sys.exit(6)

    if not restore_list:
        print(f'found no batches to restore on top of restore_topic_end_offset = {restore_topic_end_offset}')
        sys.exit(0)

    for r in restore_list:
        t1 = datetime.datetime.utcnow().timestamp()
        print(f'getting batch from s3 - {r["key"]}')
        x = s3.get_object(Bucket=f'{S3_BUCKET}', Key=r['key'])
        j = x.get('Body').read()
        futures = []
        t2 = datetime.datetime.utcnow().timestamp()
        print(f'batch received in {(t2 - t1):.3} sec')
        for m in json2batch(j):
            f = producer.send(RESTORE_TOPIC, partition=RESTORE_PARTITION, value=m)
            futures.append(f)
        offsets = []
        try:
            for f in futures:
                record_metadata = f.get(timeout=10)
                offset = record_metadata.offset
                offsets.append(offset)
        except Exception as e:
            print(f'error: fail to send the batch {r["key"]} to {RESTORE_TOPIC}: {e}')
            sys.exit(9)
        t3 = datetime.datetime.utcnow().timestamp()
        print(f'sent {r["to"] - r["from"]} messages in {(t3 - t2):.3} sec, offsets {offsets[0]}..{offsets[-1]}')

    get_topic_offsets(RESTORE_BROKER, RESTORE_TOPIC, RESTORE_PARTITION)

    print('closing producer')
    producer.close()


def dump():
    while True:
        try:
            print(f'reading last_saved_offset from s3://{S3_BUCKET}/{LAST_SAVED_OFFSET_S3KEY}')
            x = s3.get_object(Bucket=f'{S3_BUCKET}', Key=LAST_SAVED_OFFSET_S3KEY)
            last_saved_offset = int(x.get('Body').read().decode('ascii'))
            print(f'last_saved_offset = {last_saved_offset}')
        except s3.exceptions.NoSuchKey:
            print(f'NoSuchKey - assuming last_saved_offset = 0')
            last_saved_offset = 0
        except Exception as e:
            print(f'failed to read last_saved_offset from s3: {e}')
            sys.exit(1)
        c = KafkaConsumer(
            bootstrap_servers=DUMP_BROKER,
            enable_auto_commit=False, consumer_timeout_ms=DUMP_CONSUMER_TIMEOUT_MS,
        )
        topic_partition = TopicPartition(DUMP_TOPIC, DUMP_PARTITION)
        c.assign([topic_partition])
        start_offset = c.beginning_offsets([topic_partition]).get(topic_partition)
        end_offset = c.end_offsets([topic_partition]).get(topic_partition)
        print(f'topic is {start_offset}..{end_offset}, last_saved_offset = {last_saved_offset}, lag = {(end_offset - 1) - last_saved_offset}')
        if last_saved_offset != 0 and start_offset > last_saved_offset + 1:
            print('last_saved_offset is too behind, the topic rotated. need to start backup to empty s3 path')
            sys.exit(2)
        if end_offset < last_saved_offset:
            print('last_saved_offset is grater than end_offset of the topic somehow, new backup')
        i = 0
        batch = []
        read_offset = max(last_saved_offset + 1, start_offset)
        print(f'consuming from read_offset = {read_offset}')
        c.seek(offset=read_offset, partition=topic_partition)
        t1 = datetime.datetime.utcnow().timestamp()
        for m in c:
            batch.append(m)
            i += 1
            if i >= DUMP_FLUSH_SIZE:
                print('break by DUMP_FLUSH_SIZE')
                break
            t2 = datetime.datetime.utcnow().timestamp()
            if t2 - t1 > (DUMP_FLUSH_PERIOD_MS / 1000.0):
                print('break by DUMP_FLUSH_PERIOD_MS')
                break
        else:
            print('break by DUMP_CONSUMER_TIMEOUT_MS')
        if batch:
            last_message = batch[-1]
            print(f'got the batch {read_offset}..{last_message.offset} of {len(batch)} messages')
            batch_s3key = f'{S3_PATH}/{DUMP_TOPIC}/partition={DUMP_PARTITION}/{batch[0].offset}-{batch[-1].offset}.json'
            s = batch2json(batch)
            s3.put_object(Body=s.encode('ascii'), Bucket=S3_BUCKET, Key=batch_s3key)
            print(f'batch saved as s3://{S3_BUCKET}/{batch_s3key}')
            print(f'updating last_saved_offset to {last_message.offset}')
            s3.put_object(Body=str(last_message.offset).encode('ascii'), Bucket=S3_BUCKET, Key=LAST_SAVED_OFFSET_S3KEY)
            print(f'offset saved as s3://{S3_BUCKET}/{LAST_SAVED_OFFSET_S3KEY}')
        else:
            print(f'got 0 messages, nothing to do')
        print()


def main():
    if ACTION == 'dump':
        dump()
    elif ACTION == 'restore':
        while True:
            restore()
            print('sleep 600')
            time.sleep(600)
    elif ACTION == '':
        print(f'error: ACTION is empty')
        sys.exit(1)
    else:
        print(f'error: ACTION = {ACTION} is not implemented')
        sys.exit(1)


main()
