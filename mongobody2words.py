from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import string
from multiprocessing import Process
from multiprocessing import Queue
from multiprocessing import cpu_count

import html2text
from pymongo import MongoClient


def parse_args():
    parser = argparse.ArgumentParser(
        prog='cleanupbody',
        description='Read data from mongo and help to clean it up.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("mongo_url", help="mongodb url")
    parser.add_argument("--mongo_db", "-d", help="mongo database name", default="archive")
    parser.add_argument("--mongo_collection", "-c", help="mongo collection name", default="mails")
    return parser.parse_args()


def process_body(input_body):
    # leave only printable symbols
    text = remove_unprintable(input_body)

    plain = html2text.html2text(text)  # convert bytes to string
    return plain


def remove_unprintable(input_body):
    text = ''.join(filter(lambda x: x in string.printable, input_body))
    return text


def worker(url, db, collection, task_queue, done_queue):
    worker_col = MongoClient(url)[db][collection]
    for item_id in iter(task_queue.get, 'STOP'):
        item = worker_col.find_one({"_id": item_id})
        processed_text = process_body(item["body"])
        result = processed_text.split()
        done_queue.put(result)


def process_records(url, db, collection):
    col = MongoClient(url, connect=False)[db][collection]

    ids = []

    for i in col.find({}, projection={'_id': True}).limit(1000):
        ids.append(i["_id"])

    task_queue = Queue()
    done_queue = Queue()
    # Submit tasks
    for task in ids:
        task_queue.put(task)

    # Start worker processes
    for i in range(cpu_count()):
        Process(target=worker, args=(url, db, collection, task_queue, done_queue)).start()

    # Get and print results
    print('Unordered results:')
    for _ in range(len(ids)):
        for item in done_queue.get():
            print(item)

    # Tell child processes to stop
    for i in range(cpu_count()):
        task_queue.put('STOP')


if __name__ == '__main__':
    args = parse_args()
    process_records(args.mongo_url, args.mongo_db, args.mongo_collection)
