from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import multiprocessing as mp
import string

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


def get_record(url, db, collection):
    col = MongoClient(url)[db][collection]
    return col.find_one()


def process_body(input_body):
    # leave only printable symbols
    text = remove_unprintable(input_body)

    plain = html2text.html2text(text)  # convert bytes to string
    return plain


def remove_unprintable(input_body):
    text = ''.join(filter(lambda x: x in string.printable, input_body))
    return text


def process_records(url, db, collection):
    col = MongoClient(url)[db][collection]
    for doc in col.find():
        processed_text = process_body(doc["body"])
        for word in processed_text.split():
            print(word)


def print_worker(print_q):
    while True:
        item = print_q.get()
        if item is None:
            break
        print(item)
        print_q.task_done()


def worker(process_que, print_q):
    while True:
        item = process_que.get()
        if item is None:
            break
        processed_text = process_body(item)
        for w in processed_text.split():
            print_q.put(w)
        process_que.task_done()


def process_records(url, db, collection, num_workers=mp.cpu_count()):
    col = MongoClient(url)[db][collection]
    work_queue = mp.JoinableQueue()
    print_queue = mp.JoinableQueue()

    processes = []
    # print worker
    print_process = mp.Process(target=print_worker, args=(print_queue,))
    print_process.start()

    # data workers
    for i in range(num_workers):
        p = mp.Process(target=worker, args=(work_queue, print_queue))
        p.start()
        processes.append(p)

    for doc in col.find():
        work_queue.put(doc["body"])

    # block until all tasks are done
    work_queue.join()
    print_queue.join()

    # stop workers
    print_queue.put(None)
    print_process.join()

    for i in range(num_workers):
        work_queue.put(None)
    for p in processes:
        p.join()


if __name__ == '__main__':
    args = parse_args()
    process_records(args.mongo_url, args.mongo_db, args.mongo_collection)
