from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import logging
import sys
from multiprocessing import Process
from multiprocessing import Queue
from multiprocessing import cpu_count

from pymongo import MongoClient

from utils import process_body

logger = logging.getLogger("body2words")
logger.setLevel(logging.DEBUG)

ch = logging.StreamHandler(sys.stderr)
ch.setLevel(logging.DEBUG)
logger.addHandler(ch)


def parse_args():
    parser = argparse.ArgumentParser(
        prog='cleanupbody',
        description='Read data from mongo and help to clean it up.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("mongo_url", help="mongodb url")
    parser.add_argument("--mongo_db", "-d", help="mongo database name", default="archive")
    parser.add_argument("--mongo_collection", "-c", help="mongo collection name", default="mails")
    parser.add_argument("--workers", "-w", help="number of workers", default=cpu_count(), type=int)
    return parser.parse_args()


def worker(url, db, collection, task_queue, done_queue):
    worker_col = MongoClient(url)[db][collection]
    for item_id in iter(task_queue.get, 'STOP'):
        item = worker_col.find_one({"_id": item_id})
        words = process_body(item["body"])
        for word in words:
            done_queue.put(word)


def print_worker(done_queue):
    for item in iter(done_queue.get, 'STOP_PRINTING'):
        try:
            print(item)
        except UnicodeEncodeError as ex:  # just ignore UnicodeEncodeError
            logger.warning("UnicodeEncodeError happened on record %s. Ignore. Ex: %s", item, ex)
            pass


def process_records(url, db, collection, num_workers=1):
    col = MongoClient(url)[db][collection]
    cursor = col.find({}, projection={'_id': True})

    task_queue = Queue()
    print_queue = Queue()

    # Print worker
    print_process = Process(target=print_worker, args=(print_queue,))
    print_process.start()

    # Start worker processes
    worker_processes = []
    for i in range(num_workers):
        p = Process(target=worker, args=(url, db, collection, task_queue, print_queue))
        p.start()
        worker_processes.append(p)

    # Submit tasks
    num_ids = 0
    for task in cursor:
        task_queue.put(task["_id"])
        num_ids += 1

    # Tell child processes to stop
    logger.info("stopping workers via 'STOP' in queue")
    for i in range(num_workers):
        task_queue.put('STOP')

    logger.info("joining workers")
    for p in worker_processes:
        p.join()

    logger.info("stopping printer via 'STOP' in queue")
    print_queue.put('STOP_PRINTING')
    logger.info("joining printer")
    print_process.join()


if __name__ == '__main__':
    args = parse_args()
    process_records(args.mongo_url, args.mongo_db, args.mongo_collection, args.workers)
