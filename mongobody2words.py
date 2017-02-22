from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import logging
import string
import sys
import unicodedata
from multiprocessing import Process
from multiprocessing import Queue
from multiprocessing import cpu_count

import html2text
from pymongo import MongoClient

logger = logging.getLogger("body2words")
logger.setLevel(logging.DEBUG)

ch = logging.StreamHandler(sys.stderr)
ch.setLevel(logging.DEBUG)
logger.addHandler(ch)

# Prepare translation table to clean some non-important symbols.
# See http://www.unicode.org/reports/tr44/#General_Category_Values for unicode categories
exclude_unicode_set = {
    'C',  # Other:	Cc | Cf | Cs | Co | Cn
    'S',  # Symbol:	Sm | Sc | Sk | So
    'Zl',  # Line_Separator:	U+2028 LINE SEPARATOR only
    'Zp',  # Paragraph_Separator:	U+2029 PARAGRAPH SEPARATOR only
    'P'  # Punctuation:	Pc | Pd | Ps | Pe | Pi | Pf | Po
}

include_symbols = '@'  # keep this symbols
all_chars = set(chr(i) for i in range(sys.maxunicode))
exclude_chars = set(c for c in all_chars if unicodedata.category(c) in exclude_unicode_set and c not in include_symbols)
translate_table = {ord(character): None for character in exclude_chars}


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


def process_body(body):
    # lower
    text = body.lower()

    # # remove punctuation
    # table = str.maketrans('', '', string.punctuation)
    # text = text.translate(table)

    # leave only printable symbols
    # text = remove_unprintable(text)

    # clean some symbols. for example control
    text = text.translate(translate_table)

    plain = html2text.html2text(text)
    return plain


def remove_unprintable(input_body):
    text = ''.join(filter(lambda x: x in string.printable, input_body))
    return text


def worker(url, db, collection, task_queue, done_queue):
    worker_col = MongoClient(url)[db][collection]
    for item_id in iter(task_queue.get, 'STOP'):
        item = worker_col.find_one({"_id": item_id})
        processed_text = process_body(item["body"])
        for word in processed_text.split():
            done_queue.put(word)


def print_worker(done_queue):
    for item in iter(done_queue.get, 'STOP_PRINTING'):
        try:
            print(item)
        except UnicodeEncodeError as ex:  # just ignore UnicodeEncodeError
            logger.warning("UnicodeEncodeError happened on record %s. Ignore. Ex: %s", item, ex)
            pass


def process_records(url, db, collection):
    col = MongoClient(url)[db][collection]
    cursor = col.find({}, projection={'_id': True})

    task_queue = Queue()
    print_queue = Queue()

    # Print worker
    print_process = Process(target=print_worker, args=(print_queue,))
    print_process.start()

    # Start worker processes
    worker_processes = []
    for i in range(cpu_count()):
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
    for i in range(cpu_count()):
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
    process_records(args.mongo_url, args.mongo_db, args.mongo_collection)
