from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import string
from multiprocessing import Pool
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


def worker(item):
    processed_text = process_body(item["body"])
    return processed_text.split()


def process_records(url, db, collection, num_workers=cpu_count()):
    col = MongoClient(url)[db][collection]

    items = col.find({}, projection={'_id': True, "body": True})
    with Pool(processes=num_workers) as pool:
        results = pool.map(worker, items)
    for item in results:
        for subi in item:
            print(subi)

if __name__ == '__main__':
    args = parse_args()
    process_records(args.mongo_url, args.mongo_db, args.mongo_collection)
