from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import base64
import binascii
import quopri
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

if __name__ == '__main__':
    args = parse_args()
    process_records(args.mongo_url, args.mongo_db, args.mongo_collection)


