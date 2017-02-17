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

    # try to decode from base64.
    # NOTE: may be wrong with some probability
    text = urlsafe_b64decode_if_necessary(text)

    # leave only printable symbols
    text = remove_unprintable(text)

    # decode MIME quoted-printable data
    text = quopri.decodestring(text)

    # converts a HTML into ASCII text
    text_in_utf = text.decode('latin-1')
    plain = html2text.html2text(text_in_utf)  # convert bytes to string
    return plain


def remove_unprintable(input_body):
    text = ''.join(filter(lambda x: x in string.printable, input_body))
    return text


def process_records(url, db, collection):
    col = MongoClient(url)[db][collection]
    for doc in col.find():
        print(">" * 100)
        print(process_body(doc["body"]))
        print("<" * 100)


def urlsafe_b64decode_if_necessary(s):
    try:
        # try to urlsafe_b64decode and decode resulting bytes to latin-1
        bytes_decoded = base64.urlsafe_b64decode(s)
        string_decoded = bytes_decoded.decode('latin-1')
        return string_decoded
    except binascii.Error:
        return s

if __name__ == '__main__':
    args = parse_args()
    process_records(args.mongo_url, args.mongo_db, args.mongo_collection)

    # record = get_record(args.mongo_url, args.mongo_db, args.mongo_collection)
    # print(record["body"])
    # processed = process_body(record["body"])
    # print(processed)

    # from sklearn.feature_extraction.text import CountVectorizer
    # count_vect = CountVectorizer()
    # X_train_counts = count_vect.fit_transform([processed])
    # print(X_train_counts.shape)
    # print(X_train_counts)
    # print(count_vect.get_stop_words())

