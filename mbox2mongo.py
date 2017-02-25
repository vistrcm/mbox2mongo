import argparse
import mailbox
import queue
import sys
import threading
from email.header import Header, decode_header

from pymongo import MongoClient

from utils import text_to_words


def worker(mongo_collection, que):
    while True:
        message = que.get()
        if message is None:  # stop worker if got None
            break
        try:
            print("processing message {}".format(message["Message-ID"]))  # some kind of logging
            headers = {key: process_header(value) for key, value in message.items()}  # get message headers
            body = walk_payload(message)  # get body
            words = [word for word in text_to_words(body)]
            db_record = {
                "headers": headers,
                "body": body,
                "words": words
            }

            mongo_collection.insert_one(db_record)
        except Exception as ex:
            sys.stderr.write("exception '%s' happened on writing to db item: %s\n" % (ex, message))
        finally:
            que.task_done()


def walk_payload(message):
    div = "\n"

    if message.is_multipart():
        parts = []
        for part in message.walk():
            maintype, _ = part.get_content_type().split('/')
            if maintype == 'text':  # skip data with non 'text/*' context type
                payload_str = try_decode(part)
                parts.append(payload_str)
        return div.join(parts)
    else:
        return try_decode(message)


def try_decode(msg):
    charset = msg.get_content_charset('ascii')  # use ascii as failobj by default.
    part_payload = msg.get_payload(decode=True)
    try:
        payload_str = part_payload.decode(charset)
    except UnicodeDecodeError:  # Guess: try to decode using 'utf-8' if charset does not work
        # try to decode with 'replace' error handling scheme. Most probably loose some data
        payload_str = part_payload.decode('utf-8', 'ignore')
    return payload_str


def process_header(header):
    val = []
    if type(header) == Header:
        for code, encoding in decode_header(header):  # it may be many values
            if encoding in (None, "unknown-8bit"):
                val.append(code)
            else:
                val.append(code.decode(encoding))
    else:
        val = header
    return val


def process_mbox(mbox, que):
    for message in mbox:
        que.put(message)


def main(mbox_path, mongo_url, db_name, db_collection, num_worker_threads):
    mbox = mailbox.mbox(mbox_path)

    que = queue.Queue()
    threads = []
    for i in range(num_worker_threads):
        # create separate mongo clients for each worker
        mongo_client = MongoClient(mongo_url)
        collection = mongo_client[db_name][db_collection]

        t = threading.Thread(target=worker, args=(collection, que))
        t.start()
        threads.append(t)

    try:
        process_mbox(mbox, que)
        # block until all tasks are done
        que.join()
    finally:
        # stop workers even if exceptions occures
        for i in range(num_worker_threads):
            que.put(None)
        for t in threads:
            t.join()


def parse_args():
    parser = argparse.ArgumentParser(
        prog='mbox2mongo',
        description='Read mailbox, write data into mongo db.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("mbox_path", help="path to mailbox file")
    parser.add_argument("mongo_url", help="mongodb url")
    parser.add_argument("--mongo_db", "-d", help="mongo database name", default="archive")
    parser.add_argument("--mongo_collection", "-c", help="mongo collection name", default="mails")
    parser.add_argument("--workers", "-w", help="number of workers", default=3, type=int)
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()
    main(args.mbox_path, args.mongo_url, args.mongo_db, args.mongo_collection, args.workers)
