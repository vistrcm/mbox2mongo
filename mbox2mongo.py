import argparse
import mailbox
import queue
import sys
import threading

from pymongo import MongoClient

from mbox import process_header, is_chat, walk_payload


def worker(mongo_collection, que):
    while True:
        message = que.get()
        if message is None:  # stop worker if got None
            break
        try:
            print("processing message {}".format(message["Message-ID"]))  # some kind of logging

            if is_chat(message):  # skip "Chat" messages here.
                continue

            headers = {key.lower(): process_header(value) for key, value in message.items()}  # get message headers
            body = walk_payload(message)  # get body
            db_record = {
                "headers": headers,
                "body": body
            }

            mongo_collection.insert_one(db_record)
        except Exception as ex:
            sys.stderr.write("exception '%s' happened on writing to db item: %s\n" % (ex, message))
        finally:
            que.task_done()


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
