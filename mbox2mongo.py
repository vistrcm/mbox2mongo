import argparse
import mailbox
import queue
import threading

import sys
from pymongo import MongoClient


def worker(mongo_collection, que):
    while True:
        item = que.get()
        if item is None:  # stop worker if got None
            break
        try:
            mongo_collection.insert_one(item)
        except Exception as ex:
            sys.stderr.write("exception on writing to db item: %s", item)
        finally:
            que.task_done()


def walk_payload(message):
    result = ""
    div = "\n"
    for part in message.walk():
        maintype, _ = part.get_content_type().split('/')
        if maintype != 'text':
            result = result + "<looks like attachment>" + div
        else:
            result = result + part.get_payload() + div
    return result


def process_mbox(mbox, que):
    for message in mbox:
        print("processing message {}".format(message["Message-ID"]))  # some kind of logging
        db_record = {"headers": {}, "body": None}

        # get message headers
        for key, value in message.items():
            db_record["headers"][key] = value

        # get body
        db_record["body"] = walk_payload(message)

        que.put(db_record)


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

    process_mbox(mbox, que)

    # block until all tasks are done
    que.join()

    # stop workers
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
