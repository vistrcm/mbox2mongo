import argparse
import mailbox
import sys
from email.header import Header, decode_header
from multiprocessing import JoinableQueue
from multiprocessing import Process

from pymongo import MongoClient


def worker(mongo_collection, que):
    while True:
        message = que.get()
        if message is None:  # stop worker if got None
            break

        db_record = {
            "headers": {key: process_header(value) for key, value in message.items()},  # get message headers
            "body": walk_payload(message)  # get body
        }

        try:
            mongo_collection.insert_one(db_record)
        except Exception as ex:
            sys.stderr.write("issue '%s' happened inserting: %s as record %s\n" % (ex, message, db_record))
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
        print("processing message {}".format(message["Message-ID"]))  # some kind of logging
        que.put(message)


def main(mbox_path, mongo_url, db_name, db_collection, num_worker_threads):
    mbox = mailbox.mbox(mbox_path)

    que = JoinableQueue()
    processes = []
    for i in range(num_worker_threads):
        # create separate mongo clients for each worker
        mongo_client = MongoClient(mongo_url, connect=False)  # do not connect immediately connect on forked process
        collection = mongo_client[db_name][db_collection]

        p = Process(target=worker, args=(collection, que))
        p.start()
        processes.append(p)

    # noinspection PyTypeChecker
    process_mbox(mbox, que)

    # block until all tasks are done
    que.join()

    # stop workers
    for i in range(num_worker_threads):
        que.put(None)
    for p in processes:
        p.join()


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
