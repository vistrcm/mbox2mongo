import argparse
import mailbox

from mbox import process_header, walk_payload


def process_message(message):
    print("processing message {}".format(message["Message-ID"]))  # some kind of logging

    headers = {key.lower(): process_header(value) for key, value in message.items()}  # get message headers
    body = walk_payload(message)  # get body

    print(headers)
    print(body)


def process_mbox(mbox):
    for message in mbox:
        process_message(message)


def parse_args():
    parser = argparse.ArgumentParser(
        prog='mbox2files',
        description='Read mailbox, write data into files.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("mbox_path", help="path to mailbox file")
    parser.add_argument("dst", help="destination folder")
    parser.add_argument("--workers", "-w", help="number of workers", default=3, type=int)
    return parser.parse_args()


def main(mbox_path, dst, num_worker_threads):
    mbox = mailbox.mbox(mbox_path)
    process_mbox(mbox)


if __name__ == '__main__':
    args = parse_args()
    main(args.mbox_path, args.dst, args.workers)
