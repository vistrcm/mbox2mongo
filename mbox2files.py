import argparse
import hashlib
import mailbox
import os

from mbox import walk_payload, is_chat, process_header


def message2file(datafile, text):
    with open(datafile, 'w', encoding='utf-8') as data:
        data.write(text)


def meta2file(metafile, headers):
    with open(metafile, 'w', encoding='utf-8') as meta:
        for key, value in headers.items():
            meta.write(f"{key}: {value}\n")


def get_filenames(dst, mid):
    """returns two files with different extensions (data, meta)"""
    m = hashlib.md5(mid.encode('utf-8')).hexdigest()
    data = f"{dst}/{m}.txt"
    meta = f"{dst}/{m}.meta"
    return data, meta


def process_message(message, dst):
    mid = message["Message-ID"]
    print(f"processing message {mid}")  # some kind of logging

    # handle messages without message-ID
    if mid is None:
        mid = "-".join([message["From"], message["To"], message["Subject"]])
        print(f"message-id is None, using {mid}")

    data, meta = get_filenames(dst, mid)

    headers = {key.lower(): process_header(value) for key, value in message.items()}  # get message headers
    meta2file(meta, headers)

    body = walk_payload(message)  # get body

    body = body.encode('utf-16', 'surrogatepass').decode('utf-16')  # some symbols cleanup
    message2file(data, body)


def process_mbox(mbox, dst):
    for key, message in mbox.iteritems():
        print(f"processing {key}")
        if is_chat(message):  # skip "Chat" messages here.
            continue
        process_message(message, dst)


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
    os.makedirs(dst, exist_ok=True)
    process_mbox(mbox, dst)


if __name__ == '__main__':
    args = parse_args()
    main(args.mbox_path, args.dst, args.workers)
