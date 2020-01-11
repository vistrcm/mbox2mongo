import argparse
import collections
import concurrent.futures
import os
import re
import hashlib

import html2text

# durty hack with global variable. may be not safe and no guarantee
seen_hashes = collections.defaultdict(list)

def md5(text: str):
    h = hashlib.md5()
    h.update(text.encode("utf8"))
    return h.hexdigest()

def preprocess_text(text):
    # windows like
    text = re.sub(r"\r\n", "\n", text)
    # durty hack to replace \n with <br> to fool html2text
    text = re.sub(r"\n", "<br>", text)

    return text

def process_file(src, dst):
    with open(src, 'r') as src_file:
        content = src_file.read()

        # no need to process empty content
        if len(content) ==0 or content.isspace():
            return f"{src} -X {dst}"

        content = preprocess_text(content)
        text = html2text.html2text(content, bodywidth=0)
        # remove spaces at the beginning and the end
        text = text.strip()

        # clean strange symbols
        text  = text.encode('utf8','replace').decode('utf8','replace')

        # no need to save empty text
        if len(text) ==0 or text.isspace():
            return f"{src} XX {dst}"
        # check for seen hashes
        h = md5(text)
        if h in seen_hashes:
            return f"{src} -Y {dst}: {seen_hashes[h]}"
        seen_hashes[h].append(src)

    with open(dst, "w", encoding="utf8") as dst_file:
        dst_file.write(text)

    return f"{src} -> {dst}"

def get_files(src_dir, dst_dir):
    for root, _, files in os.walk(src_dir):
        for name in files:
            if name.endswith(".txt"):
                src_file = os.path.join(root, name)
                dst_file = os.path.join(dst_dir, name)
                yield src_file, dst_file

def main():
    # arguments
    parser = argparse.ArgumentParser(description="extract text from files")
    parser.add_argument("src", type=str, help="src dir")
    parser.add_argument("dst", type=str, help="dst dir")
    args = parser.parse_args()

    os.makedirs(args.dst, exist_ok=True)  # need to make sure dst exists
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = {}
        for src_file, dst_file in get_files(args.src, args.dst):
            futures[executor.submit(process_file, src_file, dst_file)] = (src_file, dst_file)
        for future in concurrent.futures.as_completed(futures):
            try:
                rst = future.result()
                print(rst)
            except UnicodeEncodeError as ex:
                print(f"got exception processing {futures[future][0]}")
                print(ex)
                raise ex

if __name__ == "__main__":
	main()
