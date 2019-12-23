import argparse
import concurrent.futures
import os

import html2text 


def process_file(src, dst):
    with open(src, 'r') as src_file:
        content = src_file.read()
        text = html2text.html2text(content)

    with open(dst, "w") as dst_file:
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
        futures = []
        for src_file, dst_file in get_files(args.src, args.dst):
            futures.append(executor.submit(process_file, src_file, dst_file))
        for future in concurrent.futures.as_completed(futures):
            rst = future.result()
            print(rst)
    
    
if __name__ == "__main__":
	main()
    
