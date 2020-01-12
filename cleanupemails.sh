#!/bin/bash

SRC_DIR=data/emails
DST_DIR=data/emails_clean

mkdir -p ${DST_DIR}

for FILE in `ls ${SRC_DIR}`
do
    SRC=${SRC_DIR}/${FILE}
    DST=${DST_DIR}/${FILE}
    cat ${SRC} | sed '/^>/d' | sed '/^|/d' | sed '/^$/d' | sed '/^[[:blank:]].*$/d' | sed '/[a-zA-Z].*/!d' > ${DST}
done

# clean empty files
find ${DST_DIR}/ -type f -size 0 -exec rm {} \;

