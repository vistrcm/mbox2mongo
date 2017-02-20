from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import mailbox

src = "src.mbox"
dst = "filtered.mbox"
id_conds = [
    "<someid@domain.com>",

]

src_mbox = mailbox.mbox(src)
dst_mbox = mailbox.mbox(dst)

last_message = False  # hack to get one more message after empty list

dst_mbox.lock()
for message in src_mbox:
    mid = message["Message-ID"]
    if mid in id_conds:
        dst_mbox.add(message)
        id_conds.remove(mid)
        if not id_conds:  # exit loop if id_conds is empty by setting last_message
            last_message = True
    elif last_message:
        dst_mbox.add(message)
        break

dst_mbox.flush()
dst_mbox.unlock()
