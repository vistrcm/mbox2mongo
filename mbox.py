from email.header import Header, decode_header


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


def is_chat(message):
    """Check if Gmail message is chat message, not email.

    Google/Hangout chats is shown in emails with label 'Chat'. Let's filter such messages."""
    labels = message.get('X-Gmail-Labels', "").split(',')
    return 'Chat' in labels


def walk_payload(message):
    div = "\n"

    if message.is_multipart():
        parts = []
        for part in message.walk():
            content_type = part.get_content_type()
            content_disposition = part.get('Content-Disposition')

            maintype, _ = content_type.split('/')
            if maintype == 'text' and content_disposition is None:  # skip data with non 'text/*' context type
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
