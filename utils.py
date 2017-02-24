from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import sys
import unicodedata

import html2text

# Prepare translation table to clean some non-important symbols.
# See http://www.unicode.org/reports/tr44/#General_Category_Values for unicode categories
exclude_unicode_set = {
    "Cc",  # Control: a C0 or C1 control code
    "Cf",  # Format: a format control character
    "Cs",  # Surrogate: a surrogate code point
    "Co",  # Private_Use: a private-use character
    "Cn",  # Unassigned: a reserved unassigned code point or a noncharacter
    "Zl",  # Line_Separator:	U+2028 LINE SEPARATOR only
    "Zp",  # Paragraph_Separator:	U+2029 PARAGRAPH SEPARATOR only
    "Sm",  # Math_Symbol: a symbol of mathematical use
    "Sc",  # Currency_Symbol: a currency sign
    "Sk",  # Modifier_Symbol: a non-letterlike modifier symbol
    "So",  # Other_Symbol: a symbol of other type
    "Pc",  # Connector_Punctuation: a connecting punctuation mark, like a tie
    "Pd",  # Dash_Punctuation: a dash or hyphen punctuation mark
    "Ps",  # Open_Punctuation: an opening punctuation mark (of a pair)
    "Pe",  # Close_Punctuation: a closing punctuation mark (of a pair)
    "Pi",  # Initial_Punctuation: an initial quotation mark
    "Pf",  # Final_Punctuation: a final quotation mark
    "Po",  # Other_Punctuation: a punctuation mark of other type
}

include_symbols = '@./?'  # "@" and "." for emails; "/" and "?" for urls (only path) glue
all_chars = set(chr(i) for i in range(sys.maxunicode))
exclude_chars = set(c for c in all_chars if unicodedata.category(c) in exclude_unicode_set)
translate_table = {ord(character): " " for character in exclude_chars}
# replace "@" and "." and others by ``None`` to glue emails and urls
translate_table.update({ord(character): None for character in include_symbols})
hex_trans_table = {ord(char): None for char in '0123456789abcdefABCDEF'}


def is_hex_number(s):
    """using hex_trans_table try to identify if string is hex number or not"""
    return s.translate(hex_trans_table) == ''


def process_body(body):
    # lower
    text = body.lower()

    # remove html at first
    text = html2text.html2text(text)

    # clean some symbols. for example control
    text = text.translate(translate_table)

    # split to words
    words = text.split()
    # filter digits
    words = filter(lambda x: not x.isdigit() and not is_hex_number(x), words)
    return words
