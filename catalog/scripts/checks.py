# -*- coding: utf-8 -*-
"""Text checks used by validate.py: does a hand-written number occur in the summary rows and sections the row
points at, and does every stated zero say out of how many trials or runs."""
import os
import re

import links

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(HERE))


def sections_of(topic):
    """heading -> body of results/<topic>/summary.md (a body runs to the next heading of the same or a higher level)"""
    lines = open(os.path.join(ROOT, links.summary_path(topic)), encoding='utf-8').read().split('\n')
    out = {}
    for i, l in enumerate(lines):
        m = re.match(r'^(#+)\s*(.+?)\s*$', l)
        if not m:
            continue
        level, body = len(m.group(1)), []
        for l2 in lines[i + 1:]:
            m2 = re.match(r'^(#+)\s', l2)
            if m2 and len(m2.group(1)) <= level:
                break
            body.append(l2)
        out[m.group(2)] = '\n'.join(body)
    return out


IDS = re.compile(r'RQ-[A-Z0-9]+-\d+|\b[A-E]-[A-Z]?\d+\b|E-[A-D]\d|B[1-6]\b|Part \d+|v\d|\^31')
NUM = re.compile(r'\d+(?:[.,]\d+)*')


def numbers(text, strip_ids=True):
    if strip_ids:
        text = IDS.sub(' ', text)
    return set(x.replace(',', '') for x in NUM.findall(text))


ZERO = re.compile(r'(?<![\d.,/〜])0(?![\d.,/〜]|\s*(?:ms|秒|MB|%|倍|番|日))')
COUNTED = re.compile(r'(?<!再)試行|実行|件中|/\s?\d')
NOT_A_COUNT = re.compile(r'RQ-[A-Z0-9]+-\d+|max_slot_wal_keep_size=-?\d+|経路の遅延 0|0 で埋まる|0 以上|使用量 0')


def zeros_without_count(text):
    """clauses that state a zero result without the number of trials or runs"""
    t = NOT_A_COUNT.sub(' ', text)
    return [s.strip() for s in re.split(r'[。]', t) if ZERO.search(s) and not COUNTED.search(s)]
