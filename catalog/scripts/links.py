# -*- coding: utf-8 -*-
"""Links and constants shared by merge.py, validate.py and build_xlsx.py: the guide's web slides (one anchor
per slide), the case-study results on GitHub, and the check date."""
import urllib.parse

CHECKED_ON = '2026-11-25'
GUIDE_TITLE = 'データ指向で設計する大規模クラウドサービス ― 全体像を押さえる 9 つのステップ'
GUIDE_REPO = 'https://github.com/Takenori-Kusaka/data-intensive-design-guide'
GUIDE_WEB = 'https://takenori-kusaka.github.io/data-intensive-design-guide/'
CASE_REPO = 'https://github.com/Takenori-Kusaka/data-intensive-design-casestudy'
CASE_BLOB = CASE_REPO + '/blob/main/'
TOPIC_NAME = {'A': '基盤をまたぐデータ統合', 'B': '置換と更新の演習', 'C': '共通基盤の費用と性能', 'D': '整合性の異常の再現'}
RECONCILE = 'ガイドの記述との照合'


def guide_slide(no):
    """The web version shows every slide as <section id="sN"> (tools/build_web_slides.py of the guide)."""
    return '%s#s%d' % (GUIDE_WEB, int(no))


def summary_path(topic):
    return 'results/%s/summary.md' % topic.lower()


def summary_link(topic, heading=None):
    """GitHub renders a heading as an anchor: lower-cased, spaces to '-', punctuation dropped."""
    url = CASE_BLOB + summary_path(topic)
    if heading:
        a = ''.join(ch for ch in heading.strip().lower().replace(' ', '-') if ch.isalnum() or ch in '-_')
        url += '#' + urllib.parse.quote(a)
    return url
