"""Dev: try statements against a product and print results or the head of the error.
    bash cs/c/py.sh cs.c.dev_probe ch|pg|duck <file with one statement per paragraph>
Lines starting with '-- body:' give the POST body of the next ClickHouse statement."""
import sys

from . import prod


def main(product, path):
    with open(path, encoding='utf-8') as f:
        stmts = [s.strip() for s in f.read().split('\n\n') if s.strip()]
    prod.down()
    con = None
    try:
        if product == 'ch':
            prod.ch_up()
        elif product == 'pg':
            prod.pg_up()
        else:
            con = prod.duck()
        for s in stmts:
            body = None
            if s.startswith('-- body:'):
                first, s = s.split('\n', 1)
                body = first[len('-- body:'):].strip().encode().decode('unicode_escape').encode()
            print('>>>', s[:300].replace('\n', ' '))
            try:
                if product == 'ch':
                    out = prod.ch_raw(s, body=body)[0] if body is not None else prod.ch(s)
                elif product == 'pg':
                    out = prod.pgq(s)
                else:
                    cur = con.execute(s)
                    out = cur.fetchall() if cur.description else None
                print('   ', str(out)[:700])
            except Exception as e:  # noqa: BLE001
                print('    ERROR', str(e).replace('\n', ' ')[:420])
    finally:
        prod.down()


if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2])
