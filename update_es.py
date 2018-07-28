import happybase
import json

conn = happybase.Connection("nes4", port=18800)
table = conn.table("lawgu_ds")

f = open('draft.csv', 'r')
for line in f.read().splitlines():
    fields = line.split(",")
    row = table.row(fields[0], columns=[b'content:ext'])
    ext = json.loads(row[b'content:ext'].decode('utf8'))
    ext['effectiveDate'] = ''
    ext['timeLimited'] = '征求意见稿或草案'
    table.put(fields[0], {b'content:ext': json.dumps(ext, ensure_ascii=False).encode()})

