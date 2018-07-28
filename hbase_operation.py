import happybase
import json
# conn = happybase.Connection("nes4", port=18800)
# table = conn.table("lawgu_ds")

file = open('q_lvshang_not_national.csv', 'r')
for line in file:
    lid = line.split(",")[0]
    print(lid)
    # row = table.row(lid, columns=['content:ext'])
    # data = json.loads(row[b'content:ext'].decode('utf8'))
    # print(data['timeLimited'])
    # data['timeLimited'] = '草案'
    table.put(lid, {b'mark:status': "1".encode()})
