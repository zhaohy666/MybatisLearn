import happybase
conn = happybase.Connection("nes4", port=18800)
table = conn.table("lawgu_ds")
with open('q_lvshang_not_national.csv', 'r') as f:
    for line in f:
        lid = line.split(",")[0]
        print(lid)
        # table.put(lid, {b'mark:status': "1".encode()})
