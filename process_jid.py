import json
import happybase
import re
from kafka import KafkaProducer

# conn = happybase.Connection('nes4', port=18800)
# table = conn.table("judgement_ds")


def wrap_content(content=""):
    return re.sub(r"([:：！!?？。])\n", r"\1", content)


producer = KafkaProducer(bootstrap_servers=['ds1:9092'])


f = open('data.json')
data = json.loads(f.read())
ff = open('result.csv', 'w')
for d in data:
    dsid = d['_source']['dsid']
    jid =d['_source']['jid']
    caseNumber = d['_source']['all_caseinfo_casenumber']
    future = producer.send('judgement_ds_structed', dsid.encode())
producer.flush()
#     row = table.row(dsid, columns=[b'content:text'])
#     text = row[b'content:text'].decode('utf8')
#     text = wrap_content(text)
#     print(text)
    # table.put(dsid, {b'content:text': text.encode()})
