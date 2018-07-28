from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, Q
import csv
import json
source = ['jid']

q_length = Q('bool',  filter=[Q('script', script="_source.paragraphs.size() < 3")])


es = Elasticsearch(hosts="nes1:9206")
s = Search(using=es, index="judgement_20180518", doc_type="judgement").source(source).query(q_length)
print(json.dumps(s.to_dict()))
with open('q_error_paragraph.csv', 'w') as f:
    w = csv.DictWriter(f, source)
    w.writeheader()
    for hit in s.scan():
        w.writerow(hit.to_dict())
        print(hit)


