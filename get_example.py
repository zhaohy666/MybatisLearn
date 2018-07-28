from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, Q
import csv


source = ['lid', 'title', 'dispatch_authority', 'document_number', 'effective_range', 'eff_level', 'url']

# 征求意见稿）or 征求意见稿 or 草案）or 草案

q_by_eff_level = Q('bool', must=[Q("match", eff_level="军事法规")])

count = 0
es = Elasticsearch(hosts="nes1:9206")
s = Search(using=es, index="law_regu_dev", doc_type="law_regu").source(source).query(q_by_eff_level)
with open('q_by_eff_level.csv', 'a') as f:
    w = csv.DictWriter(f, source)
    # w.writeheader()
    for hit in s.scan():
        hit['url'] = 'https://dev.alphalawyer.cn/#/app/tool/lawsResult/%7B%5B%5D,%7D/detail/%7B' + hit['lid'] + ',%20%7D'
        w.writerow(hit.to_dict())
        count += 1
        if count == 200:
            break
