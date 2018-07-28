from elasticsearch import Elasticsearch, NotFoundError
import json

client = Elasticsearch(hosts="nes4:9206")
while True:
    res = client.search(index="judgement_20180409", doc_type='judgement', body={
        "_source": [
            "litigant_company"
        ],
        "query": {
            "filtered": {
                "filter": {
                    "exists": {
                        "field": "litigant_company"
                    }
                }
            }
        },
        "aggs": {
            "colors": {
                "terms": {
                    "field": "litigant_company",
                    "order": {
                        "_count": "desc"
                    }
                }
            }
        }
    })
    print(res)
