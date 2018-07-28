from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, Q
import csv
import json

count = 0
# source = ['lid', 'title', 'ds_source', 'eff_level', 'dispatch_authority', 'time_limited', 'posting_date',
#           'document_number', 'effective_range', 'province']
source = ['lid', 'title', 'content', 'url']
new_source = ['lid', 'title', 'new_title', 'url']

q_eff_level_missing = Q('bool', must_not=[Q("exists", field="eff_level")])
q_nation_local = Q('bool', must=[Q("match", effective_range="全国")],
                   should=[Q("match", eff_level="地方法规/文件"), Q("match", eff_level="地方司法文件")],
                   minimum_should_match=1)
q_province_missing = Q('bool', must_not=[Q("exists", field="province")])
q_city_missing = Q('bool', must_not=[Q("exists", field="effective_range")])
q_dispatch_authority_missing = Q('bool', must_not=[Q("exists", field="dispatch_authority")])
q_lvshang_not_national = Q('bool', must=[Q("term", ds_source="fg_lvshangsite")],
                           must_not=[Q("term", effective_range="全国")])
q_lvshang_disaptch_empty = Q('bool', must=[Q("term", ds_source="fg_lvshangsite")],
                             must_not=[Q("exists", field="dispatch_authority")])
q_faxin_efflevel_empty = Q('bool', must=[Q("term", ds_source="fg_crawl_faxin")],
                           must_not=[Q("exists", field="eff_level")])
q_pkulaw_efflevel_empty = Q('bool', must=[Q("term", ds_source="fg_crawl_pkulaw")],
                            must_not=[Q("exists", field="eff_level")])

q_faxin_effective_range_empty = Q('bool', must=[Q("term", ds_source="fg_crawl_faxin")],
                                  must_not=[Q("exists", field="effective_range")])
q_pkulaw_effective_range_empty = Q('bool', must=[Q("term", ds_source="fg_crawl_pkulaw")],
                                   must_not=[Q("exists", field="effective_range")])

q_faxin_disaptch_empty = Q('bool', must=[Q("term", ds_source="fg_crawl_faxin")],
                           must_not=[Q("exists", field="dispatch_authority")])
q_pkulaw_disaptch_empty = Q('bool', must=[Q("term", ds_source="fg_crawl_pkulaw")],
                            must_not=[Q("exists", field="dispatch_authority")])

q_posting_date_empty = Q('bool', must_not=[Q("exists", field="posting_date")])
q_draft = Q('bool', must=[Q("term", time_limited="征求意见稿或草案")])
# 》
q_bad_quote = Q('bool', must=[Q("term", title="》")], must_not=[Q("term", title="《")])
# 《
q_bad_quote1 = Q('bool', must=[Q("term", title="《")], must_not=[Q("term", title="》")])

q_lose = Q('bool', should=[Q("regexp", title_term=".*《[^》]*《.*"), Q("regexp", title_term=".*》[^《]*》.*"),
                           Q("regexp", title_term="[^《》]*《[^》]*"), Q("regexp", title_term="[^《》]*》.*"),
                           Q("regexp", title_term="([^《》]*《[^《》]*》[^《》]*)*《[^》]*"),
                           Q("regexp", title_term="([^《》]*《[^《》]*》[^《》]*)*》.*")])

# 《《
q_bad_quote6 = Q('bool', must=[Q("regexp", title_term=".*《[^》]*《.*")])
q_bad_quote66 = Q('bool', must=[Q("regexp", title_term="([^《》]*《[^《》]*》[^《》]*)*》.*")])
# 》》
q_bad_quote7 = Q('bool', must=[Q("regexp", title_term=".*》[^《]*》.*")])
# 《开头
q_bad_quote8 = Q('bool', must=[Q("regexp", title_term="《.*")])
# 》 结尾
q_bad_quote9 = Q('bool', must=[Q("regexp", title_term=".*》")])
# 《.*》
q_bad_quote10 = Q('bool', must=[Q("regexp", title_term="《.*》")])
# 《全是文字》
q_bad_quote4 = Q('bool', must=[Q("regexp", title_term="《[^《》〉〈]*》")])

# 需要调整的标题
q_modify = Q('bool', should=[Q("regexp", title_term="《[^《》〉〈]*》"), Q("regexp", title_term="《[^《》]*〈[^《》〈]*〉[^《》〈〉]*》")])
# 《<>》
q_bad_quote2 = Q('bool', must=[Q("regexp", title_term="《[^《》]*〈[^《》〈]*〉[^《》〈〉]*》")])
# 《》《》《
q_bad_quote22 = Q('bool', must=[Q("regexp", title_term="([^《》]*《[^《》]*》[^《》]*)*《[^》]*")])

q_bad_quote222 = Q('bool', must=[Q("regexp", title_term="[^《》]*<.*")])

# 《《《》》
q_bad_quote5 = Q('bool', must=[Q("regexp", title_term="[^《》]*《[^《》]*《[^《》]*《[^《》]*》[^《》]*》[^》]*")])
# 《》《》
q_double = Q('bool', must=[Q("regexp", title_term="[^《》].*?《[^《》]*》[^《》].*?《[^《》]*》[^《》]*")])
q_double1 = Q('bool', must=[Q("regexp", title_term=".([^《》].*?《[^《》]*》[^《》].*?)?")])

# 应该去掉两边的外书名号的
q_drop = Q('bool', must=[Q("regexp", title_term="[^《]*》.*")])

# q_bad_quote3 = Q('bool', must=[Q("regexp", title_term="[^《》]*")])

es = Elasticsearch(hosts="ds1:9206")
s = Search(using=es, index="law_0713b", doc_type="law_regu").source(source).query(q_lose)
print(json.dumps(s.to_dict()))
with open('q_bad_quote_auto.csv', 'a') as f:
    with open('q_bad_quote_artificial.csv', 'a') as f1:
        w = csv.DictWriter(f, source)
        w1 = csv.DictWriter(f1, source)

        w.writeheader()
        w1.writeheader()
        for hit in s.scan():
            content = hit["content"].split('\n')[:3]
            lid = hit["lid"]
            title = hit["title"]
            new_title = ''
            url = "http://yjs.alphalawyer.cn/#/app/law-statute-manage?id=" + lid + ""
            d = {}
            d1 = {}
            for t in title:
                if (t not in ['《', "》", "（", "）", "〈", "〉"]):
                    new_title += t

            for content01 in content:
                new_content = ""
                for t in content01:
                    if (t not in ['《', "》", "（", "）", "〈", "〉"]):
                        new_content += t

                if (new_title in new_content):
                    if((content01.endswith('）')) and ('号' in new_content) and ('修' not in new_content) and ('试行' not in new_content) ):
                        index=content01.find('（')
                        content01=content01[:index]
                    d = {'lid': lid, 'title': title, 'content': content01, 'url': url}
                    w.writerow(d)
                    break

                else:
                    d1 = {'lid': lid, 'title': title, 'content': content, 'url': url}
                    w1.writerow(d1)
                    break

            # print(content)

            # w.writerow(hit.to_dict())
            count += 1
            print(title)
            print()
            print(count)

# 《全是文字》去除外层的大括号
# s = Search(using=es, index="law_0713b", doc_type="law_regu").source(new_source).query(q_modify)
# with open('q_modify.csv', 'a') as f:
#     w = csv.DictWriter(f, new_source)
#     w.writeheader()
#     for hit in s.scan():
#         # content = hit["content"].split('\n')[:3]
#         lid = hit["lid"]
#         title = hit["title"]
#         url = "http://yjs.alphalawyer.cn/#/app/law-statute-manage?id=" + lid + ""
#
#         new_title = title.strip('《》')
#         for t in new_title:
#             if (t in ["〈", "〉"]):
#                 new_title = new_title.replace('〈', '《').replace('〉', '》')
#         d = {'lid': lid, 'title': title, 'new_title': new_title, 'url': url}
#         w.writerow(d)
#         # print(content)
#
#         # w.writerow(hit.to_dict())
#         count += 1
#         print(title)
#         print()
#         print(count)
# 《<>》
# s = Search(using=es, index="law_0713b", doc_type="law_regu").source(new_source).query(q_bad_quote2)
# with open('q_bad_quote3.csv', 'a') as f:
#     w = csv.DictWriter(f, new_source)
#     w.writeheader()
#     for hit in s.scan():
#         # content = hit["content"].split('\n')[:3]
#         lid = hit["lid"]
#         title = hit["title"]
#         new_title = title.strip('《》').replace('〈', '《').replace('〉', '》')
#
#         url = "http://yjs.alphalawyer.cn/#/app/law-statute-manage?id=" + lid + ""
#
#         d = {'lid': lid, 'title': title, 'new_title': new_title, 'url': url}
#         w.writerow(d)
#         # print(content)
#
#         # w.writerow(hit.to_dict())
#         count += 1
#         print(title)
#         print()
#         print(count)
