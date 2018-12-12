# -*- coding: utf-8 -*-
import ConfigParser
import urllib2
import json
from bs4 import BeautifulSoup
import time

import redis

"""
author:junhong
date:2018-11-21
desc:百科词条抓取
step1:
    fetch_seed()
step2:
    fetch_detail()
"""

redis_db_index = 1
redis_db_index_2 = 2

conf = ConfigParser.SafeConfigParser()
conf.read("baike-crawler.ini")
redis_db_host = conf.get("local", "redis.host")
redis_db_port = conf.get("local", "redis.port")



def fetch(url):
    return urllib2.urlopen(url).read()


def do_run(index, page_num=100):
    for pn in range(1, page_num + 1):
        try:
            url = 'http://api.hudong.com/flushjiemi.do?flag=2&topic=%d&page=%d&type=2' % (index, pn)
            retText = fetch(url)
            print("ret = %s" % retText)
            ret_json = json.loads(retText, encoding='utf-8')
            result = ret_json["result"]
            artical_list = []
            if len(result) > 0:
                for ob in result:
                    # artical_list.append(ob["article_topic_name"])
                    # artical_list.append("%s%s%s" % (ob["article_topic_name"], "-", ob["article_id"]))
                    artical_list.append(ob["article_id"])
                save2redis(index, artical_list)
            # sleep
            if pn % 5 == 0:
                print 'pn=%d, sleeping...' % pn
                time.sleep(1)
        except:
            print "http get or parse error!"

    return 1


def save2redis(index, article_list):
    r = redis.Redis(host=redis_db_host, port=redis_db_port, db=redis_db_index)
    for article in article_list:
        r.sadd("%s-%d" % ("news.set", index), article)


def fetch_seeds():
    print "-- fetch seeds --"
    cnt = 0
    for def_index in range(4, 10):
        ret = do_run(index=def_index)
        cnt += ret
    print("cnt =  %d" % cnt)


def fetch_with_class(soup, class_type="jiemi-content"):
    return soup.find(class_=class_type).get_text()


def crawl(page_no):
    url = 'http://jiemi.baike.com/pa/detail?id=%s&type=1' % page_no
    print "url=", url
    content = fetch(url)
    soup = BeautifulSoup(content, "html.parser")
    return fetch_with_class(soup, class_type="jiemi-content")


def save_detail(seed, result=""):
    r = redis.Redis(host=redis_db_host, port=redis_db_port, db=redis_db_index_2)
    r.set("id_%s" % seed, result)
    return 1


def fetch_detail():
    print "-- fetch detail --"
    r = redis.Redis(host=redis_db_host, port=redis_db_port, db=redis_db_index)
    cnt = 0
    for news_index in range(4, 10):
        seeds = r.smembers("%s-%s" % ("news.set", news_index))
        if len(seeds) > 0:
            for seed in seeds:
                try:
                    ret = crawl(seed)
                    cnt += 1
                    if cnt % 10 == 0:
                        time.sleep(2)
                        print 'cnt=%d, sleeping...' % cnt
                    # save to redis
                    save_detail(seed, result=ret)
                    # break  # unit test
                except:
                    print "fetch detail error!!!"
    pass


if __name__ == '__main__':
    start = time.time()
    print "start fetch_seeds... "
    fetch_seeds()
    print "fetch_seeds finish! "
    print "start fetch_detail... "
    fetch_detail()
    print " fetch_detail finish!"
    # ret = crawl(11424)
    # print "ret = %s" % ret
    print "time used = ", (time.time() - start), '(s)'
