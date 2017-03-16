# coding=utf-8
import time
import redis
import json
import sys
r = redis.Redis(host='192.168.2.135',port=6379,db=0)
try:
    with open('wp_origin1') as f:
        for line in f:
            items=[word.strip() for word in line.split(' ')]
            json_items = json.dumps(items)
            r.lpush('tq',json_items)
        sys.exit(0)
except Exception as e:
    print e
