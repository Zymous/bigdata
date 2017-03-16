#coding: utf-8
#"it's a oop trying"
import sys 
reload(sys) 
sys.setdefaultencoding('utf-8')
import redis
import datetime
import MySQLdb
import json
import re

class Mysql_Insert(object):
    def redis_connect(self):
        #conn = redis.Redis(host='192.168.2.31',port=7000,db=0)
        conn = redis.Redis(host='192.168.2.135',port=6379,db=0)
        return conn
    def insert_Mysql(self):
        conn = MySQLdb.connect(host='localhost',port=3306,user='lcube',passwd='123456',db='bigdata',charset='utf8')
        cur = conn.cursor()
        sql1="insert into goods(item_id,time,mac,user_mac,url,ec_platform,item_name,category1,category2) values(%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        sql2="insert into wp_goods_detail(item_id,crawler_time,price) values(%s,%s,%s)"
        red = self.redis_connect()
        while red.llen('tq1'):
            ori_line = red.brpop('tq1',0)[1]
            sec_data = json.loads(ori_line)
            time = sec_data[0]
            mac = sec_data[1]
            user_mac = sec_data[2]
            url = sec_data[3]
            redis_text = url.split("-")[3]
            wp_id = "wp_"+redis_text.split(".")[0]
            item_id = redis_text.split(".")[0]
            item_name = red.hmget(wp_id,'brand_detail')
            category1 = red.hmget(wp_id,'category_sec')
            category2 = red.hmget(wp_id,'category_top')
            price = red.hmget(wp_id,'price')
            crawler_time = datetime.datetime.now()
            cur.execute(sql1,(item_id,time,mac,user_mac,url,'vip',item_name,category1,category2))
            cur.execute(sql2,(item_id,crawler_time,price))
            conn.commit()
        cur.close()
        conn.close()
        return
        # try:
        #     cur.executemany(sql1,goods)
        #     cur.executemany(sql2,details)
        #     cur.close()
        #     conn.commit()
        # except Exception as e:
        #     print traceback.print_exc()
        #     conn.rollback()
if __name__ == '__main__':
    c = Mysql_Insert()
    c.insert_Mysql()
