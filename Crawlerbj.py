#coding: utf-8
#"it's a oop trying"
import urllib2
import re
import pdb
import socket
from bs4 import BeautifulSoup
import multiprocessing
import redis
import datetime
import MySQLdb
import pdb
import time
import threading
import traceback
socket.setdefaulttimeout(5)

SLEEP_TIME = 1

class Crawler (object):
    ori=''
    fail_txt=''
    goods = []
    details = []
    fails = []
    ori_data = []
    #初始化导入文件
    def __init__(self,ori,fail_txt,goods=[],details=[],fails=[],ori_data=[]):
        self.ori = ori
        self.fail_txt = fail_txt
    #处理txt中的数据
    def handle(self):
        try:
            with open(self.ori) as f:
                for line in f:
                    items=[word.strip() for word in line.split(' ')]
                    self.ori_data.append(items)
        except Exception as e:
            print e
    #根据url得到html文档内容
    def url_parse(self,url):
        # proxy = {'http':'124.88.67.10:80'}
        # proxy_support = urllib2.ProxyHandler(proxy)
        # opener = urllib2.build_opener(proxy_support)
        # urllib2.install_opener(opener)
        i_headers = {'User-Agent':'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.48'}
        req = urllib2.Request(url,headers=i_headers)
        html = urllib2.urlopen(req)
        if url == html.geturl():
            doc = html.read()
            return doc
        return
    #连接redis
    def redis_connect(self):
        conn = redis.Redis(host='192.168.2.31',port=7000,db=0)
        return conn
    #根据所获取的网页内容获取所需字段
    def wp_html_crwal(self):
        r = self.redis_connect()
        while True:
            try:
                ori_line = self.ori_data.pop()
            except IndexError:
                break
            else:
                time = ori_line[0]
                mac = ori_line[1]
                user_mac = ori_line[2]
                url = ori_line[3]
                redis_text = url.split("-")[3]
                wp_id = "wp_"+redis_text.split(".")[0]
                try:
                    doc = self.url_parse(url)
                    if r.exists(wp_id):
                        item_info=r.get(wp_id)
                    else:
                        soup = BeautifulSoup(doc, "html.parser")
                        name_list_table = soup.find_all("script")
                        if(soup.find_all("p",class_="bt_title")):
                            brand_detail = soup.find_all("p",class_="bt_title")[0].string.strip()
                        elif(soup.find_all("p",class_="pib-title-class")):
                            brand = soup.find_all("p",class_="pib-title-class")[0].string.strip()
                            detail = soup.find_all("p",class_="pib-title-detail")[0].string.strip()
                            brand_detail = brand + detail
                        else:
                            brand_detail = soup.find_all("p",class_="pib-title-detail")[0].string.strip()
                        category_top_text = soup.find_all("div",class_="M-class")[0]
                        category_top = category_top_text.find_all(href=re.compile("html"))[0].string
                        category_regex = re.compile('\'category_name\'.*')
                        price_regex = re.compile('\'vipshop_price\'.*')
                        if len(name_list_table) == 26:
                            category_text = category_regex.findall(name_list_table[5].string)[0]
                            price_text = price_regex.findall(name_list_table[5].string)[0]
                        elif len(name_list_table) == 63:
                            category_text = category_regex.findall(name_list_table[-3].string)[0]
                            price_text = price_regex.findall(name_list_table[-3].string)[0]
                        category_sec = category_text.split('\'')[3]
                        price = price_text.split('\'')[3]
                        item_info=category_top+","+category_sec+","+brand_detail+","+price
                        r.set(wp_id,item_info)
                    items = item_info.split(",")
                    #item_id_text = url.split("-")[2]
                    #item_id = item_id_text.split(".")[0]
                    id_regex = re.compile('(\d{9})')
                    item_id = id_regex.findall(url)[0]
                    now = datetime.datetime.now()
                    self.goods.append((item_id,time,mac,user_mac,url,"vip",items[2],items[1],items[0]))
                    self.details.append((item_id,now,items[3]))
                except Exception as e:
                    print traceback.print_exc()
                    self.fails.append([time+' '+mac+' '+user_mac+' '+url])
                    pass
    #多线程处理
    def thread_handle(self):
        threads=[]
        while threads or self.ori_data:
            for thread in threads:
                if not thread.is_alive():
                    threads.remove(thread)
            while self.ori_data:
                thread = threading.Thread(target=self.wp_html_crwal)
                thread.setDaemon(True)
                thread.start()
                threads.append(thread)
                time.sleep(SLEEP_TIME)
    #将失败的数据输出到文件中
    def write_file(self,filename):
        with open(filename, 'a') as f:
            while self.fails:
                f.write(self.fails.pop()[0]+'\n')
    #插入数据进数据库中
    def insert_Mysql(self,goods,details):
        conn = MySQLdb.connect(host='localhost',port=3306,user='lcube',passwd='123456',db='bigdata',charset='utf8')
        cur = conn.cursor()
        sql1="insert into goods(item_id,time,mac,user_mac,url,ec_platform,item_name,category1,category2) values(%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        sql2="insert into wp_goods_detail(item_id,crawler_time,price) values(%s,%s,%s)"
        cur.executemany(sql1,goods)
        cur.executemany(sql2,details)
        cur.close()
        conn.commit()
        conn.close()

if __name__ == '__main__':
    c = Crawler('base_databi','failbi.txt')
    c.handle()
    #pdb.set_trace()
    #r = c.redis_connect()
    #goods,details = c.wp_html_crwal(data,c.fail)
    c.thread_handle()
    c.write_file(c.fail_txt)
    c.insert_Mysql(c.goods,c.details)
