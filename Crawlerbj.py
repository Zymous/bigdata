#coding: utf-8
#"it's a oop trying"
import sys 
reload(sys) 
sys.setdefaultencoding('utf-8')
import urllib2
import re
import pdb
import socket
from bs4 import BeautifulSoup
import multiprocessing
import redis
import datetime
#import MySQLdb
import pdb
import time
import threading
import multiprocessing
import traceback
import json
socket.setdefaulttimeout(10)

SLEEP_TIME = 1

class Crawler (object):
    ori=''
    fail_txt=''
    result_txt=''
    goods = []
    details = []
    fails = []
    red = ''
    #初始化导入文件
    def __init__(self,ori,fail_txt,result_txt,red='',goods=[],details=[],fails=[]):
        self.ori = ori
        self.fail_txt = fail_txt
        self.result_txt = result_txt
        self.red = self.redis_connect()
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
        # proxy = {'http':'172.17.84.125:80'}
        # proxy_support = urllib2.ProxyHandler(proxy)
        # opener = urllib2.build_opener(proxy_support)
        # urllib2.install_opener(opener)
        i_headers = {'User-Agent':'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.48'}
        req = urllib2.Request(url,headers=i_headers)
        # try:
        html = urllib2.urlopen(req,data=None,timeout=10)
        # except urllib2.HTTPError,e:  
        #     print e.code  
        #     print e.reason  
        #     print e.geturl()  
        #     print e.read()  
        if url == html.geturl():
            doc = html.read()
            #print doc
            return doc
        return
    #连接redis
    def redis_connect(self):
        #conn = redis.Redis(host='192.168.2.31',port=7000,db=0)
        conn = redis.Redis(host='192.168.2.135',port=6379,db=0)
        return conn
    #根据所获取的网页内容获取所需字段
    def wp_html_crwal(self):
        #r = self.redis_connect()
        while self.red.llen('tq'):
            try:
                #ori_line = self.ori_data.pop()
                ori_line = self.red.brpop('tq',0)[1]
                #time.sleep(SLEEP_TIME)
                #print type(ori_line)
                sec_data = json.loads(ori_line)
                # print type(sec_data)
                # print len(sec_data)
                # break
                time = sec_data[0]
                #print type(time)
                mac = sec_data[1]
                user_mac = sec_data[2]
                url = sec_data[3]
                redis_text = url.split("-")[3]
                wp_id = "wp_"+redis_text.split(".")[0]
                #print time,mac,user_mac,url
                if self.red.hexists(wp_id,'price'):
                    #item_info=self.red.hgetall(wp_id)
                    # print item_info
                    # print item_info['price']
                    # print hlen(item_info)
                    self.red.lpush('tq1',ori_line)
                else:
                    #print 123
                    doc = self.url_parse(url)
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
                    if(soup.find_all("div",class_="M-class")):
                        category_top_text = soup.find_all("div",class_="M-class")[0]
                        category_top = category_top_text.find_all(href=re.compile("html"))[0].string
                    else:
                        category_top_text = soup.find_all("div",class_="bt_crumbs")[0]
                        if (category_top_text.find_all(href=re.compile("html"))):
                            category_top = category_top_text.find_all(href=re.compile("html"))[0].string
                        else:
                            category_top = category_top_text.find_all("a")[1].string or 'null'
                    category_regex = re.compile('\'category_name\'.*')
                    price_regex = re.compile('\'vipshop_price\'.*')
                    # number_rule = re.compile(r'^[0-9]+[\.][0-9]+$')
                    # number_rule1 = re.compile(r'^[0-9]+$')
                    if len(name_list_table) == 26:
                        category_text = category_regex.findall(name_list_table[5].string)[0]
                        price_text = price_regex.findall(name_list_table[5].string)[0]
                    elif len(name_list_table) == 63:
                        category_text = category_regex.findall(name_list_table[-3].string)[0]
                        price_text = price_regex.findall(name_list_table[-3].string)[0]
                    category_sec = category_text.split('\'')[3]
                    price = price_text.split('\'')[3]
                    item_info = {'brand_detail':brand_detail,'category_sec':category_sec,'category_top':category_top,'price':price}
                    # print item_info
                    #print item_info
                    self.red.hmset(wp_id,item_info)
                    self.red.lpush('tq1',ori_line)
                #     print price
                #     if re.match(number_rule,price) or re.match(number_rule1,price):
                #         try:
                #             item_info=(category_top,category_sec,brand_detail,price)
                #             r.hset(wp_id,category_top,category_sec,brand_detail,price)
                #         except:
                #             print url,price,category_top,category_sec,brand_detail
                #     else:
                #         print url,price,category_top,category_sec,brand_detail
                #         break
                # # items = item_info
                # print type(item_info)
                # print items[3]
                #item_id_text = url.split("-")[2]
                #item_id = item_id_text.split(".")[0]
                # id_regex = re.compile('(\d{8,})')
                # item_id = id_regex.findall(url)[0]
                # #print item_id
                # now = datetime.datetime.now()
                # self.goods.append((item_id,time,mac,user_mac,url,"vip",item_info['brand_detail'],item_info['category_sec'],item_info['category_top']))
                # self.details.append((item_id,now,item_info['price']))
            except Exception as e:
                # print traceback.print_exc()
                # print url
                self.red.lpush('tq2',ori_line)
                # self.fails.append([time+' '+mac+' '+user_mac+' '+url+' '+bytes(e)])
                pass
        #sys.exit(0) 
    #多线程处理
    def thread_handle(self):
        threads=[]
        while threads or self.red.llen('tq'):
            for thread in threads:
                if not thread.is_alive():
                    threads.remove(thread)
            while self.red.llen('tq'):
                thread = threading.Thread(target=self.wp_html_crwal)
                thread.setDaemon(True)
                thread.start()
                threads.append(thread)
                time.sleep(2)
    def multi_process(self):
        processes = []
        #num_cpus = multiprocessing.cpu_count()
        #pool = multiprocessing.Pool(5)
        for i in range(5):
            p = multiprocessing.Process(target=self.thread_handle)
            p.start()
            processes.append(p)
            time.sleep(2)
        # while self.red.llen('tq1'):
        #     pool.apply_async(self.thread_handle)
        for p in processes:
            p.join()
        return
        # pool.close()
        # pool.join()

    #将失败的数据输出到文件中
    # def write_file(self,filename):
    #     with open(filename, 'a') as f:
    #         while self.fails:
    #             f.write(self.fails.pop()[0]+'\n')
    # def write_result(self,filename):
    #      with open(filename, 'a') as f:
    #         while self.details and self.goods:
    #             f.write(self.goods.pop().__str__()+' '+self.details.pop().__str__()+'\n')
    #插入数据进数据库中
    # def insert_Mysql(self,goods,details):
    #     conn = MySQLdb.connect(host='localhost',port=3306,user='lcube',passwd='123456',db='bigdata',charset='utf8')
    #     cur = conn.cursor()
    #     sql1="insert into goods_test(item_id,time,mac,user_mac,url,ec_platform,item_name,category1,category2) values(%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    #     sql2="insert into wp_goods_detail(item_id,crawler_time,price) values(%s,%s,%s)"
    #     try:
    #         cur.executemany(sql1,goods)
    #         cur.executemany(sql2,details)
    #         cur.close()
    #         conn.commit()
    #     except Exception as e:
    #         print traceback.print_exc()
    #         conn.rollback()
    #     conn.close()

if __name__ == '__main__':
    #print '123'
    c = Crawler('wp_origin1','fail.txt','result.txt')
    #c.handle()
    #pdb.set_trace()
    #r = c.redis_connect()
    #goods,details = c.wp_html_crwal(data,c.fail)
    c.multi_process()
    # c.write_file(c.fail_txt)
    # c.write_result(c.result_txt)
    # c.insert_Mysql(c.goods,c.details)
