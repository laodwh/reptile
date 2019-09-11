# _*_ coding:utf‐8 _*
import os
import random
import time

import pymysql
from bs4 import BeautifulSoup
from urllib import request
from config_info import *
import openpyxl


def get_htmls(url,num,items = []):

    # 用户名：root 密码：root 数据库名：python_db
    db = pymysql.connect(ip, user, password, table, charset="utf8")
    cursor = db.cursor()
    try:
        for i in range(1, num+1):
            print(url % i)
            get_html(url%i,items)
            if i%2==0 or i==num:
               # write_excel(items)

                for nips in range(len(items)):
                    cursor.execute('SET NAMES utf8mb4;')
                    sql = "insert into tb_ip(country,ip_address,port,server_address,anonymous,type,speed,connection,lifetime,verification) values('" + \
                          items[nips][0] + "'" + "," + "'" + items[nips][1] + "'" + "," + "'" + items[nips][
                              2] + "'" + "," + "'" + items[nips][3] + "'" + "," + "'" + items[nips][4] + "'" + "," + "'" + \
                          items[nips][5] + "'" + "," + "'" + items[nips][6] + "'" + "," + "'" + items[nips][7] + "'" + "," + \
                          "'" + items[nips][8]+"'" + "," + "'" + items[nips][9]+ "')"
                    print('---' + sql + '---');
                    cursor.execute(sql)
                    db.commit()

                items = []
    except Exception as e:
        pass
        print(e);


def get_html(url,items,num_retries = 2):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36'}
    try:
        randint_data = random.randint(1, 20)
        if randint_data < 7:
            randint_data = 7
        time.sleep(randint_data)

        response = request.Request(url=url, headers=headers)
        html = request.urlopen(response).read().decode('utf-8')
        soup = BeautifulSoup(html, 'html.parser')
        trs = soup.find_all('tr')
        # items = []
        for i in range(1, len(trs)):
            try:
                tds = trs[i].find_all("td")
                tds0,tds6,tds7 = '','',''
                if len(tds) == 10:
                    if tds[0].img: tds0 = tds[0].img["alt"]
                    if tds[6].div:tds6 = tds[6].div["title"]
                    if tds[7].div:tds7 = tds[7].div["title"]
                    item = (tds0,tds[1].get_text(),tds[2].get_text(),
                             tds[3].get_text().strip(),tds[4].get_text(),
                             tds[5].get_text(),tds6,tds7,tds[8].get_text(),
                             tds[9].get_text())
                    items.append(item)
            except TypeError as e:
                print('get_html_td TypeError:' + e.__str__())
                continue
        #write_excel(items)
    except request.URLError as e:
        print('get_html Error:'+e.reason)
        html = None
        if num_retries>0:
            if hasattr(e,'code') and 500<=e.code<600:
                # recursively retry 5xx HTTP errors
                return get_html(url,num_retries-1)
if __name__=='__main__':
    url = 'http://www.xicidaili.com/nt/%s'
    get_htmls(url,1)