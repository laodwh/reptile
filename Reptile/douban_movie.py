#!/usr/bin/python
# -*- coding: utf-8 -*-
# http://scikit-learn.org/stable/modules/feature_extraction.html#tfidf-term-weighting


import http.cookiejar  # import cookielib
import json
import os
import random
import urllib
import requests
import time
import urllib

import urllib.request
from http import cookiejar

import bs4

from config_info import *
from bs4 import BeautifulSoup as BS
import pymysql
import lxml.html
import re

etree = lxml.html.etree
import gzip

import csv
import os
import random
import requests
import re
from pymongo import MongoClient
from requests import RequestException
from urllib.parse import urlencode

# 浏览器请求头
agents = [
    # Firefox
    "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:34.0) Gecko/20100101 Firefox/34.0",
    "Mozilla/5.0 (X11; U; Linux x86_64; zh-CN; rv:1.9.2.10) Gecko/20100922 Ubuntu/10.10 (maverick) Firefox/3.6.10",
    # chrome
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.71 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11",
    "Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US) AppleWebKit/534.16 (KHTML, like Gecko) Chrome/10.0.648.133 Safari/534.16",
    # UC浏览器
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.122 UBrowser/4.0.3214.0 Safari/537.36",
    # IPhone
    "Mozilla/5.0 (iPhone; U; CPU iPhone OS 4_3_3 like Mac OS X; en-us) AppleWebKit/533.17.9 (KHTML, like Gecko) Version/5.0.2 Mobile/8J2 Safari/6533.18.5",
    # IPod
    "Mozilla/5.0 (iPod; U; CPU iPhone OS 4_3_3 like Mac OS X; en-us) AppleWebKit/533.17.9 (KHTML, like Gecko) Version/5.0.2 Mobile/8J2 Safari/6533.18.5",
    # IPAD
    "Mozilla/5.0 (iPad; U; CPU OS 4_2_1 like Mac OS X; zh-cn) AppleWebKit/533.17.9 (KHTML, like Gecko) Version/5.0.2 Mobile/8C148 Safari/6533.18.5",
    "Mozilla/5.0 (iPad; U; CPU OS 4_3_3 like Mac OS X; en-us) AppleWebKit/533.17.9 (KHTML, like Gecko) Version/5.0.2 Mobile/8J2 Safari/6533.18.5",
    # Android
    "Mozilla/5.0 (Linux; U; Android 2.2.1; zh-cn; HTC_Wildfire_A3333 Build/FRG83D) AppleWebKit/533.1 (KHTML, like Gecko) Version/4.0 Mobile Safari/533.1",
    "Mozilla/5.0 (Linux; U; Android 2.3.7; en-us; Nexus One Build/FRF91) AppleWebKit/533.1 (KHTML, like Gecko) Version/4.0 Mobile Safari/533.1"
]

# 使用代理防止被封
proxies = {

}

headers = {
    # 随机取请求头
    "User-Agent": random.choice(agents),
    "Origin": "https://movie.douban.com",
    "Referer": "https://movie.douban.com"
}

# 用户名：root 密码：root 数据库名：python_db
db = pymysql.connect(ip, user, password, table, charset="utf8")
cursor = db.cursor()

conn = MongoClient('139.199.119.245', 27017)
db_mongo = conn.mydb  # 连接mydb数据库，没有则自动创建
actor_info = db_mongo.actor_info  # 使用actor_info集合，没有则自动创建


def ungzip(data):
    try:
        data = gzip.decompress(data)
    except:
        pass
    return data


# 将所有热门电影存入数据库 (
# 媒资信息<片名,导演,编剧,主演,类型,制片国家,语言,上映时间,片场,别名,豆瓣评分,评价人数>
# 媒资演员<演员姓名,性别,星座,出生日期,置业,演员图片地址,>
# 媒资影评<评论人,标题,详细内容,评论时间>
# )


# 解析html页面 soup
def getSoup(url, cookies):
    # 增加cookie
    headers = {
        # 随机取请求头
        "User-Agent": random.choice(agents),
        "Origin": "https://movie.douban.com",
        "Referer": "https://movie.douban.com"
    }
    oo = requests.get(url, headers=headers, cookies=cookies)
    soup = BS(oo.text, 'html.parser')

    # sleep for several secs
    randint_data = random.randint(7, 20)
    if randint_data < 7:
        randint_data = 7
    time.sleep(randint_data)
    return soup


# 获取文件后缀名
def get_file_extension(file):
    return os.path.splitext(file)[1]


# 创建文件目录,并返回该目录
def mkdir(path):
    # 去除左右两边的空格
    path = path.strip()
    # 去除尾部 \符号
    path = path.rstrip("\\")

    if not os.path.exists(path):
        os.makedirs(path)

    return path


# 读取文件内容 url->欲抓取的文件url
def get_file(url):
    try:
        cj = http.cookiejar.LWPCookieJar()

        opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))

        urllib.request.install_opener(opener)

        req = urllib.request.Request(url)
        operate = opener.open(req)
        data = operate.read()
        return data
    except Exception:
        print("data error:", repr(Exception));
        return None


# 文件保存到本地, path->文件地址
def save_file(path, file_name, data):
    if data == None:
        print("data error:", repr(Exception));
        return

    mkdir(path)
    if (not path.endswith("/")):
        path = path + "/"
    file = open(path + file_name, "wb")
    file.write(data)
    file.flush()
    file.close()


# 取字符串中两个符号之间的东东
def txt_wrap_by(start_str, end, html):
    start = html.find(start_str)
    if start >= 0:
        start += len(start_str)
        end = html.find(end, start)
        if end >= 0:
            return html[start:end].strip()


def getReviewInPage(reviewList_soup):
    tmp = reviewList_soup.find_all('div', class_='mod-bd')
    reviewList = tmp[0].find_all('div', attrs={'data-cid': True})  # 短评列表
    review_s = []
    for nReview in range(len(reviewList)):
        print('           --- Review', nReview + 1)
        temp_List = {}

        # 影评唯一标识
        rid = reviewList[nReview]['data-cid'].strip()
        temp_List["review_id"] = rid;

        # 影评标题
        temp_List["review_title"] = "";

        # 影评内容
        temp_List["review_text"] = "";
        if not reviewList[nReview].find('span', class_='short') is None:
            temp_List["review_text"] = reviewList[nReview].find('span', class_='short').text.strip().replace("'", "");

        # 影评时间
        temp_List["review_time"] = "";
        if not reviewList[nReview].find('span', class_='comment-time') is None:
            temp_List["review_time"] = reviewList[nReview].find('span', class_='comment-time').text.strip().replace("'", "");

        # 状态值 0:待处理 1:已处理
        temp_List["status"] = "0";

        # 创建时间
        temp_List['create_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime());

        # 有用个数
        temp_List['up_num'] = "0";
        if not reviewList[nReview].find('span', class_='votes') is None:
            temp_List['up_num'] = reviewList[nReview].find('span', class_='votes').text.strip().replace("'", "");

        # 回应数量
        temp_List['reply_num'] = "0";

        review_s.append(temp_List)
    return review_s


# 登录返回cookie
def login(url, user, password):
    # 登录
    req_headers = {
        'User-Agent': "Mozilla/5.0 (X11; U; Linux x86_64; zh-CN; rv:1.9.2.10) Gecko/"
                      "20100922 Ubuntu/10.10 (maverick) Firefox/3.6.10"
    }

    req_data = {
        'ck': '',
        'ticket': '',
        'name': user,
        'password': password,
        'remember': False
    }

    # 使用Session post数据
    session = requests.Session()
    resp = session.post(url, data=req_data, headers=req_headers)
    print(resp.cookies.get_dict())  # cookies内容
    temp = session.cookies;
    session.close();  # 避免没必要的开销
    return temp;


# 拼接指定格式


def getLatersList(soup_laters):
    infop_ = {}
    infoc_ = []

    items = soup_laters.find('div', id="showing-soon").find_all('div', class_="item");
    for num in range(len(items)):
        temp_ = items[num]

        info = {}

        thumb = temp_.find_all('a', class_='thumb')[0]['href']
        id = thumb.strip().replace("https://movie.douban.com/subject/", "").replace("/", "");

        src = temp_.find('img')['src']
        if not src is None:
            if src != "":
                save_file(image_path, id + '.jpg', get_file(src.replace("webp", "jpg")));
        img = id + '.jpg'

        intro = temp_.find('div', class_="intro")

        movieName = intro.find_all('h3')[0].text.strip()

        date_ = intro.find_all('li')[0].text.strip()
        type_ = intro.find_all('li')[1].text.strip()
        address = intro.find_all('li')[2].text.strip()
        look_ = intro.find_all('li')[3].text.strip()

        info["id"] = id
        info["img"] = img
        info["movieName"] = movieName
        info["date"] = date_
        info["type"] = type_
        info["address"] = address
        info["look_"] = look_
        infoc_.append(info)
    infop_["id"] = "beijing"
    infop_["data"] = infoc_
    return infop_


def getNowplayingList(soup_nowplayings):
    infop_ = {}
    now_ = []
    up_ = []

    # nowplaying 正在上映的
    items = soup_nowplayings.find('div', id="nowplaying").find('ul', class_="lists").find_all("li", class_="list-item");
    for num in range(len(items)):
        now = {}
        temp_ = items[num]
        id = temp_['id']
        src = temp_.find('img')['src']
        if not src is None:
            if src != "":
                save_file(image_path, id + '.jpg', get_file(src.replace("webp", "jpg")));
        img = id + '.jpg'

        now["id"] = id
        now["title"] = temp_['data-title']
        now["score"] = temp_.find('li', class_="srating").text.strip()
        now["star"] = temp_['data-star']
        now["release"] = temp_['data-release']
        now["duration"] = temp_['data-duration']
        now["region"] = temp_['data-region']
        now["director"] = temp_['data-director']
        now["actors"] = temp_['data-actors']
        now["category"] = temp_['data-category']
        now["enough"] = temp_['data-enough']
        now["showed"] = temp_['data-showed']
        now["votecount"] = temp_['data-votecount']
        now["subject"] = temp_['data-subject']
        now["img"] = img
        now_.append(now)

    # upcoming 即将上映的
    items_up = soup_nowplayings.find('div', id="upcoming").find('ul', class_="lists").find_all("li", class_="list-item");
    for num in range(len(items_up)):
        now = {}
        temp_ = items_up[num]

        src = temp_.find('img')['src']
        if not src is None:
            if src != "":
                save_file(image_path, id + '.jpg', get_file(src.replace("webp", "jpg")));
        img = temp_['id'] + '.jpg'

        now["id"] = temp_['id']
        now["title"] = temp_['data-title']
        now["wish"] = temp_['data-wish']
        now["duration"] = temp_['data-duration']
        now["region"] = temp_['data-region']
        now["director"] = temp_['data-director']
        now["actors"] = temp_['data-actors']
        now["category"] = temp_['data-category']
        now["enough"] = temp_['data-enough']
        now["subject"] = temp_['data-subject']
        now["release-date"] = temp_.find('li', class_="release-date").text.strip()

        now["img"] = img
        up_.append(now)
    infop_["id"] = "beijing"
    infop_["nowplaying"] = now_
    infop_["upcoming"] = up_
    return infop_


def getHotList(soup_hots):
    infop_ = {}
    infoc_ = []

    hots = soup_hots['subjects']
    for num in range(len(hots)):
        info = {}

        temp = hots[num]

        id = temp['id']

        src = temp['cover']
        if not src is None:
            if src != "":
                save_file(image_path, id + '.jpg', get_file(src.replace("webp", "jpg")));
        img = id + '.jpg'

        info['id'] = id
        info['rate'] = temp['rate']
        info['cover_x'] = temp['cover_x']
        info['title'] = temp['title']
        info['url'] = temp['url']
        info['playable'] = temp['playable']
        info['cover'] = temp['cover']
        info['id'] = temp['id']
        info['cover_y'] = temp['cover_y']
        info['is_new'] = temp['is_new']
        info['img'] = img
        infoc_.append(info)
    infop_["id"] = "hot"
    infop_["data"] = infoc_
    return infop_


def getNewList(soup_nowplayings):
    infop_ = {}
    infoc_ = []

    hots = soup_nowplayings['subjects']
    for num in range(len(hots)):
        info = {}

        temp = hots[num]

        id = temp['id']

        src = temp['cover']
        if not src is None:
            if src != "":
                save_file(image_path, id + '.jpg', get_file(src.replace("webp", "jpg")));
        img = id + '.jpg'

        info['id'] = id
        info['rate'] = temp['rate']
        info['cover_x'] = temp['cover_x']
        info['title'] = temp['title']
        info['url'] = temp['url']
        info['playable'] = temp['playable']
        info['cover'] = temp['cover']
        info['id'] = temp['id']
        info['cover_y'] = temp['cover_y']
        info['is_new'] = temp['is_new']
        info['img'] = img
        infoc_.append(info)
    infop_["id"] = "news"
    infop_["data"] = infoc_
    return infop_


def main():
    # login to douban 报错会等待,然后重新登录获取
    cookie = login('https://accounts.douban.com/j/mobile/login/basic', db_username, db_password)

    try:
        print('___start___')
        laters = "https://movie.douban.com/cinema/later/beijing/"
        nowplayings = "https://movie.douban.com/cinema/nowplaying/beijing/"

        # 上面会出现 载入中...
        # hots = "https://movie.douban.com/explore#!type=movie&tag=%E7%83%AD%E9%97%A8&sort=time&page_limit=20&page_start=0"
        hots = "https://movie.douban.com/j/search_subjects?type=movie&tag=%E7%83%AD%E9%97%A8&sort=time&page_limit=20&page_start=0"

        #news = "https://movie.douban.com/explore#!type=movie&tag=%E6%9C%80%E6%96%B0&page_limit=20&page_start=0"
        news = "https://movie.douban.com/j/search_subjects?type=movie&tag=%E6%9C%80%E6%96%B0&page_limit=20&page_start=0"

        print("laters")
        # soup_laters = getSoup(laters, cookie)
        # data_laters = getLatersList(soup_laters)
        # actor_info.laters.insert(data_laters)

        print("nowplayings")
        # soup_nowplayings = getSoup(nowplayings, cookie)
        # data_nowplayings = getNowplayingList(soup_nowplayings)
        # actor_info.nowplayings.insert(data_nowplayings)

        print("hots")
        #soup_hots = requests.get(url=hots, headers=headers, cookies=cookie).json()
        #data_hots = getHotList(soup_hots)
        #actor_info.hots.insert(data_hots)

        print("news")
        #soup_news = requests.get(url=news, headers=headers, cookies=cookie).json()
        #data_news = getNewList(soup_news)
        #actor_info.news.insert(data_news)

        print('___end___')
    except Exception as e:
        pass
        print(e);
        print('执行出现异常,等待下次执行! 403  403  403 bye!');
        time.sleep(3600)
        main()  # 重新执行


if __name__ == "__main__":
    main()
