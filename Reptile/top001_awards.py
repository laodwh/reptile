#!/usr/bin/python
# -*- coding: utf-8 -*-
# http://scikit-learn.org/stable/modules/feature_extraction.html#tfidf-term-weighting
import datetime
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
import simplejson as simplejson

from config_info import *
from bs4 import BeautifulSoup as BS
import pymysql
import lxml.html
import re

etree = lxml.html.etree
import gzip
import logging
import csv
import os
import random
import requests
import re
from pymongo import MongoClient
from requests import RequestException
from urllib.parse import urlencode

LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"    # 日志格式化输出
DATE_FORMAT = "%m/%d/%Y %H:%M:%S %p"                        # 日期格式
fp = logging.FileHandler('log.txt', encoding='utf-8')
fs = logging.StreamHandler()
logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT, datefmt=DATE_FORMAT, handlers=[fp, fs])    # 调用

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


# 解析html页面 soup
def getSoup(url, cookies):
    # 拼接请求头
    headers = {
        # 随机取请求头
        "User-Agent": random.choice(agents),
        "Origin": "https://movie.douban.com",
        "Referer": "https://movie.douban.com"
    }

    # 增加cookie
    oo = requests.get(url, headers=headers, cookies=cookies)
    soup = BS(oo.text, 'html.parser')

    # sleep for several secs
    randint_data = random.randint(5, 10)
    if randint_data < 5:
        randint_data = 5
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
        logging.debug("data error:", repr(Exception));
        return None


# 文件保存到本地, path->文件地址
def save_file(path, file_name, data):
    if data == None:
        logging.debug("data error:", repr(Exception));
        return

    mkdir(path)
    if (not path.endswith("/")):
        path = path + "/"
    file = open(path + file_name, "wb")
    file.write(data)
    file.flush()
    file.close()


# 取字符串中两个符号之间的
def txt_wrap_by(start_str, end, html):
    start = html.find(start_str)
    if start >= 0:
        start += len(start_str)
        end = html.find(end, start)
        if end >= 0:
            return html[start:end].strip()


# 电影详细系信息
def getMoveDetail(moveDetail_soup, id, review_url):
    # parent = moveDetail_soup.find('div',attrs={'id':'content'})
    movie = {}

    divInfo = moveDetail_soup.find('div', id='info').text;

    movie['imdb'] = '';
    imdb_ = txt_wrap_by('IMDb链接:', '\n', divInfo);
    if not imdb_ is None:
        movie['imdb'] = txt_wrap_by('IMDb链接:', '\n', divInfo);

    movie['type'] = type;

    # 影片名称
    name_ = moveDetail_soup.find('span', property="v:itemreviewed");
    movie['name'] = '';
    if not name_ is None:
        movie['name'] = moveDetail_soup.find('span', property="v:itemreviewed").text.strip().replace("'", "");

    # 上映年
    release_year_ = moveDetail_soup.find('span', class_='year');
    movie['release_year'] = '';
    if not release_year_ is None:
        year = str(moveDetail_soup.find('span', class_='year').text).strip().replace("(", "").replace(")", "");
        movie['release_year'] = year;
    if release_year_ is None:
        movie['release_year'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime());  # 后续通过sql 更新为null

    # 导演
    movie['director'] = '';
    director_ = moveDetail_soup.find_all('a', rel="v:directedBy");
    if not director_ is None:
        directors = moveDetail_soup.find_all('a', rel="v:directedBy");
        for director in directors:
            movie['director'] += director.text.replace("'", "") + " ";

    # 编剧
    movie['author'] = '';
    author_ = txt_wrap_by('编剧:', '\n', divInfo);
    if not author_ is None:
        movie['author'] = txt_wrap_by('编剧:', '\n', divInfo);

    # 演员
    movie['actor'] = '';
    actor_ = moveDetail_soup.find_all('a', rel="v:starring");
    if not actor_ is None:
        stars = moveDetail_soup.find_all('a', rel="v:starring");
        for star in stars:
            movie['actor'] += star.text.replace("'", "") + " ";

    # 类型
    movie['genre'] = ''
    genre_ = moveDetail_soup.find_all('span', property="v:genre");
    if not genre_ is None:
        categorys = moveDetail_soup.find_all('span', property="v:genre")
        for category in categorys:
            movie['genre'] += category.text + " ";

    # 地区
    movie['location'] = '';
    location_ = txt_wrap_by('制片国家/地区:', '\n', divInfo);
    if not location_ is None:
        movie['location'] = txt_wrap_by('制片国家/地区:', '\n', divInfo);

    # 语言
    movie['language'] = '';
    language_ = txt_wrap_by('语言:', '\n', divInfo);
    if not language_ is None:
        movie['language'] = txt_wrap_by('语言:', '\n', divInfo);

    # 发行时间
    date_published_ = moveDetail_soup.find('span', property="v:initialReleaseDate");
    movie['date_published'] = '';
    if not date_published_ is None:
        release = moveDetail_soup.find('span', property="v:initialReleaseDate").text.strip();
        movie['date_published'] = re.sub(u"\\(.*?\\)|\\{.*?}|\\[.*?]", "", release);

    if date_published_ is None:
        movie['date_published'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime());

    # 单集片长
    movie['duration'] = '';
    duration_ = txt_wrap_by('片长:', '\n', divInfo);
    if not duration_ is None:
        movie['duration'] = txt_wrap_by('片长:', '\n', divInfo);

    # 别名
    movie['alias'] = '';
    alias_ = txt_wrap_by('又名:', '\n', divInfo);
    if not alias_ is None:
        movie['alias'] = txt_wrap_by('又名:', '\n', divInfo);

    # 评分
    rating_value_ = moveDetail_soup.find('strong', property="v:average");
    movie['rating_value'] = '';
    if not rating_value_ is None:
        movie['rating_value'] = moveDetail_soup.find('strong', property="v:average").text.strip().replace("'", "");

    # 影片简介
    description_ = moveDetail_soup.find('span', property='v:summary');
    movie['description'] = '';
    if not description_ is None:
        movie['description'] = moveDetail_soup.find('span', property='v:summary').text.strip().replace("'", "");

    # 集数
    movie['episode'] = '';
    episode_ = txt_wrap_by('集数:', '\n', divInfo);
    if not episode_ is None:
        movie['episode'] = txt_wrap_by('集数:', '\n', divInfo);

    # imdb的id
    movie['origin_movie_id'] = id;

    # 处理状态 0:未抓取评论 1:已抓取评论
    movie['status'] = "0";

    # 创建时间
    movie['create_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime());

    # 影片海报下载地址
    downPath = moveDetail_soup.find('img', rel="v:image");
    if not downPath is None:
        link = downPath.get('src')
        save_file(image_path, id + '.jpg', get_file(link.replace("webp", "jpg")));
        movie['image'] = id + '.jpg';
    else:
        movie['image'] = "";

    return movie


# 影评
def getReviewContent(review_soup):
    review = ''
    tmp = review_soup.find_all('div', id='link-report')
    review_split = tmp[0].find_all('p')
    if review_split != []:
        for i in range(len(review_split)):
            if review_split[i].text.strip() != '':
                if i != len(review_split) - 1:
                    review = review + '    ' + review_split[i].text.strip() + '<br>'  # 拼接影评的多个段落
                else:
                    review = review + '    ' + review_split[i].text.strip() + '<br>'
    else:
        review_ = tmp[0].contents[1].contents
        review = ' '.join(str(i) for i in review_)
    return review


# 短评
def getReviewInPage(reviewList_soup):
    tmp = reviewList_soup.find_all('div', class_='mod-bd')
    reviewList = tmp[0].find_all('div', attrs={'data-cid': True})  # 短评列表
    review_s = []
    for nReview in range(len(reviewList)):
        logging.debug('           --- Review', nReview + 1)
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


# 验证码
def _get_captcha_info(url, user, password):
    captcha_info = dict()
    response = requests.get(url, headers=headers)
    html = '';
    if response.status_code == 200:
        html = response.text
    # 验证码url
    pat = re.compile(r'<img id="captcha_image" src="(.*?)" alt="captcha" class="captcha_image"/>', re.S)
    response_1 = re.search(pat, html)
    if response_1:
        captcha_url = response_1.group(1)
        logging.debug("验证码图片：{}".format(captcha_url))
        captcha_solution = input('验证码:')
        captcha_info['captcha-solution'] = captcha_solution
    # 验证码ID
    pat1 = re.compile(r'<input type="hidden" name="captcha-id" value="(.*?)"/>', re.S)
    response_2 = re.search(pat1, html)
    if response_2:
        captcha_id = response_2.group(1)
        captcha_info['captcha-id'] = captcha_id
    return captcha_info


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
    logging.debug(resp.cookies.get_dict())  # cookies内容
    temp = session.cookies;
    session.close();  # 避免没必要的开销
    return temp;


# 演员
def getActorList(actor_soup, cookie):
    actorList = actor_soup.find_all('li', class_='celebrity')  # 演员列表

    actor_s = []
    for na in range(len(actorList)):
        aHref = actorList[na].find_all('a')[0]['href']  # 没做异常处理,按理没问题

        if aHref is None:
            continue

        actorInfo_ = {}

        # 演员名称
        actorName = actorList[na].find('a', class_='name').text;

        # 说明有数据,访问获取演员详细信息
        actor_single__soup = getSoup(aHref, cookie)

        # 演员信息
        actorInfo = actor_single__soup.find('div', class_='info').text.strip().replace("\n", "").replace(' ', '');

        actorInfo_["name"] = actorName;
        actorInfo_["info"] = actorInfo;

        actor_s.append(actorInfo_);
        logging.debug('----' + actorName + "-&&&-" + actorInfo)
    return actor_s;


def getAwardsList(awards_soup, cookie, movie_id):

    if awards_soup is None:
        logging.error("movie_id ---> 443")
        raise Exception("get error !!!")

    actor_s = []
    content = awards_soup.find('div', id='content')

    if content is None:
        return actor_s

    title = content.find_all("h1")[0].text
    logging.info("---" + title + "---start:" + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

    awardsList = content.find_all('div', class_='awards')
    if awardsList is None or len(awardsList) == 0:
        return actor_s

    award_return = {}
    award_return_list =[]
    for na in range(len(awardsList)):
        award_ = {}

        # 奖名
        awardTitle = awardsList[na].find('div', class_="hd")

        href = awardTitle.find_all('a')[0]['href']

        awardTitle_ = awardTitle.text.strip().replace("\n", "").replace(' ', '')
        logging.info("title:" + awardTitle_+",href:"+href)

        award_c_list = [];
        awardList = awardsList[na].find_all('ul', class_='award')
        for na in range(len(awardList)):
            award_person_ = {}
            prize = awardList[na].find_all('li')[0].text
            name = awardList[na].find_all('li')[1].text

            award_person_["prize"] = prize;
            award_person_["name"] = name;
            logging.info("prize:" + prize + ",name:" + name)

    #拼接字符串,存mongo
            award_c_list.append(award_person_)
        award_["href"] = href
        award_["title"] = awardTitle_
        award_["awards"] = award_c_list
        award_return_list.append(award_)
    award_return["movieId"] = movie_id;
    award_return["detail"] = award_return_list;
    actor_s.append(award_return);
    logging.info("---" + title + "---end:" + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    return actor_s

def main():
    # mysql 用户名：root 密码：root 数据库名：python_db
    db = pymysql.connect(ip, user, password, table, charset="utf8")
    cursor = db.cursor()

    # mongo
    conn = MongoClient('139.199.119.245', 27017)
    db_mongo = conn.mydb  # 连接mydb数据库，没有则自动创建
    actor_info = db_mongo.actor_info  # 没有则自动创建

    # login to douban 报错会等待,然后重新登录获取
    cookie = login('https://accounts.douban.com/j/mobile/login/basic', db_username, db_password)

    try:
        logging.debug('___start___')
        sql = "SELECT a.origin_movie_id FROM tb_douban_movie a WHERE a.`status_awards` = '%s'" % (0)
        logging.debug('sql:' + sql)
        cursor.execute(sql)
        rs = cursor.fetchall()
        for data_ in rs:
            # print data_[0],'  -  ', data_[1]
            movie_id = data_[0];
            movie_url = "https://movie.douban.com/subject/" + movie_id + "/awards/"

            logging.debug('---movie-->' + movie_id + "---url--->" + movie_url)

            # 请求url,获取原始html页面文本
            awards_soup = getSoup(movie_url, cookie)

            # 解析文本
            awards_s = getAwardsList(awards_soup, cookie, movie_id)

            # 输出插入的所有文档对应的 _id 值
            x = actor_info.douban_award.insert_many(awards_s)

            # 存入mongo数据库,返回id
            #logging.debug(x.inserted_ids)

            # 更新为已处理
            sql = "update tb_douban_movie set status_awards =1  WHERE origin_movie_id = '%s'" % (movie_id)
            cursor.execute(sql)
            db.commit()
        logging.debug('___end___')
    except Exception as e:
        pass
        logging.error(e);
        logging.error('执行出现异常,等待下次执行! 403  403  403 bye!');
        time.sleep(3600)
        main()  # 重新执行


if __name__ == "__main__":
    main()