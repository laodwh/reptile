#!/usr/bin/python
# -*- coding: utf-8 -*-
# http://scikit-learn.org/stable/modules/feature_extraction.html#tfidf-term-weighting


import http.cookiejar  # import cookielib
import os
import io
import random
import urllib
import uuid
import bs4
import datetime
import requests
import sys
import time
import re
from config_info import *
from bs4 import BeautifulSoup as BS
import pymysql
import lxml.html
import re

etree = lxml.html.etree

import gzip

# 浏览器代理
agents = [
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:17.0; Baiduspider-ads) Gecko/17.0 Firefox/17.0",
    "Mozilla/5.0 (Linux; U; Android 2.3.6; en-us; Nexus S Build/GRK39F) AppleWebKit/533.1 (KHTML, like Gecko) Version/4.0 Mobile Safari/533.1",
    "Avant Browser/1.2.789rel1 (http://www.avantbrowser.com)",
    "Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US) AppleWebKit/532.5 (KHTML, like Gecko) Chrome/4.0.249.0 Safari/532.5",
    "Mozilla/5.0 (Windows; U; Windows NT 5.2; en-US) AppleWebKit/532.9 (KHTML, like Gecko) Chrome/5.0.310.0 Safari/532.9",
    "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US) AppleWebKit/534.7 (KHTML, like Gecko) Chrome/7.0.514.0 Safari/534.7",
    "Mozilla/5.0 (Windows; U; Windows NT 6.0; en-US) AppleWebKit/534.14 (KHTML, like Gecko) Chrome/9.0.601.0 Safari/534.14",
    "Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US) AppleWebKit/534.14 (KHTML, like Gecko) Chrome/10.0.601.0 Safari/534.14",
    "Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US) AppleWebKit/534.20 (KHTML, like Gecko) Chrome/11.0.672.2 Safari/534.20",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/534.27 (KHTML, like Gecko) Chrome/12.0.712.0 Safari/534.27",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.1 (KHTML, like Gecko) Chrome/13.0.782.24 Safari/535.1",
    "Mozilla/5.0 (Windows NT 6.0) AppleWebKit/535.2 (KHTML, like Gecko) Chrome/15.0.874.120 Safari/535.2",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.7 (KHTML, like Gecko) Chrome/16.0.912.36 Safari/535.7",
    "Mozilla/5.0 (Windows; U; Windows NT 6.0 x64; en-US; rv:1.9pre) Gecko/2008072421 Minefield/3.0.2pre",
    "Mozilla/5.0 (Windows; U; Windows NT 5.1; zh-CN; rv:1.9b4) Gecko/2008030317 Firefox/3.0b4",
    "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.9.0.10) Gecko/2009042316 Firefox/3.0.10",
    "Mozilla/5.0 (Windows; U; Windows NT 6.0; en-GB; rv:1.9.0.11) Gecko/2009060215 Firefox/3.0.11 (.NET CLR 3.5.30729)",
    "Mozilla/5.0 (Windows; U; Windows NT 6.0; en-US; rv:1.9.1.6) Gecko/20091201 Firefox/3.5.6 GTB5",
    "Mozilla/5.0 (Windows; U; Windows NT 5.1; tr; rv:1.9.2.8) Gecko/20100722 Firefox/3.6.8 ( .NET CLR 3.5.30729; .NET4.0E)",
    "Mozilla/5.0 (Windows; U; MSIE 6.0; Windows NT 5.1; SV1; .NET CLR 2.0.50727; BIDUBrowser 7.6)",
    "Mozilla/5.0 (Windows NT 6.3; WOW64; Trident/7.0; rv:11.0) like Gecko",
    "Mozilla/5.0 (Windows NT 6.3; WOW64; rv:46.0) Gecko/20100101 Firefox/46.0",
    "Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.99 Safari/537.36",
    "Mozilla/5.0 (Windows NT 6.3; Win64; x64; Trident/7.0; Touch; LCJB; rv:11.0) like Gecko",
    "Mozilla/5.0 (Windows NT 6.1; rv:2.0.1) Gecko/20100101 Firefox/4.0.1",
    "Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:2.0.1) Gecko/20100101 Firefox/4.0.1",
    "Mozilla/5.0 (Windows NT 5.1; rv:5.0) Gecko/20100101 Firefox/5.0",
    "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:6.0a2) Gecko/20110622 Firefox/6.0a2",
    "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:7.0.1) Gecko/20100101 Firefox/7.0.1",
    "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:2.0b4pre) Gecko/20100815 Minefield/4.0b4pre",
    "Mozilla/4.0 (compatible; MSIE 5.5; Windows NT 5.0 )",
    "Mozilla/4.0 (compatible; MSIE 5.5; Windows 98; Win 9x 4.90)",
    "Mozilla/5.0 (Windows; U; Windows XP) Gecko MultiZilla/1.6.1.0a",
    "Mozilla/2.02E (Win95; U)",
    "Mozilla/3.01Gold (Win95; I)",
    "Mozilla/4.8 [en] (Windows NT 5.1; U)",
    "Mozilla/5.0 (Windows; U; Win98; en-US; rv:1.4) Gecko Netscape/7.1 (ax)",
    "HTC_Dream Mozilla/5.0 (Linux; U; Android 1.5; en-ca; Build/CUPCAKE) AppleWebKit/528.5  (KHTML, like Gecko) Version/3.1.2 Mobile Safari/525.20.1",
    "Mozilla/5.0 (hp-tablet; Linux; hpwOS/3.0.2; U; de-DE) AppleWebKit/534.6 (KHTML, like Gecko) wOSBrowser/234.40.1 Safari/534.6 TouchPad/1.0",
    "Mozilla/5.0 (Linux; U; Android 1.5; en-us; sdk Build/CUPCAKE) AppleWebkit/528.5  (KHTML, like Gecko) Version/3.1.2 Mobile Safari/525.20.1",
    "Mozilla/5.0 (Linux; U; Android 2.1; en-us; Nexus One Build/ERD62) AppleWebKit/530.17 (KHTML, like Gecko) Version/4.0 Mobile Safari/530.17",
    "Mozilla/5.0 (Linux; U; Android 2.2; en-us; Nexus One Build/FRF91) AppleWebKit/533.1 (KHTML, like Gecko) Version/4.0 Mobile Safari/533.1",
    "Mozilla/5.0 (Linux; U; Android 1.5; en-us; htc_bahamas Build/CRB17) AppleWebKit/528.5  (KHTML, like Gecko) Version/3.1.2 Mobile Safari/525.20.1",
    "Mozilla/5.0 (Linux; U; Android 2.1-update1; de-de; HTC Desire 1.19.161.5 Build/ERE27) AppleWebKit/530.17 (KHTML, like Gecko) Version/4.0 Mobile Safari/530.17",
    "Mozilla/5.0 (Linux; U; Android 2.2; en-us; Sprint APA9292KT Build/FRF91) AppleWebKit/533.1 (KHTML, like Gecko) Version/4.0 Mobile Safari/533.1",
    "Mozilla/5.0 (Linux; U; Android 1.5; de-ch; HTC Hero Build/CUPCAKE) AppleWebKit/528.5  (KHTML, like Gecko) Version/3.1.2 Mobile Safari/525.20.1",
    "Mozilla/5.0 (Linux; U; Android 2.2; en-us; ADR6300 Build/FRF91) AppleWebKit/533.1 (KHTML, like Gecko) Version/4.0 Mobile Safari/533.1",
    "Mozilla/5.0 (Linux; U; Android 2.1; en-us; HTC Legend Build/cupcake) AppleWebKit/530.17 (KHTML, like Gecko) Version/4.0 Mobile Safari/530.17",
    "Mozilla/5.0 (Linux; U; Android 1.5; de-de; HTC Magic Build/PLAT-RC33) AppleWebKit/528.5  (KHTML, like Gecko) Version/3.1.2 Mobile Safari/525.20.1 FirePHP/0.3",
    "Mozilla/5.0 (Linux; U; Android 1.6; en-us; HTC_TATTOO_A3288 Build/DRC79) AppleWebKit/528.5  (KHTML, like Gecko) Version/3.1.2 Mobile Safari/525.20.1",
    "Mozilla/5.0 (Linux; U; Android 1.0; en-us; dream) AppleWebKit/525.10  (KHTML, like Gecko) Version/3.0.4 Mobile Safari/523.12.2",
    "Mozilla/5.0 (Linux; U; Android 1.5; en-us; T-Mobile G1 Build/CRB43) AppleWebKit/528.5  (KHTML, like Gecko) Version/3.1.2 Mobile Safari 525.20.1",
    "Mozilla/5.0 (Linux; U; Android 1.5; en-gb; T-Mobile_G2_Touch Build/CUPCAKE) AppleWebKit/528.5  (KHTML, like Gecko) Version/3.1.2 Mobile Safari/525.20.1",
    "Mozilla/5.0 (Linux; U; Android 2.0; en-us; Droid Build/ESD20) AppleWebKit/530.17 (KHTML, like Gecko) Version/4.0 Mobile Safari/530.17",
    "Mozilla/5.0 (Linux; U; Android 2.2; en-us; Droid Build/FRG22D) AppleWebKit/533.1 (KHTML, like Gecko) Version/4.0 Mobile Safari/533.1",
    "Mozilla/5.0 (Linux; U; Android 2.0; en-us; Milestone Build/ SHOLS_U2_01.03.1) AppleWebKit/530.17 (KHTML, like Gecko) Version/4.0 Mobile Safari/530.17",
    "Mozilla/5.0 (Linux; U; Android 2.0.1; de-de; Milestone Build/SHOLS_U2_01.14.0) AppleWebKit/530.17 (KHTML, like Gecko) Version/4.0 Mobile Safari/530.17",
    "Mozilla/5.0 (Linux; U; Android 3.0; en-us; Xoom Build/HRI39) AppleWebKit/525.10  (KHTML, like Gecko) Version/3.0.4 Mobile Safari/523.12.2",
    "Mozilla/5.0 (Linux; U; Android 0.5; en-us) AppleWebKit/522  (KHTML, like Gecko) Safari/419.3",
    "Mozilla/5.0 (Linux; U; Android 1.1; en-gb; dream) AppleWebKit/525.10  (KHTML, like Gecko) Version/3.0.4 Mobile Safari/523.12.2",
    "Mozilla/5.0 (Linux; U; Android 2.0; en-us; Droid Build/ESD20) AppleWebKit/530.17 (KHTML, like Gecko) Version/4.0 Mobile Safari/530.17",
    "Mozilla/5.0 (Linux; U; Android 2.1; en-us; Nexus One Build/ERD62) AppleWebKit/530.17 (KHTML, like Gecko) Version/4.0 Mobile Safari/530.17",
    "Mozilla/5.0 (Linux; U; Android 2.2; en-us; Sprint APA9292KT Build/FRF91) AppleWebKit/533.1 (KHTML, like Gecko) Version/4.0 Mobile Safari/533.1",
    "Mozilla/5.0 (Linux; U; Android 2.2; en-us; ADR6300 Build/FRF91) AppleWebKit/533.1 (KHTML, like Gecko) Version/4.0 Mobile Safari/533.1",
    "Mozilla/5.0 (Linux; U; Android 2.2; en-ca; GT-P1000M Build/FROYO) AppleWebKit/533.1 (KHTML, like Gecko) Version/4.0 Mobile Safari/533.1",
    "Mozilla/5.0 (Linux; U; Android 3.0.1; fr-fr; A500 Build/HRI66) AppleWebKit/534.13 (KHTML, like Gecko) Version/4.0 Safari/534.13",
    "Mozilla/5.0 (Linux; U; Android 3.0; en-us; Xoom Build/HRI39) AppleWebKit/525.10  (KHTML, like Gecko) Version/3.0.4 Mobile Safari/523.12.2",
    "Mozilla/5.0 (Linux; U; Android 1.6; es-es; SonyEricssonX10i Build/R1FA016) AppleWebKit/528.5  (KHTML, like Gecko) Version/3.1.2 Mobile Safari/525.20.1",
    "Mozilla/5.0 (Linux; U; Android 1.6; en-us; SonyEricssonX10i Build/R1AA056) AppleWebKit/528.5  (KHTML, like Gecko) Version/3.1.2 Mobile Safari/525.20.1"
]

# 使用代理防止被封
proxies = {

}


def ungzip(data):
    try:
        data=gzip.decompress(data)
    except:
        pass
    return data

# 将所有热门电影存入数据库 (
# 媒资信息<片名,导演,编剧,主演,类型,制片国家,语言,上映时间,片场,别名,豆瓣评分,评价人数>
# 媒资演员<演员姓名,性别,星座,出生日期,置业,演员图片地址,>
# 媒资影评<评论人,标题,详细内容,评论时间>
# )

# 解析html页面 soup
def getSoup(page):
    headers = {
        # 随机取请求头
        "User-Agent": random.choice(agents),
        "Origin": "https://movie.douban.com",
        "Referer": "https://movie.douban.com"
    }

    req = urllib.request.urlopen(page)
    req.addheaders = [headers]


    html = req.read().decode('utf-8', errors='ignore')
    soup = BS(html, 'html.parser')
    # sleep for several secs
    randint_data = random.randint(7, 20)
    if randint_data < 7:
        randint_data = 7
    time.sleep(randint_data)
    return soup


# 解析HTML页面 xpath
def getXpath(page):
    headers = {
        # 随机取请求头
        "User-Agent": random.choice(agents),
        "Origin": "https://movie.douban.com",
        "Referer": "https://movie.douban.com"
    }

    req = urllib.request.urlopen(page)
    req.addheaders = [headers]
    html = req.read().decode('utf-8', errors='ignore')
    selector = etree.HTML(html)
    # sleep for several secs
    randint_data = random.randint(1, 20)
    if randint_data < 7:
        randint_data = 7
    time.sleep(randint_data)
    return selector


# 获取当前url的tile,id返回list
def getMoveList(url, headers):
    page_now = requests.get(url=url, headers=headers, proxies=proxies).json();  # 获取json串
    movieInfo = page_now['subjects'];
    idList = [x['id'] for x in movieInfo]  # 影片id
    titleList = [x['title'] for x in movieInfo]  # 标题
    urlList = [x['url'] for x in movieInfo]  # 影片详细信息
    coverList = [x['cover'] for x in movieInfo]  # 影片海报
    movieList = {'title': titleList, 'url': urlList, 'cover': coverList, 'id': idList}
    randint_data = random.randint(1, 20)
    if randint_data < 7:
        randint_data = 7
    time.sleep(randint_data)
    return movieList


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


def GetMiddleStr(content, startStr, endStr):
    if content in startStr and content in endStr:
        startIndex = content.index(startStr)
        if startIndex >= 0:
            startIndex += len(startStr)
        endIndex = content.index(endStr)
        return content[startIndex:endIndex]
    elif startStr =="IMDb链接:":
        return content.split(startStr,2)[1].strip().replace("'", "")
    return '';

#取字符串中两个符号之间的东东
def txt_wrap_by(start_str, end, html):
    start = html.find(start_str)
    if start >= 0:
        start += len(start_str)
        end = html.find(end, start)
        if end >= 0:
            return html[start:end].strip()

def getMoveDetail(moveDetail_soup, id, review_url):
    # parent = moveDetail_soup.find('div',attrs={'id':'content'})
    movie = {}

    divInfo = moveDetail_soup.find('div', id='info').text;

    #movie['imdb'] = txt_wrap_by('IMDb链接:','\n',divInfo);
    movie['imdb'] ='';
    imdb_ = txt_wrap_by('IMDb链接:','\n',divInfo);
    if not imdb_ is None:
        movie['imdb'] = txt_wrap_by('IMDb链接:','\n',divInfo);


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
        movie['release_year'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()); #后续通过sql 更新为null

    # 导演
    movie['director'] = '';
    director_ = moveDetail_soup.find_all('a', rel="v:directedBy");
    if not director_ is None:
        directors = moveDetail_soup.find_all('a', rel="v:directedBy");
        for director in directors:
            movie['director'] += director.text.replace("'", "") + " ";

    # 编剧
    #movie['author'] = GetMiddleStr(divInfo, '编剧:', '主演:').strip().replace("'", "");
    movie['author'] ='';
    author_ = txt_wrap_by('编剧:','\n',divInfo);
    if not author_ is None:
        movie['author'] = txt_wrap_by('编剧:','\n',divInfo);

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
    #movie['location'] = GetMiddleStr(divInfo, '制片国家/地区:', '语言:').strip().replace("'", "");
    movie['location'] ='';
    location_ = txt_wrap_by('制片国家/地区:','\n',divInfo);
    if not location_ is None:
        movie['location'] = txt_wrap_by('制片国家/地区:','\n',divInfo);


    # 语言
    #movie['language'] = GetMiddleStr(divInfo, '语言:', '上映日期:').strip().replace("'", "");
    movie['language'] ='';
    language_ = txt_wrap_by('语言:','\n',divInfo);
    if not language_ is None:
        movie['language'] = txt_wrap_by('语言:','\n',divInfo);

    # 发行时间
    date_published_ = moveDetail_soup.find('span', property="v:initialReleaseDate");
    movie['date_published'] = '';
    if not date_published_ is None:
        release = moveDetail_soup.find('span', property="v:initialReleaseDate").text.strip();
        movie['date_published'] = re.sub(u"\\(.*?\\)|\\{.*?}|\\[.*?]", "", release);

    if date_published_ is None:
        movie['date_published'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime());

    # 单集片长
    # duration_ = moveDetail_soup.find('span', property="v:runtime")
    # if not duration_ is None:
    #     movie['duration'] = moveDetail_soup.find('span', property="v:runtime").text.strip().replace("'", "");
    movie['duration'] = '';
    duration_ = txt_wrap_by('片长:','\n',divInfo);
    if not duration_ is None:
        movie['duration'] = txt_wrap_by('片长:','\n',divInfo);

    # 别名
    # movie['alias'] = "";
    # if "又名:" in divInfo:
    #     movie['alias'] = GetMiddleStr(divInfo, '又名:', 'IMDb链接:').strip();
    movie['alias'] = '';
    alias_ = txt_wrap_by('又名:','\n',divInfo);
    if not alias_ is None:
        movie['alias'] = txt_wrap_by('又名:','\n',divInfo);

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
    # movie['episode'] = "";
    # if "集数:" in divInfo:
    #     movie['episode'] = GetMiddleStr(divInfo, '集数:', '单集片长:').strip();
    movie['episode'] = '';
    episode_ = txt_wrap_by('集数:','\n',divInfo);
    if not episode_ is None:
        movie['episode'] = txt_wrap_by('集数:','\n',divInfo);

    # imdb的id
    movie['origin_movie_id'] = id;

    # 处理状态 0:未抓取评论 1:已抓取评论
    movie['status'] = "0";

    # 创建时间
    movie['create_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime());

    # 影片海报下载地址
    downPath = moveDetail_soup.find('img', rel="v:image");
    if not downPath is None:
        link=downPath.get('src')
        save_file(image_path, id + '.jpg', get_file(link.replace("webp", "jpg")));
        movie['image'] = id + '.jpg';
    else:
        movie['image'] ="";

    return movie



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


def getReviewInPage(reviewList_soup):
    tmp = reviewList_soup.find_all('div', class_='review-list')
    reviewList = tmp[0].find_all('div', attrs={'data-cid': True})  # 影评列表  attrs={'data-cid': True} --> 这里获取所有具有data-cid属性的li标签
    review_s = []
    for nReview in range(len(reviewList)):
        print('           --- Review', nReview + 1)
        temp_List = {}

        # 影评唯一标识
        rid = reviewList[nReview]['data-cid'].strip()
        temp_List["review_id"] = rid;

        # 影评内容
        review_page = 'https://movie.douban.com/review/%s/' % rid
        review_soup = getSoup(review_page)

        review_content = getReviewContent(review_soup)  # 获取影评内容
        temp_List["review_text"] = review_content.strip().replace("'", "");

        # 影评标题
        temp_List["review_title"] = "";
        if not review_soup.find('span', property='v:summary') is None:
            temp_List["review_title"] = review_soup.find('span', property='v:summary').text.strip().replace("'", "");

        # 影评时间
        temp_List["review_time"] = "";
        if not review_soup.find('span', class_='main-meta') is None:
            temp_List["review_time"] = review_soup.find('span', class_='main-meta').text.strip();

        # 对应电影的id

        # 状态值 0:待处理 1:已处理
        temp_List["status"] = "0";

        # 创建时间
        temp_List['create_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime());

        # 有用个数
        temp_List['up_num'] = "0";
        if not review_soup.find('button', class_='useful_count') is None:
            temp_List['up_num'] = review_soup.find('button', class_='useful_count').text.replace("有用", "").strip().replace("'", "");
        if temp_List['up_num'] == '':
            temp_List['up_num'] = "0";

        # 回应数量
        temp_List['reply_num'] = "0";
        if not review_soup.find('button', class_='useless_count') is None:
            temp_List['reply_num'] = review_soup.find('button', class_='useless_count').text.replace("没用", "").strip().replace("'", "");
        if temp_List['reply_num'] == '':
            temp_List['reply_num'] = "0";

        review_s.append(temp_List)
    return review_s


def main():
    nStart = Start;  # 起始页
    nBias = Bias;  # 抓取总的页数

    # 用户名：root 密码：root 数据库名：python_db
    db = pymysql.connect(ip, user, password, table, charset="utf8")
    cursor = db.cursor()

    try:
        # for bias in range(nStart, nStart + nBias):  # (a,b) a为开始数,默认0 b为结束数 不包含当前的数
            # 每次加载20部影片
            # url = base_url + '%d' % (bias * 20);

        headers = {
            "User-Agent": random.choice(agents),
            "Origin": "https://movie.douban.com",
            "Referer": "https://movie.douban.com"
        }

        print('开始处理电影了 1,2,3......');
        #sql = "SELECT origin_movie_id FROM tb_douban_movie WHERE status = '%s' limit 500,50" % (0)
        #sql = "SELECT origin_movie_id FROM tb_douban_movie WHERE status = '%s' ORDER BY origin_movie_id limit 100,50" % (0)
        sql = "SELECT a.origin_movie_id FROM tb_douban_movie a WHERE a.origin_movie_id NOT IN (SELECT origin_movie_id FROM tb_douban_review GROUP BY origin_movie_id) AND a.`status` = '%s' LIMIT 100,50" % (0)
        print('执行的sql是'+sql);
        cursor.execute(sql)
        rs = cursor.fetchall()
        for data_ in rs:
            # print data_[0],'  -  ', data_[1]
            movie_id = data_[0];
            movie_url = "https://movie.douban.com/subject/" + movie_id + "/";

            print('电影-->'+movie_id);

            # 删除已经存在的数据,防止数据
            sql = "delete FROM tb_douban_review WHERE origin_movie_id = '%s'" % (movie_id)
            #sql = "update tb_douban_review set origin_movie_id = '-1' WHERE origin_movie_id = '%s'" % (movie_id)
            cursor.execute(sql)
            db.commit()

            # 获取影评信息
            review_soup = getSoup(movie_url + 'reviews')

            this_page = review_soup.find('span', class_='thispage')  # 查询span
            total_page = 0

            if not this_page:
                source_n = 1
            else:
                total_page = int(this_page['data-total-page'])  # 获取总的评论页数
                source_n = review

            if total_page < source_n and source_n != 1:
                source_n = total_page

            for nPage in range(source_n):
                print('      >>>>>> CurrentPage', int(nPage + 1))
                reviewList_page = movie_url + 'reviews?start=%s' % (nPage * 20)  # 第nPage页
                reviewList_soup = getSoup(reviewList_page)  # 解析网页

                review_s = getReviewInPage(reviewList_soup)  # 获取第nPage页所有影评

                if len(review_s):
                    for review_db in range(len(review_s)):
                        cursor.execute('SET NAMES utf8mb4;')
                        sql = "insert into tb_douban_review(review_id,review_title,review_text,review_time,origin_movie_id,status,create_time,up_num,reply_num) values('" + \
                              review_s[review_db]['review_id'] + "'" + "," + "'" + review_s[review_db]['review_title'] + "'" + "," + "'" + review_s[review_db][
                                  'review_text'] + "'" + "," + "'" + review_s[review_db]['review_time'] + "'" + "," + "'" + movie_id + "'" + "," + "'" + \
                              review_s[review_db]['status'] + "'" + "," + "'" + review_s[review_db]['create_time'] + "'" + "," + "'" + review_s[review_db][
                                  'up_num'] + "'" + "," + "'" + review_s[review_db]['reply_num'] + "')"
                        print('---' + sql + '---');
                        cursor.execute(sql)
                        db.commit()
                else:
                    print('   >>>> empty ');

            # 更新为已处理
            sql = "update tb_douban_movie set status =1  WHERE origin_movie_id = '%s'" % (movie_id)
            cursor.execute(sql)
            db.commit()
    except Exception as e:
        pass
        print(e);
        print('执行出现异常,等待下次执行! 403  403  403 bye!');
        time.sleep(3600)
        main()  # 重新执行


if __name__ == "__main__":
    #main()

    reviewList_soup = getSoup("https://movie.douban.com/subject/26794435/reviews")  # 解析网页
    review_s = getReviewInPage(reviewList_soup)  # 获取第nPage页所有影评
    print('1-----');