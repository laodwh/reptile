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
    print(path + "--> image save success")


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
def getActorList(actor_soup, cookie, movie_id):
    actor_p = []

    list_wrapper = actor_soup.find_all('div', class_='list-wrapper')
    if list_wrapper is None:
        return actor_p

    actor_movie = {}
    # actor_s =[]
    class_ = {}
    for na in range(len(list_wrapper)):
        directors = []

        # 所属分类
        type = list_wrapper[na].find_all('h2')[0].text
        type = re.sub('[^a-zA-Z]', '', type)  # 去除中文

        actorList = list_wrapper[na].find_all('li', class_='celebrity')  # 演员列表

        for na in range(len(actorList)):
            actorInfo_ = {}

            temp_ = actorList[na]

            # 演员id
            ahref = temp_.find_all('a')[0]['href']

            id = ahref.strip().replace("https://movie.douban.com/celebrity/", "").replace("/", "");

            # 演员名称
            actorName = temp_.find('a', class_='name').text;

            # 角色
            role = temp_.find('span', class_='role').text;

            # 图片 规则(id+",jpg")
            style = temp_.contents[1].contents[1].attrs['style']

            # 保存图片
            if not style is None:
                downPath = style.replace("background-image: url(", "").replace(")", "");
                if downPath != "":
                    save_file(image_path, id + '.jpg', get_file(downPath.replace("webp", "jpg")));

            # 代表作
            work_list = []
            works = temp_.find('span', class_='works')
            if not works is None:
                work_c_temp = works.find_all('a')
                for na in range(len(work_c_temp)):
                    work_child = {}
                    wc_href = work_c_temp[na]['href']
                    wc_id = wc_href.strip().replace("https://movie.douban.com/subject/", "").replace("/", "");
                    wc_title = work_c_temp[na]['title']

                    work_child["id"] = wc_id
                    work_child["name"] = wc_title
                    work_list.append(work_child)
            actorInfo_["id"] = id
            actorInfo_["name"] = actorName
            actorInfo_["image"] = id + '.jpg';
            actorInfo_["role"] = role
            actorInfo_["works"] = work_list
            directors.append(actorInfo_)
        class_[type] = directors
        # actor_s.append(class_)

    actor_movie["movie_id"] = movie_id;
    actor_movie["movie_celebrities"] = class_;
    actor_p.append(actor_movie)
    return actor_p


def getCelebrityList(soup, cookie,celebrity_id):
    infos_ = {}
    content = soup.find('div', id="content");

    name = content.find_all('h1')[0].text

    downPath = content.find_all('a', class_='nbg')[0]['href']
    if not downPath is None:
        if downPath != "":
            save_file(image_path, celebrity_id + '.jpg', get_file(downPath.replace("webp", "jpg")));

    pic = str(celebrity_id + '.jpg');

    infos = soup.find('div', class_='info').find_all('li');
    sex = ""
    zodiac = ""
    birthday = ""
    birth_location = ""
    career = ""
    family_member = ""
    imdb_id = ""
    for num in range(len(infos)):
        temp_ = infos[num].text.strip().replace(" ", "").replace("\n", "")
        if "性别" in temp_:
            sex = temp_.replace("性别:", "")
        elif "星座" in temp_:
            zodiac = temp_.replace("星座:", "")
        elif "出生日期" in temp_:
            birthday = temp_.replace("出生日期:", "")
        elif "出生地" in temp_:
            birth_location = temp_.replace("出生地:", "")
        elif "职业" in temp_:
            career = temp_.replace("职业:", "")
        elif "家庭成员" in temp_:
            family_member = temp_.replace("家庭成员:", "")
        elif "imdb编号" in temp_:
            imdb_id = temp_.replace("imdb编号:", "")

    base = {}
    base["sex"] = sex
    base["zodiac"] = zodiac
    base["birthday"] = birthday
    base["birth_location"] = birth_location
    base["career"] = career
    base["family_member"] = family_member
    base["imdb_id"] = imdb_id

    introduction = soup.find('div', id='intro').find('div', class_='bd').text.strip().replace("\n", "");

    infos_["celebrity_id"] = celebrity_id
    infos_["name"] = name
    infos_["pic"] = pic
    infos_["info"] = base
    infos_["introduction"] = introduction
    return infos_


def getPhotosList(soup, cookie,source):
    infop_ = {}
    infos_ = []
    photo = soup.find('div', id="content").find('div', class_="article").find_all('li');
    for num in range(len(photo)):
        info_ = {}
        id = photo[num].find('a')['href'].strip().replace("https://movie.douban.com/celebrity/1002862/photo/", "").replace("/", "");
        src = photo[num].find('img')['src']
        if not src is None:
            if src != "":
                save_file(image_path, id + '.jpg', get_file(src.replace("webp", "jpg")));
        img = id + '.jpg'
        prop = photo[num].find('div', class_='prop').text.strip().replace(" ", "");
        info_['id'] = id
        info_['img'] = img
        info_['prop'] = prop
        infos_.append(info_)

    infop_["id"] = source
    infop_["data"] = infos_
    return infop_


def getAwardsList(soup, cookie,source):
    infop_ = {}
    infos_ = []
    awards = soup.find('div', id="content").find('div', class_="article").find_all('div', class_="awards");
    for num in range(len(awards)):
        years = {}
        year = awards[num].find('div', class_="hd").text
        award_c_list = [];
        awardList = awards[num].find_all('ul', class_='award')
        for na in range(len(awardList)):
            award_person_ = {}
            awars = awardList[na].find_all('li')[0].text.strip()
            prize = awardList[na].find_all('li')[1].text
            subject = awardList[na].find_all('li')[2].text

            award_person_["awars"] = awars;
            award_person_["prize"] = prize;
            award_person_["subject"] = subject;
            award_c_list.append(award_person_)
        years['year'] = year
        years['data'] = award_c_list
        infos_.append(years)
    infop_["id"] = source
    infop_["data"] = infos_
    return infop_


def getMoviesList(soup, cookie,source):
    total_page = 0
    this_page = soup.find('span', class_='count').text.strip().replace("(共", "").replace("条)", "")

    # total_page = round(int(this_page)/20);  # 获取总的评论页数
    total_page = 2  # 暂时只抓一页,后续看着放开上面一行就行
    infop_ = {}
    movies_ = []
    for nPage in range(total_page):
        this_page = 'https://movie.douban.com/celebrity/1002862/movies?start=%s' % (nPage * 10) + '&format=pic&sortby=vote&'  # 第nPage页
        current_soup = getSoup(this_page, cookie)
        article = current_soup.find('div', class_="grid_view")
        if article is None:
            continue

        temp_li = article.contents[3].find_all('li')
        for na in range(len(temp_li)):
            movie = {}

            temp_ = temp_li[na]
            href = temp_.find('a', class_='nbg')['href'];
            id = href.strip().replace("https://movie.douban.com/subject/", "").replace("/", "");

            src = temp_.find('img')['src']
            if not src is None:
                if src != "":
                    save_file(image_path, id + '.jpg', get_file(src.replace("webp", "jpg")));
            img = id + '.jpg'
            title = temp_.find("h6").find("a").text.strip()
            year = temp_.find("h6").find_all("span")[0].text.strip()
            actor = temp_.find("h6").find_all("span")[1].text.strip()

            temp_director = temp_.find("dd").find("dl").find_all("dd")
            director = '';
            star = '';
            if len(temp_director) == 2:
                director = temp_.find("dd").find("dl").find_all("dd")[0].text.strip()
                star = temp_.find("dd").find("dl").find_all("dd")[1].text.strip()
            else:
                star = temp_.find("dd").find("dl").find_all("dd")[0].text.strip()

            movie["id"] = id
            movie["img"] = img
            movie["title"] = title
            movie["year"] = year
            movie["actor"] = actor
            movie["director"] = director
            movie["star"] = star
            movies_.append(movie)
    infop_["id"] = source
    infop_["data"] = movies_
    return infop_


def getPartnersList(soup, cookie,source):
    total_page = 0
    this_page = soup.find('span', class_='count').text.strip().replace("(共", "").replace("条)", "")

    # total_page = round(int(this_page)/20);  # 获取总的评论页数
    total_page = 1  # 暂时只抓一页,后续看着放开上面一行就行
    infop_ = {}
    partners_ = []
    for nPage in range(total_page):
        this_page = 'https://movie.douban.com/celebrity/1002862/partners?start=%s' % (nPage * 10)  # 第nPage页
        current_soup = getSoup(this_page, cookie)
        partner = current_soup.find_all('div', class_="partners item")
        if partner is None:
            continue

        for na in range(len(partner)):
            partner_ = {}

            temp_ = partner[na]
            t_id = temp_.find('div', class_="pic").find('a')['href']
            id = t_id.strip().replace("https://movie.douban.com/celebrity/", "").replace("/", "");

            src = temp_.find('img')['src']
            if not src is None:
                if src != "":
                    save_file(image_path, id + '.jpg', get_file(src.replace("webp", "jpg")));
            img = id + '.jpg'

            info = temp_.find('div', class_='info')

            name = info.find_all('h2')[0].text
            role = info.find("ul").find_all("li")[0].text.strip()

            cooperations = info.find("ul").find_all("li")[1].find_all('a')

            if cooperations is None:
                continue

            cooperations_ = []
            for nc in range(len(cooperations)):
                cooperation = {}

                href = cooperations[nc]["href"]
                name_ = cooperations[nc].text.strip()
                cooperation["href"] = href
                cooperation["name"] = name_

                cooperations_.append(cooperation)

            partner_["id"] = id
            partner_["img"] = img
            partner_["name"] = name
            partner_["role"] = role
            partner_["cooperations"] = cooperations_

            partners_.append(partner_)
    infop_["id"] = source
    infop_["data"] = partners_
    return infop_


def main():
    # login to douban 报错会等待,然后重新登录获取
    cookie = login('https://accounts.douban.com/j/mobile/login/basic', db_username, db_password)

    try:
        print('___start___')
        id = "1002862";
        celebrity = "https://movie.douban.com/celebrity/1002862/"
        photos = "https://movie.douban.com/celebrity/1002862/photos/"
        awards = "https://movie.douban.com/celebrity/1002862/awards/"
        movies = "https://movie.douban.com/celebrity/1002862/movies?sortby=time&format=pic"
        partners = "https://movie.douban.com/celebrity/1002862/partners"

        print("celebrity")
        soup_celebrity = getSoup(celebrity,cookie)
        data_celebrity = getCelebrityList(soup_celebrity,cookie,id)
        actor_info.celebrity.insert(data_celebrity)

        print("photos")
        soup_photos = getSoup(photos,cookie)
        data_photos = getPhotosList(soup_photos,cookie,id)
        actor_info.data_photos.insert(data_photos)


        print("awards")
        soup_awards = getSoup(awards,cookie)
        data_awards = getAwardsList(soup_awards,cookie,id)
        actor_info.data_awards.insert(data_awards)

        print("movies")
        soup_movies = getSoup(movies,cookie)
        data_movies = getMoviesList(soup_movies,cookie,id)
        actor_info.data_movies.insert(data_movies)

        print("partners")
        soup_partners = getSoup(partners,cookie)
        data_partners = getPartnersList(soup_partners,cookie,id)
        actor_info.data_partners.insert(data_partners)

        print('___end___')
    except Exception as e:
        pass
        print(e);
        print('执行出现异常,等待下次执行! 403  403  403 bye!');
        time.sleep(3600)
        main()  # 重新执行


if __name__ == "__main__":
    main()
