#!/usr/bin/python
# -*- coding: utf-8 -*-
# http://scikit-learn.org/stable/modules/feature_extraction.html#tfidf-term-weighting


import http.cookiejar  # import cookielib
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
    #Firefox
    "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:34.0) Gecko/20100101 Firefox/34.0",
    "Mozilla/5.0 (X11; U; Linux x86_64; zh-CN; rv:1.9.2.10) Gecko/20100922 Ubuntu/10.10 (maverick) Firefox/3.6.10",
    #chrome
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.71 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11",
    "Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US) AppleWebKit/534.16 (KHTML, like Gecko) Chrome/10.0.648.133 Safari/534.16",
    #UC浏览器
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.122 UBrowser/4.0.3214.0 Safari/537.36",
    #IPhone
    "Mozilla/5.0 (iPhone; U; CPU iPhone OS 4_3_3 like Mac OS X; en-us) AppleWebKit/533.17.9 (KHTML, like Gecko) Version/5.0.2 Mobile/8J2 Safari/6533.18.5",
    #IPod
    "Mozilla/5.0 (iPod; U; CPU iPhone OS 4_3_3 like Mac OS X; en-us) AppleWebKit/533.17.9 (KHTML, like Gecko) Version/5.0.2 Mobile/8J2 Safari/6533.18.5",
    #IPAD
    "Mozilla/5.0 (iPad; U; CPU OS 4_2_1 like Mac OS X; zh-cn) AppleWebKit/533.17.9 (KHTML, like Gecko) Version/5.0.2 Mobile/8C148 Safari/6533.18.5",
    "Mozilla/5.0 (iPad; U; CPU OS 4_3_3 like Mac OS X; en-us) AppleWebKit/533.17.9 (KHTML, like Gecko) Version/5.0.2 Mobile/8J2 Safari/6533.18.5",
    #Android
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
db_mongo = conn.mydb  #连接mydb数据库，没有则自动创建
actor_info = db_mongo.actor_info #使用actor_info集合，没有则自动创建

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
def getSoup_Test(url,agent):

    #增加cookie
    headers = {
        # 随机取请求头
        "User-Agent": agent,
        "Origin": "https://movie.douban.com",
        "Referer": "https://movie.douban.com"
    }
    oo = requests.get(url, headers=headers)
    soup = BS(oo.text, 'html.parser')
    print("--------------"+agent+"---------------")
    print(soup.find('div', id='content').text.strip().replace(" ", ""))
    print("-----------------------------")
    # sleep for several secs
    randint_data = random.randint(3, 8)
    if randint_data < 3:
        randint_data = 3
    time.sleep(randint_data)
    return soup

# 解析html页面 soup
def getSoup(url,cookies):

    #增加cookie
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
    print(path+"--> image save success")


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
        temp_List["review_text"] ="";
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


def _get_captcha_info(url,user,password):
    captcha_info=dict()
    response=requests.get(url,headers=headers)
    html ='';
    if response.status_code==200:
        html=response.text
    #验证码url
    pat =re.compile(r'<img id="captcha_image" src="(.*?)" alt="captcha" class="captcha_image"/>',re.S)
    response_1 =re.search(pat,html)
    if response_1:
        captcha_url =response_1.group(1)
        print("验证码图片：{}".format(captcha_url))
        captcha_solution =input('验证码:')
        captcha_info['captcha-solution']=captcha_solution
    #验证码ID
    pat1 =re.compile(r'<input type="hidden" name="captcha-id" value="(.*?)"/>',re.S)
    response_2 =re.search(pat1,html)
    if response_2:
        captcha_id =response_2.group(1)
        captcha_info['captcha-id']=captcha_id
    return captcha_info

#登录返回cookie
def login(url,user,password):
    #登录
    req_headers={
        'User-Agent':"Mozilla/5.0 (X11; U; Linux x86_64; zh-CN; rv:1.9.2.10) Gecko/"
                     "20100922 Ubuntu/10.10 (maverick) Firefox/3.6.10"
    }

    req_data={
        'ck':'',
        'ticket': '',
        'name':user,
        'password':password,
        'remember':False
    }

    # 使用Session post数据
    session=requests.Session()
    resp=session.post(url,data=req_data,headers=req_headers)
    print(resp.cookies.get_dict()) #cookies内容
    temp = session.cookies;
    session.close(); #避免没必要的开销
    return temp;

#拼接指定格式
def getActorList(actor_soup,cookie,movie_id):
    actor_p =[]

    list_wrapper = actor_soup.find_all('div', class_='list-wrapper')
    if list_wrapper is None:
        return actor_p

    actor_movie ={}
    #actor_s =[]
    class_ = {}
    for na in range(len(list_wrapper)):
        directors = []

        #所属分类
        type = list_wrapper[na].find_all('h2')[0].text
        type = re.sub('[^a-zA-Z]','',type) #去除中文

        actorList = list_wrapper[na].find_all('li', class_='celebrity')  # 演员列表

        for na in range(len(actorList)):
            actorInfo_ ={}

            temp_ = actorList[na]

            #演员id
            ahref = temp_.find_all('a')[0]['href']


            id = ahref.strip().replace("https://movie.douban.com/celebrity/", "").replace("/", "");

            #演员名称
            actorName = temp_.find('a', class_='name').text;

            #角色
            role = temp_.find('span', class_='role').text;

            #图片 规则(id+",jpg")
            style = temp_.contents[1].contents[1].attrs['style']

            #保存图片
            if not style is None:
                downPath = style.replace("background-image: url(", "").replace(")", "");
                if downPath !="":
                    save_file(image_path, id + '.jpg', get_file(downPath.replace("webp", "jpg")));

            #代表作
            work_list =[]
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
        #actor_s.append(class_)

    actor_movie["movie_id"] = movie_id;
    actor_movie["movie_celebrities"] = class_;
    actor_p.append(actor_movie)
    return actor_p

def main():

    #login to douban 报错会等待,然后重新登录获取
    cookie = login('https://accounts.douban.com/j/mobile/login/basic',db_username,db_password)

    try:
        print('___start___')
        sql = "SELECT a.origin_movie_id FROM tb_douban_movie a WHERE a.`status_actor` = '%s'" % (0)
        print('sql:'+sql)
        cursor.execute(sql)
        rs = cursor.fetchall()
        for data_ in rs:
            # print data_[0],'  -  ', data_[1]
            movie_id = data_[0];
            movie_url = "https://movie.douban.com/subject/" + movie_id + "/celebrities"

            print('---movie-->'+movie_id+"---url--->"+movie_url)

            #请求url,获取原始html页面文本
            actor_soup = getSoup(movie_url,cookie)

            #解析文本
            actor_s = getActorList(actor_soup,cookie,movie_id)

            #存入mongo数据库
            actor_info.my_set.insert(actor_s)

            # 更新为已处理
            sql = "update tb_douban_movie set status_actor =1  WHERE origin_movie_id = '%s'" % (movie_id)
            cursor.execute(sql)
            db.commit()
        print('___end___')
    except Exception as e:
        pass
        print(e);
        print('执行出现异常,等待下次执行! 403  403  403 bye!');
        time.sleep(3600)
        main()  # 重新执行


if __name__ == "__main__":
    main()

