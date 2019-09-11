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

# 浏览器代理


# 使用代理防止被封
proxies = {

}


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
    randint_data = random.randint(10, 20)
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
    randint_data = random.randint(10, 20)
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
    randint_data = random.randint(10, 20)
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
    elif startStr =="IMDb链接:" and content.find(startStr)>=0:
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
        movie['imdb'] = txt_wrap_by('IMDb链接:','\n',divInfo).replace("'", "");


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
        movie['author'] = txt_wrap_by('编剧:','\n',divInfo).replace("'", "");

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
        movie['location'] = txt_wrap_by('制片国家/地区:','\n',divInfo).replace("'", "");


    # 语言
    #movie['language'] = GetMiddleStr(divInfo, '语言:', '上映日期:').strip().replace("'", "");
    movie['language'] ='';
    language_ = txt_wrap_by('语言:','\n',divInfo);
    if not language_ is None:
        movie['language'] = txt_wrap_by('语言:','\n',divInfo).replace("'", "");

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
        movie['duration'] = txt_wrap_by('片长:','\n',divInfo).replace("'", "");

    # 别名
    # movie['alias'] = "";
    # if "又名:" in divInfo:
    #     movie['alias'] = GetMiddleStr(divInfo, '又名:', 'IMDb链接:').strip();
    movie['alias'] = '';
    alias_ = txt_wrap_by('又名:','\n',divInfo);
    if not alias_ is None:
        movie['alias'] = txt_wrap_by('又名:','\n',divInfo).replace("'", "");

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
        movie['episode'] = txt_wrap_by('集数:','\n',divInfo).replace("'", "");

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
                    review = review + '    ' + review_split[i].text.strip() + '\n'  # 拼接影评的多个段落
                else:
                    review = review + '    ' + review_split[i].text.strip()
    else:
        # review_split = tmp[0].contents[1].contents
        # for item in review_split:
        #     if type(item) is bs4.element.NavigableString and item.strip() != '':
        #         review = review + '    ' + item.strip() + '\n'
        review = tmp[0].contents[1].text
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

        # 影评标题
        temp_List["review_title"] = "";
        if not reviewList[nReview].find('h2') is None:
            temp_List["review_title"] = reviewList[nReview].find('h2').text.strip().replace("'", "");

        # 影评内容
        review_page = 'https://movie.douban.com/review/%s/' % rid
        review_soup = getSoup(review_page)
        review_content = getReviewContent(review_soup)  # 获取影评内容
        temp_List["review_text"] = review_content.strip().replace("'", "");

        # 影评时间
        temp_List["review_time"] = "";
        if not reviewList[nReview].find('span', class_='main-meta') is None:
            temp_List["review_time"] = reviewList[nReview].find('span', class_='main-meta').text.strip();

        # 对应电影的id

        # 状态值 0:待处理 1:已处理
        temp_List["status"] = "0";

        # 创建时间
        temp_List['create_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime());

        # 有用个数
        temp_List['up_num'] = "0";
        if not reviewList[nReview].find('span', id='r-useful_count-' + rid) is None:
            temp_List['up_num'] = reviewList[nReview].find('span', id='r-useful_count-' + rid).text.strip().replace("'", "");
        if temp_List['up_num'] == '':
            temp_List['up_num'] = "0";

        # 回应数量
        temp_List['reply_num'] = "0";
        if not reviewList[nReview].find('a', class_='reply ') is None:
            temp_List['reply_num'] = reviewList[nReview].find('a', class_='reply ').text.strip().replace("回应", "").replace("'", "");
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
            # 随机取请求头
            "User-Agent": random.choice(agents),
            "Origin": "https://movie.douban.com",
            "Referer": "https://movie.douban.com"
        }
        # 保存媒资信息+演员+影片
        info_s = []
        pers = 'https://movie.douban.com/subject/';
        #idList = []; # Todo: add movie list here.
        idList = [
                  '1292790']

        moveList = {'id': idList}
        for url_ in range(len(moveList['id'])):
            # 校验影片是否已存在,处理跳过
            movie_check = cursor.execute('select origin_movie_id from tb_douban_movie where origin_movie_id ="' + moveList['id'][url_] + '"')
            review_url = pers + moveList['id'][url_] + "/";
            movie_id = moveList['id'][url_];
            print(movie_id + "------");
            if movie_check != 0:
                continue

            s_url_ = getSoup(review_url);  # 解析html页面

            # 解析媒资详细信息
            info_s.append(getMoveDetail(s_url_, movie_id, review_url))  # 持久化媒资详细信息
            print(movie_id + "---success");

            if len(info_s) == 5:
                for info_db in range(len(info_s)):
                    cursor.execute('SET NAMES utf8mb4;')
                    sql = "insert into tb_douban_movie(origin_movie_id,type,name,release_year,image,director,actor,genre,location,language,date_published," \
                          "duration,alias,rating_value,description,episode,imdb,status,create_time,author) " \
                          "values('" + info_s[info_db]['origin_movie_id'] + "'" + "," + "'" + info_s[info_db]['type'] + "'" + "," + "'" + info_s[info_db][
                              'name'] + "'" + "," + "'" + info_s[info_db]['release_year'] + "'" + "," + "'" + info_s[info_db]['image'] + "'" + "," + "'" + info_s[info_db][
                              'director'] + "'" + "," + "'" + info_s[info_db]['actor'] + "'" + "," + "'" + info_s[info_db]['genre'] + "'" + "," + "'" + info_s[info_db][
                              'location'] + "'" + "," + "'" + info_s[info_db]['language'] + "'" + "," + "'" + info_s[info_db]['date_published'] + "'" + "," + "'" + \
                          info_s[info_db]['duration'] + "'" + "," + "'" + info_s[info_db]['alias'] + "'" + "," + "'" + info_s[info_db]['rating_value'] + "'" + "," + "'" + \
                          info_s[info_db]['description'] + "'" + "," + "'" + info_s[info_db]['episode'] + "'" + "," + "'" + info_s[info_db]['imdb'] + "'" + "," + "'" + \
                          info_s[info_db]['status'] + "'" + "," + "'" + info_s[info_db]['create_time'] + "'" + "," + "'" + info_s[info_db]['author'] + "')";
                    # print "---"+sql+"---"
                    print("sql:" + sql + "---success");
                    cursor.execute(sql)
                    db.commit()
                info_s.clear();
                print("插入操作执行完成");
        if len(info_s) < 5:
            for info_db in range(len(info_s)):
                cursor.execute('SET NAMES utf8mb4;')
                sql = "insert into tb_douban_movie(origin_movie_id,type,name,release_year,image,director,actor,genre,location,language,date_published," \
                      "duration,alias,rating_value,description,episode,imdb,status,create_time,author) " \
                      "values('" + info_s[info_db]['origin_movie_id'] + "'" + "," + "'" + info_s[info_db]['type'] + "'" + "," + "'" + info_s[info_db][
                          'name'] + "'" + "," + "'" + info_s[info_db]['release_year'] + "'" + "," + "'" + info_s[info_db]['image'] + "'" + "," + "'" + info_s[info_db][
                          'director'] + "'" + "," + "'" + info_s[info_db]['actor'] + "'" + "," + "'" + info_s[info_db]['genre'] + "'" + "," + "'" + info_s[info_db][
                          'location'] + "'" + "," + "'" + info_s[info_db]['language'] + "'" + "," + "'" + info_s[info_db]['date_published'] + "'" + "," + "'" + \
                      info_s[info_db]['duration'] + "'" + "," + "'" + info_s[info_db]['alias'] + "'" + "," + "'" + info_s[info_db]['rating_value'] + "'" + "," + "'" + \
                      info_s[info_db]['description'] + "'" + "," + "'" + info_s[info_db]['episode'] + "'" + "," + "'" + info_s[info_db]['imdb'] + "'" + "," + "'" + \
                      info_s[info_db]['status'] + "'" + "," + "'" + info_s[info_db]['create_time'] + "'" + "," + "'" + info_s[info_db]['author'] + "')";
                # print "---"+sql+"---"
                print("sql:" + sql + "---success");
                cursor.execute(sql)
                db.commit()
        print("插入操作执行完成");
    except Exception as e:
        pass
        print(e);
        print('执行出现异常,等待下次执行! 403  403  403 bye!');
        time.sleep(360)
        main()  # 重新执行


if __name__ == "__main__":
    main()
