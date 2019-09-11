#!/usr/bin/python
# -*- coding: UTF-8 -*-

import os
import sys
import time
import requests
import bs4
import io
from bs4 import BeautifulSoup as BS
#from urllib import request
import urllib as request


def getSoup(page):
    req = request.urlopen(page)
    html = req.read().decode('utf-8')
    soup = BS(html, 'html.parser')
    # sleep for several secs
    time.sleep(3)
    return soup


def getMovieList(url, headers):
    page_now = requests.get(url=url, headers=headers).json()
    movieInfo = page_now['subjects']
    titleList = [x['title'] for x in movieInfo]
    idList = [y['id'] for y in movieInfo]
    movieList = {'title': titleList, 'id': idList}
    return movieList


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
        review_split = tmp[0].contents[1].contents
        for item in review_split:
            if type(item) is bs4.element.NavigableString and item.strip() != '':
                review = review + '    ' + item.strip() + '\n'
    return review


def save2txt(filename, review_content):
    f = io.open(filename, 'w', encoding='utf-8')
    f.write(review_content)
    f.close()


def getReviewInPage(reviewList_soup, movie_name, nPage, save_path):
    tmp = reviewList_soup.find_all('div', class_='review-list')
    reviewList = tmp[0].find_all('div', attrs={'data-cid': True})  # 影评列表  attrs={'data-cid': True} --> 这里获取所有具有data-cid属性的li标签
    for nReview in range(len(reviewList)):
        print('           --- Review', nReview + 1)
        rid = reviewList[nReview]['data-cid']  # 第nReview个影评的id
        review_page = 'https://movie.douban.com/review/%s/' % rid  # 第nReview个影评的网页 #%s 类似于java占位符 % xxx 一次匹配
        review_soup = getSoup(review_page)  # 解析网页
        review_content = getReviewContent(review_soup)  # 获取影评内容
        review_content = movie_name + ' ' + str(nPage*20 + nReview + 1) + '\n' + review_content  # 加入电影名、影片序号
        filename = save_path + '/%s_%03d.txt' % (movie_name, nPage * 20 + nReview + 1)
        save2txt(filename, review_content)  # 写入txt文件
    return 0


def main(argv):
    base_url = 'https://movie.douban.com/j/search_subjects?type=movie&tag=%E6%9C%80%E6%96%B0&page_limit=20&page_start='  # "豆瓣电影-选电影-最新"网址 (未加载)
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36'}

    if not argv:
        # 默认参数值
        nStart = 0
        nBias = 9
        NPage = 5
    else:
        nStart = int(argv[1])
        nBias = int(argv[2])
        NPage = int(argv[3])

    print('nStart = ', nStart)
    print('nBias = ', nBias)
    print('NPage = ', NPage)

    save_folder_path = 'data/review/data_download/'  # 影评保存路径

    for bias in range(nStart, nStart + nBias): #(a,b) a为开始数,默认0 b为结束数 不包含当前的数
        url = base_url + '%d' % (bias*20)  # 每次加载20部影片
        movieList = getMovieList(url, headers)  # 获取新增20部影片列表, 影片title + 影片id
        for nMovie in range(len(movieList['id'])): #类似于java .length() nMovie-->for循环 0
            print('============ Downloading Movie', (bias*20 + nMovie + 1), '============')
            movie_page = 'https://movie.douban.com/subject/' + movieList['id'][nMovie] + '/'  # 影片网页
            movie_name = movieList['title'][nMovie]  # 影片名

            # 爬取影评时, 若该片已爬取过, 则跳过
            movie_saved = [item[item.find('_')+1:] for item in os.listdir(save_folder_path)]
            if movie_name in movie_saved:
                continue

            # 为每部影片新建文件夹, 保存摘取影评
            fid = len(os.listdir(save_folder_path)) + 1
            save_path = save_folder_path + str(fid) + '_' + movie_name
            os.makedirs(save_path)

            movie_soup = getSoup(movie_page + 'reviews')
            this_page = movie_soup.find('span', class_='thispage') #查询span class等于thispage的span
            if not this_page:
                total_page = 1
            else:
                total_page = int(this_page['data-total-page']) #获取span中属性的值

            # 爬取共NPage页影评, 每页包含20条影评
            for nPage in range(min(total_page, NPage)):
                print('      >>>>>> Page', int(nPage + 1))
                reviewList_page = movie_page + 'reviews?start=%s' % (nPage * 20)  # 第nPage页
                reviewList_soup = getSoup(reviewList_page)  # 解析网页
                getReviewInPage(reviewList_soup, movie_name, nPage, save_path)  # 获取第nPage页所有影评


if __name__ == '__main__':
    '''
        依次输入 nStart, nBias, NPage 三个参数
        nStart: 设定起始页, 即'选电影-最新'网页从加载nStart次开始, nStart = 0为未加载
        nBias:  设定加载次数, 即'选电影-最新'网页 '加载更多'的次数, 每加载一次，新增20部影片
        NPage:  每部影片下载影评的页数
    '''
    argv = [0,0,9,5]
    #main(sys.argv)
    main(argv)
