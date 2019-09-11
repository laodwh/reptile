import time
from datetime import datetime
import requests
from selenium.common.exceptions import NoSuchElementException, ElementNotInteractableException, TimeoutException
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support import  expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium import  webdriver
from selenium.webdriver.common.by import By
import lxml.html
etree=lxml.html.etree
def  tit():

    respond=webdriver.Firefox()
    #使用Firefox浏览器，当然你也可以将Firefox设置成无头类型，这样更快
    respond.get('https://news.sina.com.cn/world/')
    wait=WebDriverWait(respond,3)
    #设置显示等待3秒
    while True:
        try:
            input1 = wait.until(
            EC.presence_of_element_located((By.XPATH, '//span[@class="pagebox_next"]'))
            )
            #直到找到定位元素位置
            break

        except TimeoutException:
            js ='var q=document.documentElement.scrollTop=100000'
            respond.execute_script(js)
            #超时就通过解析js进行下拉滑块
    html = respond.page_source
    #获取当前页面源码
    ff = etree.HTML(html)
    #解析当前页面原码
    xx0=ff.xpath('//div[@id="subShowContent1_news1"]/div[@class="news-item first-news-item img-news-item"]/h2/a/text()')
    xx1= ff.xpath('//div[@id="subShowContent1_news2"]/div[@class="news-item  img-news-item"]/h2/a/text()')
    xx2 = ff.xpath('//div[@id="subShowContent1_news4"]/div[@class="news-item  img-news-item"]/h2/a/text()')
    xx3= ff.xpath('//div[@id="subShowContent1_news4"]/div[@class="news-item"]/h2/a/text()')
    ss0=ff.xpath('//div[@id="subShowContent1_news1"]/div[@class="news-item first-news-item img-news-item"]/h2/a/@href')
    ss1 = ff.xpath('//div[@id="subShowContent1_news2"]/div[@class="news-item  img-news-item"]/h2/a/@href')
    ss2=ff.xpath('//div[@id="subShowContent1_news4"]/div[@class="news-item  img-news-item"]/h2/a/@href')
    ss3=ff.xpath('//div[@id="subShowContent1_news4"]/div[@class="news-item"]/h2/a/@href')
    t=xx0+xx1+xx2+xx3
    s=ss0+ss1+ss2+ss3
    #爬取所有链接及新闻标题
    return t,s
def xx(ss):
    ee=requests.get(ss)
    ee.encoding='utd-8'
    mm=etree.HTML(ee.text)
    dd = mm.xpath('//span[@class="date"]/text()')
    time = datetime.strptime(dd[0], '%Y年%m月%d日 %H:%M')
    #将dd转换成时间格式
    print(time)
if __name__ =='__main__':

    t=tit()
    print(t[0])
    print(t[1])
    print(len(t[1]))
    for title,ss in zip(t[0],t[1]):
        xx(ss)
    #通过for遍历得到每个链接传入xx方法，这里我只爬取时间，其他的同理添加可得