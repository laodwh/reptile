import urllib
from io import BytesIO
from os.path import join
import sqlite3
import xlsxwriter
import xlwt
import pandas
import lxml.html
from PIL import Image

etree = lxml.html.etree
import json
import os
import requests
from bs4 import BeautifulSoup


def ff():
    user = input('请输入学号：')
    password = input('请输入密码：')
    header = {
        'Host': '210.37.0.22',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:65.0) Gecko/20100101 Firefox/65.0',
        'Referer': 'http://www.hainnu.edu.cn/',
        'Upgrade-Insecure-Requests': '1',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2'
    }
    #构建请求头部信息先访问登陆页面
    s = requests.session()
    #用于记录服务器返回的cookie
    ur = 'http://210.37.0.22/'
    qq = s.get(ur, headers=header)
    ss = s.cookies
    #获得放回的cookie
    soup = BeautifulSoup(qq.text, 'html.parser')
    vie = soup.select('input[name="__VIEWSTATE"]')[0]['value']
    #解析页面得到viewstate
    res = s.get('http://210.37.0.22/CheckCode.aspx', headers=header, cookies=ss)
    img = Image.open(BytesIO(res.content))
    img.show()
    txtSecretCode = input('请输入验证码:')
    #获取到验证码，并输入
    print('ggggggggggggggggggggggggggggggg')
    paem = {
        'txtUserName': user,
        'TextBox2': password,
        'txtSecretCode': txtSecretCode,
        'RadioButtonList1': '学生',
        '__VIEWSTATE': vie,
        'Textbox1': '',
        'Button1': '',
        'lbLanguage': '',
        'hidPdrs': '',
        'hidsc': '',
    }

    headera = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
        'Host': '210.37.0.22',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:65.0) Gecko/20100101 Firefox/65.0',

        'Content-Type': 'application/x-www-form-urlencoded',
        'Upgrade-Insecure-Requests': '1',
        'Referer': 'http://210.37.0.22/',
    }
    url = 'http://210.37.0.22/default2.aspx'
    xx = requests.post(url, headers=headera, data=paem, cookies=ss)
    #使用post请求登陆
    return s, user
    #返回登陆后的cookie和学号

def re(ss):
    u = 'http://210.37.0.22/xscj_gc.aspx?xh={}&xm=%D1%CF%B7%C7%B7%B2&gnmkdm=N121617'
    url = u.format(ss[1])
    #将学号填入u中得到真确地址url
    header = {
        'Host': '210.37.0.22',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:65.0) Gecko/20100101 Firefox/65.0',
        'Referer': url,
        'Upgrade-Insecure-Requests': '1',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
        'Content-Length': '87041',
        'Content-Type': 'application/x-www-form-urlencoded',
        'Connection': 'keep-alive',
    }
    heade = {
        'Host': '210.37.0.22',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:65.0) Gecko/20100101 Firefox/65.0',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
        'Accept-Encoding': 'gzip, deflate',
        'Referer': 'http://210.37.0.22/default2.aspx',
        'Connection': 'keep-alive',
    }
    oo = requests.get(url, headers=heade, cookies=ss[0].cookies)
    #进入到登陆后的通知页面
    sou = BeautifulSoup(oo.text, 'html.parser')
    view = sou.select('input[name="__VIEWSTATE"]')[0]['value']
    #解析页面得到需要post的viewstate来查询成绩
    dat = {
        '__VIEWSTATEGENERATOR': '	DB0F94E3',
        '__VIEWSTATE': view,
        'Button1': ' % B0 % B4 % D1 % A7 % C6 % DA % B2 % E9 % D1 % AF'
    }
    #构造data
    res = requests.post(url, headers=header, data=dat, cookies=ss[0].cookies)
    soup = BeautifulSoup(res.text, 'html.parser')
    return soup
    #返回BeautifulSoup解析的soup

def dw(soup):
    i = 0
    tit = []
    #构造列表用于获取成绩
    while True:
        link = soup.select('table#Datagrid1>tr')
        result = {}
        #构造字典用于存放
        result['datatime'] = link[i].select('td')[0].contents[0]
        #获取第i个tr的第1个数据的内容
        result['class'] = link[i].select('td')[3].contents[0]
        result['xscore'] = link[i].select('td')[6].contents[0]
        result['score'] = link[i].select('td')[8].contents[0]
        tit.append(result)
        #将所有tr里的数据添加到tit列表
        i = i + 1
        if i == len(link):
            break
        #当i值等于link列表的长度时表示获取到全部的数据退出循环
    return tit
    #返回tit列表数据

def aa(tit):
    df = pandas.DataFrame(tit)
    df.to_excel('qqqqq.xls')
    #通过pandas整理数据并将其创建xls保存

if __name__ == '__main__':
    pp = ff()
    xx = re(pp)
    #将cookie和学号传入re方法中执行
    mm = dw(xx)
    #将soup传入dw方法执行
    aa(mm)
    #将tit列表数据传入aa方法中执行









