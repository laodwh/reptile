import sqlite3
import time

import pandas
import requests
import json
from bs4 import BeautifulSoup
import urllib3
def head():
    #获取请求头
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:66.0) Gecko/20100101 Firefox/66.0',
        'Referer': 'https://www.lagou.com/jobs/list_Python?labelWords=&fromSearch=true&suginput=',
        'Host': 'www.lagou.com'
    }
    resp = requests.get(init_url, headers=headers, timeout=10,verify=False)
    print(resp.cookies.get_dict())
    #得到响应的cookie
    search_id = resp.cookies.get('SEARCH_ID')
    lgr_id = resp.cookies.get('LGRID')
    jession_id = resp.cookies.get('JSESSIONID')
    #将响应的cookie值分别赋给上述3个变量
    headers['Cookie'] = 'JSESSIONID={}; user_trace_token={}; LGSID={}; LGUID={}; LGRID={}; SEARCH_ID={}'.format(
        jession_id, lgr_id, lgr_id, lgr_id, lgr_id, search_id)
    #为headers添加正确的cookie
    return  headers
    #返回headers

def rr(headers):
    #获取数据字典
    resp = requests.post(url, headers=headers, data=formdata, timeout=10,verify=False)
    #请求数据链接得到json数据
    jd=json.loads(resp.text)
    #将str数据转换成字典类型
    return jd
    #返回jd

def jk(jd):
    #提取得到的字典中的数据
    re=[]
    js=jd['content']['positionResult']['result']
    #获取字典中到所有数据存入js列表中
    for xx in  js:
    #通过for遍历将所有需要的数据得到
        cc = {}
        cc['salary'] = xx['salary']
        cc['positionName']=xx['positionName']
        cc['city'] = xx['city']
        re.append(cc)
        #每次循环时都将cc字典中得到的所有数据添加到re列表
    return re
    #返回re列表
if __name__ == '__main__':
    keyword = input('请输入你想获取的数据：')
    init_url = 'https://www.lagou.com/jobs/list_{}?city=%E5%85%A8%E5%9B%BD&cl=false&fromSearch=true&labelWords=&suginput='.format(
        keyword)
    url = 'https://www.lagou.com/jobs/positionAjax.json?needAddtionalResult=false'
    urllib3.disable_warnings()
    # 取消警告
    xx=head()
    i=1
    x=0
    tt=[]
    while True:
        formdata = {
            'first': 'true',
            'pn': i,
            'kd': keyword
        }
        ll=rr(xx)
        vv=jk(ll)
        print('正在爬取第'+str(i)+'页')
        tt.extend(vv)
        #将返回的vv列表合并
        i=i+1
        x=x+1
        time.sleep(3)
        #暂停3秒防反爬
        if x==10:
            xx=head()
            x=0
        #一个请求得到的cookie只能访问10次，当访问完第10此时，重新获取请求头在开始访问
        if vv==[]:break
        #当返回的vv的数据列表为空时退出循环
    df=pandas.DataFrame(tt)
    #通过pandas整理数据
    print(df)
    with sqlite3.connect('news.sql') as db:
        df.to_sql('news',con=db)
        df2=pandas.read_sql_query('SELECT * FROM news',con=db)
    #创建数据库并查询