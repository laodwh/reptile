#!/usr/bin/python
# -*- coding: utf-8 -*-
# http://scikit-learn.org/stable/modules/feature_extraction.html#tfidf-term-weighting


import requests
#定义cookie对象
cookiejar = requests.cookies.RequestsCookieJar()

print('******************访问豆瓣主页 start*****************')
print('')
res = requests.get('https://www.douban.com/',verify =False,allow_redirects =False)
print(res.status_code)
print(res.headers)
print(res.cookies)
for key in dict(res.cookies):
    cookiejar.set(key,res.cookies[key])
print('')
print('******************访问豆瓣主页 end*****************')

#获取图片验证码token
print('******************获取图片验证码token start*****************')
print('')
data = {

}
res = requests.get('https://www.douban.com/j/misc/captcha',data= data,cookies =cookiejar,verify =False,allow_redirects =False)
print(res.text)
print(res.status_code)
print(res.headers)
print(res.cookies)
for key in dict(res.cookies):
    cookiejar.set(key,res.cookies[key])
token = res.json()['token']
imgurl = 'https:' + res.json()['url']
print('token:{},url:{}'.format(token,imgurl))
print('')
print('******************获取图片验证码token end*****************')

#根据token获取验证码图片，保存到本地
print('******************根据token获取验证码图片，保存到本地 start*****************')
print('')
res = requests.get(imgurl,cookies = cookiejar,verify =False,allow_redirects =False)
print(res.status_code)
print(res.cookies)
with open('dbimage.jpg','wb') as f:
    f.write(res.content)
print('')
print('******************根据token获取验证码图片，保存到本地 end*****************')

#验证用户账户名密码
print('******************验证用户账户名密码 start*****************')
print('')
data = {
    'captcha-id':token,
    'captcha-solution':input('请输入验证码:'),
    'form_email':'181767171@qq.com',
    'form_password':'dailong0815',
    'source':'index_nav',
}
print(data)
res = requests.post('https://accounts.douban.com/passport/login',data =data,cookies =cookiejar,verify =False,allow_redirects =False)
print(res.status_code)
print(res.text)
print(res.cookies)
for key in dict(res.cookies):
    cookiejar.set(key,res.cookies[key])
print('')
print('******************验证用户账户名密码 end*****************')

#验证登陆账号信息
print('******************验证登陆账号信息 start*****************')
print('')
res = requests.get('https://www.douban.com/accounts/',cookies = cookiejar,verify =False,allow_redirects =False)
print(res.status_code)
print(res.cookies)
print('******************验证登陆账号信息 end*****************')