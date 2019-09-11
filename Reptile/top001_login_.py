#!/usr/bin/python
# -*- coding: utf-8 -*-
# http://scikit-learn.org/stable/modules/feature_extraction.html#tfidf-term-weighting
import http
import urllib
from http import cookiejar


if __name__ == '__main__':
    url = 'https://movie.douban.com/'
    headers = {
        'User-Agent': 'Mozilla/5.0(Windows NT 10.0; WOW64)AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.104 Safari/537.36 Core/1.53.3427.400 QQBrowser/9.6.12513.400'
    }

    cookie = cookiejar.CookieJar()
    handler = urllib.request.HTTPCookieProcessor(cookie)
    opener = urllib.request.build_opener(handler)
    resp = opener.open(url)

    cookieStr = ''
    for item in cookie:
        cookieStr = cookieStr + item.name + '=' + item.value + ';'

    print(cookieStr)