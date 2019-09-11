#version:
#       0.1
#release date:
#       2019-02-27
#change log:
#       1、python-reptile
#
#docker build -t reptile .
#docker run -i -t -p 8080:8080 reptile /bin/sh


FROM python:3.6-stretch

# 拷贝源文件
COPY .           /usr/src/reptile

RUN pip3 install numpy \
	&&	pip3 install BeautifulSoup4 \
	&&	pip3 install requests \
	&&	pip3 install bs4 \
	&&	pip3 install lxml \
	&&	pip3 install pymysql \
	&&	pip3 install requests \
	&&	pip3 install pandas \
	&&	pip3 install xlrd \
	&&	pip3 install jieba \
	&&	pip3 install sklearn

# 设置工作目录
WORKDIR /usr/src/reptile

ENTRYPOINT [ "python", "top001_key.py" ]

