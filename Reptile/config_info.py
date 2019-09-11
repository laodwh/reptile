# This Python file uses the following encoding: utf-8

# 图片存储路径
image_path = 'data/image'

#mysql配置
ip = '192.166.169.81' #数据库ip
user = 'root'   #用户名
password = 'root'   #密码
table = 'labeldb0314' #数据库

#抓取参数
base_url = "https://movie.douban.com/j/search_subjects?type=movie&tag=热门&sort=recommend&page_limit=20&page_start=";  # "豆瓣电影-选电影-热门"
type = "Movie";
Start = 1 #抓取起始页
Bias = 1 #总页数
review = 2 #抓取评论页数

review_short = 1 #抓取短评页数

#douban info
db_username = '181767171@qq.com'
db_password = 'dailong0815'
