# This Python file uses the following encoding: utf-8

# 数据路径
data_path = 'data/review/data_download'

# 停用词表路径
stopword_path = 'data/stopwords/CNstopwords_1128.txt'

# 映射表路径
map_path = 'data/map/map1.xls'

# 自定义字典路径
dict_path = 'data/dict.txt'

# 关键词提取模式, 'tf' 或 'tfidf'
mode = 'tf'

# 关键词词性
# pos = ['n', 'ns', 'nz', 'nr']
# pos = ['n', 'ns', 'nz', 'v', 't', 'vn', 'a', 'd']
pos = ['n', 'ns', 'nz', 't', 'vn', 'a']

# 关键词提取数
topK = 30

# 高频关键词提取数
topN = 15
