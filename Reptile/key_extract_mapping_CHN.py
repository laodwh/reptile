#!/usr/bin/python
# coding=utf-8
# TF-IDF提取文本关键词
# http://scikit-learn.org/stable/modules/feature_extraction.html#tfidf-term-weighting

import sys
import os
from config_ch import *
import chardet
import numpy as np
import pandas as pd
import xlrd
import copy
import glob
import jieba.posseg
import jieba.analyse
import io
from sklearn import feature_extraction
from sklearn.feature_extraction.text import TfidfTransformer
from sklearn.feature_extraction.text import CountVectorizer
"""
       TF-IDF权重：
           1、CountVectorizer  构建词频矩阵
           2、TfidfTransformer 构建TF-IDF权值计算
           3、文本的关键词
           4、对应的TF-IDF矩阵
"""
# 数据读取
"""
    输入数据所在文件夹路径data_path, 输出data为一字典, 包含'id', 'title', 'abstract'
"""
def dataRead(data_path):
    file_list = os.listdir(data_path)
    idList, titleList, abstractList = range(0, len(file_list)), [], []  # 构建3个list, 用于存放文本编号, 文本标题, 文本内容
    for file_name in file_list:
        file_path = os.path.join(data_path, file_name)
        if os.path.isfile(file_path):
            f = io.open(file_path, 'rb').read()
            encoding_type = chardet.detect(f)  # 获取文本的编码形式
            if not encoding_type['encoding']:
                encoding_type['encoding'] = 'utf-8-sig'  # 一些文本编码形式为none, 强制转换
            file = f.decode(encoding_type['encoding'])
            titleList.append(file[0:file.find('\n', 1)+1])  # 文本第一行为标题
            abstractList.append(file)
    data = {"id": idList, "title": titleList, "abstract": abstractList}
    return data

# 预处理
"""
    输入文本text及停用词表stopword, 输出分词结果text_seg
    预处理包括jieba分词, 去停用词, 筛选词性
"""
def dataPrepos(text, stopword):
    text_seg = []
    seg = jieba.posseg.cut(text)  # 分词
    for i in seg:
        if i.word not in stopword and i.flag in pos:  # 去停用词 + 筛选词性
            text_seg.append(i.word)
    return text_seg

# 关键词映射
"""
    输入关键词key及映射表mapword, 输出key_left_mapped,
    包括映射后剩余关键词"left"及映射得到的关键词"mapped"
    映射表第1列为atom词列表, 从第2列起为替换词列表,
    若key中某词属于atom列表, 则将该atom对应的替换词加入mappedList, 并从leftList中删除该词,
    若key中某词本身属于替换词列表, 则将该词加入mappedList, 并从leftList中删除
"""
def keysMapping(key, mapword):# key中关键词若存在于atom中，则加入mappedList，leftList只保留未出现在atom中的关键词
    leftList, mappedList = copy.deepcopy(key), []  # 初始化leftList, mappedList
    atom = mapword.col_values(0)
    for i in key:
        if i in atom:  # 关键词为atom列表中的词, 则用对应的替换词进行替换
            mappedList.extend(mapword.row_values(atom.index(i))[1:])
            mappedList = list(filter(None, mappedList))  # 去除""字符串
            leftList.pop(leftList.index(i))  # 从leftList中删除
        else:
            for n in range(len(atom)):
                row = mapword.row_values(n)[1:]
                if i in row:  # 关键词本身为替换词列表中的词, 则加入mappedList, 并从leftList中删除
                    mappedList.extend([i])
                    leftList.pop(leftList.index(i))
                    break

    mappedList = list(set(mappedList))  # 去除重复词
    key_left_mapped = {"left": leftList, "mapped": mappedList}
    return key_left_mapped

# TF-IDF提取topK关键词
"""
    输入包括数据data, 停用词表stopword, 映射表mapword, 及中间变量mapped和keys_all,
    当mode为'tf'时, 每个文本单独调用getKeyword, 需传入文本id,
    当mode为'tfidf'时, 多个文本作为整体只调用一次getKeyword, 不需id, 令id = 0
"""
def getKeywords(data, id, stopword, mapword, mapped, keys_all):
    # 从data中取出id, title, abstract, 构建3个list
    if mode == 'tfidf':
        idList, titleList, abstractList = data['id'], data['title'], data['abstract']
    elif mode == 'tf':  # 取出第id个文本的信息
        idList, titleList, abstractList = [data['id'][id]], [data['title'][id]], [data['abstract'][id]]

    corpus = []  # 将所有文本到输出到一个list中, 每行为一个文本
    result = pd.DataFrame({"id": [], "title": [], "key": [], "left": [], "mapped": []},
                          columns=['id', 'title', 'key', 'left', 'mapped'])
    # 分别对每个文本进行预处理, 将处理后的词连接成字符串(空格分隔), 输入到corpus中的一行
    for index in range(len(idList)):
        text = '%s' % abstractList[index]
        text_seg = dataPrepos(text, stopword)
        text_seg = " ".join(text_seg)
        corpus.append(text_seg)
    if corpus == ['']:
        return result  # 空文本
    # 1、构建词频矩阵，将文本中的词语转换成词频矩阵
    vectorizer = CountVectorizer()
    X = vectorizer.fit_transform(corpus)  # 词频矩阵
    # 2、统计每个词的TF-IDF权值
    transformer = TfidfTransformer()
    tfidf = transformer.fit_transform(X)
    # 3、获取词袋模型中的关键词
    word = vectorizer.get_feature_names()
    # 4、获取TF-IDF矩阵
    weight = tfidf.toarray()
    # 5、打印词语权重
    # 以下变量分别用于存放文本编号, 标题, 提取出的关键词, 映射得到的关键词, 映射后剩余的关键词
    ids, titles, keys, keys_mapped, keys_left = [], [], [], [], []
    for i in range(len(weight)):
        print(u"-------这里输出第", i+1, u"篇文本的词语TF-IDF------")
        ids.append(idList[i])  # 添加编号到ids
        titles.append(titleList[i])  # 添加标题到titles
        df_word, df_weight = [], []  # 当前文本的所有词汇列表、词汇对应权重列表
        for j in range(len(word)):
            print(word[j], weight[i][j])
            if weight[i][j] == 0:
                df_word.append(' ')  # 用空字符串替换权重为0的词
            else:
                df_word.append(word[j])
            df_weight.append(weight[i][j])
        # 将df_word和df_weight转换为pandas中的DataFrame形式, 用于排序
        df_word = pd.DataFrame(df_word, columns=['word'])
        df_weight = pd.DataFrame(df_weight, columns=['weight'])
        word_weight = pd.concat([df_word, df_weight], axis=1)  # 拼接词汇列表和权重列表
        word_weight = word_weight.sort_values(by="weight", ascending=False)  # 按照权重值降序排列
        keyword = np.array(word_weight['word'])  # 选择词汇列并转成数组格式
        key = [keyword[x] for x in range(0, min(topK, len(word)))]  # 抽取前topK个词汇作为关键词
        keys_all.extend(key)  # 将当前文本提取出的关键词加入keys_all中, 用于后续的高频关键词提取

        # 关键词映射
        key_left_mapped = keysMapping(key, mapword)
        # 将list中的词连接成字符串
        key = " ".join(key)
        key_left_split = " ".join(key_left_mapped["left"])
        key_mapped_split = " ".join(key_left_mapped["mapped"])

        mapped.extend(key_left_mapped["mapped"])  # 将每个文本映射后的关键词合并到mapped中, 有重复

        keys.append(key)
        keys_left.append(key_left_split)
        keys_mapped.append(key_mapped_split)

    result = pd.DataFrame({"id": ids, "title": titles, "key": keys, "left": keys_left, "mapped": keys_mapped}, columns=['id', 'title', 'key', 'left', 'mapped'])
    return result

# 提取topN高频关键词
"""
    输入keys_all为每个文本提取出的topK关键词合并后的列表,
    输出key_most为提取出的topN个高频关键词
"""
def getKeymost(keys_all):
    counts = []
    keys_nodup = list(set(keys_all))  # keys_all去重后结果
    for item in keys_nodup:
        counts.append(keys_all.count(item))  # 统计每个关键词出现的次数
    key_word = pd.DataFrame(keys_nodup, columns=['key'])
    count_word = pd.DataFrame(counts, columns=['count'])
    key_count = pd.concat([key_word, count_word], axis=1)
    key_count = key_count.sort_values(by="count", ascending=False)
    key_freq = np.array(key_count['key'])

    key_most = [key_freq[x] for x in range(0, min(topN, len(key_word)))]
    return key_most


def main():

    # 删除历史结果
    for f in glob.glob(os.path.join('result', '*.xls')):
        os.remove(f)

    # 加载停用词表
    stopword = [w.strip() for w in io.open(stopword_path, 'r', encoding='UTF-8').readlines()]

    # 加载映射表
    mapword = xlrd.open_workbook(map_path).sheet_by_index(0)

    # 加载自定义字典，用于jieba分词
    jieba.load_userdict(dict_path)

    folderList = os.listdir(data_path)

    for folder in folderList:  # 遍历全部电影文件夹, 每个文件夹中为1部电影的全部影评
        folder_path = os.path.join(data_path, folder)

        # 读取数据
        data = dataRead(folder_path)

        keys_all = []  # 用于存放所有文本提取出的关键词
        mapped = []  # 用于合并所有文本映射后的关键词

        # 关键词提取,
        if mode == 'tfidf':
            result = getKeywords(data, 0, stopword, mapword, mapped, keys_all)
            result.to_csv("result/CHkeys_tfidf_" + folder + ".xls", index=False, encoding='utf-8-sig')
        elif mode == 'tf':
            for i in range(len(data['id'])):  # 'tf'模式下, 每个文本单独调用getKeywords
                result = getKeywords(data, i, stopword, mapword, mapped, keys_all)
                result.to_csv("result/CHkeys_tf_" + folder + ".xls", mode='a', header=False, index=False, encoding='utf-8-sig')

        mapped = list(set(mapped))  # 去除重复词
        mapped_result = pd.DataFrame({"mapped": [" ".join(mapped)]}, columns=['mapped'])
        pd.DataFrame({"": [" ".join([])]}).to_csv("result/CHkeys_tf_" + folder + ".xls", mode='a', index=False)  # 增加空行
        mapped_result.to_csv("result/CHkeys_tf_" + folder + ".xls", mode='a', index=False, encoding='utf-8-sig', columns=['', '', 'mapped'])

        # 提取高频关键词
        key_most = getKeymost(keys_all)
        key_most = pd.DataFrame({"most mentioned": [" ".join(key_most)]}, columns=['most mentioned'])
        pd.DataFrame({"": [" ".join([])]}).to_csv("result/CHkeys_tf_" + folder + ".xls", mode='a', index=False)  # 增加空行
        key_most.to_csv("result/CHkeys_tf_" + folder + ".xls", mode='a', index=False, encoding='utf-8-sig', columns=['', '', 'most mentioned'])


if __name__ == '__main__':
    main()
