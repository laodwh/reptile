#!/usr/bin/python
# -*- coding: utf-8 -*-
# TF-IDF提取文本关键词
# http://scikit-learn.org/stable/modules/feature_extraction.html#tfidf-term-weighting

import sys
import os
import uuid

import pymysql  # import MySQLdb
import uuid as uuid
import time
from config_ch import *
import numpy as np
import pandas as pd
import xlrd
import jieba.posseg
import jieba.analyse
import copy
import io
from config_info import *
from sklearn.feature_extraction.text import TfidfTransformer
from sklearn.feature_extraction.text import CountVectorizer

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


# 关键词映射
"""
    输入关键词key及映射表mapword, 输出key_left_mapped,
    包括映射后剩余关键词"left"及映射得到的关键词"mapped"
    映射表第1列为atom词列表, 从第2列起为替换词列表,
    若key中某词属于atom列表, 则将该atom对应的替换词加入mappedList, 并从leftList中删除该词,
    若key中某词本身属于替换词列表, 则将该词加入mappedList, 并从leftList中删除
"""


def keysMapping(key, mapword):  # key中关键词若存在于atom中，则加入mappedList，leftList只保留未出现在atom中的关键词
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


def getKeyWord(text, stopword, mapword):
    corpus = []  # 将文本到输出到一个list中, 每行为一个文本
    keys_all = []  # 用于存放所有文本提取出的关键词
    mapped = []  # 用于合并所有文本映射后的关键词

    text_seg = dataPrepos(text, stopword)
    text_seg = " ".join(text_seg)
    corpus.append(text_seg)

    if corpus == ['']:
        return ''  # 空文本

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
    ids, titles, keys, keys_mapped, keys_left = [], [], [], [], []
    for i in range(len(weight)):
        print(u"-------这里输出第", i + 1, u"篇文本的词语TF-IDF------")
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
        key_left_split = " ".join(key_left_mapped["left"])  # 字符串
        key_mapped_split = " ".join(key_left_mapped["mapped"])  # 字符串

        mapped.extend(key_left_mapped["mapped"])  # 将每个文本映射后的关键词合并到mapped中, 有重复
        mapped = list(set(mapped))  # 去除重复词

        key_most = getKeymost(keys_all)  # 高频关键字

        keys.append(key)
        keys_left.append(key_left_split)
        keys_mapped.append(key_mapped_split)

        return key_most


def main():
    # 用户名：root 密码：root 数据库名：python_db
    db = pymysql.connect(ip, user, password, table, charset="utf8")
    cursor = db.cursor()

    # 加载停用词表
    stopword = [w.strip() for w in io.open(stopword_path, 'r', encoding='UTF-8').readlines()]

    # 加载映射表
    mapword = xlrd.open_workbook(map_path).sheet_by_index(0)

    # 加载自定义字典，用于jieba分词
    jieba.load_userdict(dict_path)

    # 获取所有的影片
    try:
        sql = "SELECT origin_movie_id FROM tb_douban_movie WHERE status = '%s'" % (1)
        cursor.execute(sql)
        rs = cursor.fetchall()
        for data_ in rs:
            movie_id = data_[0]
            if not movie_id.strip():
                continue

            # 删除已经存在的数据,防止数据
            sql_1 = "delete FROM tb_review_keyword_rel WHERE origin_movie_id = '%s'" % (movie_id)
            cursor.execute(sql_1)
            db.commit()

            # 获取当前影片下所有评论
            sql_review = "SELECT review_text,review_title,review_id FROM tb_douban_review WHERE origin_movie_id = '%s'" % (movie_id)
            cursor.execute(sql_review)
            rs_review = cursor.fetchall()
            for data_review in rs_review:
                review_text = data_review[0]
                review_id = data_review[2]

                if not review_text.strip():
                    continue

                keyWord = getKeyWord(review_text, stopword, mapword)
                if len(keyWord):
                    for key_ in keyWord:

                        # 查询关键字是否存在
                        sql_key = "SELECT key_id FROM tb_keyword WHERE key_text = '%s'" % (key_)
                        cursor.execute(sql_key)
                        rs_key = cursor.fetchall()
                        keyId = "";
                        if len(rs_key):
                            for data in rs_key:
                                keyId = str(data[0]);
                                break;
                        else:
                            # 生成关键字uuid
                            keyId = str(uuid.uuid1()).replace("-", "");

                            cursor.execute('SET NAMES utf8mb4;')
                            sql_2 = "insert into tb_keyword(key_id,key_text,create_time) values('" + keyId + "','" + key_ + "','" + time.strftime("%Y-%m-%d %H:%M:%S",time.localtime()) + "')"
                            cursor.execute(sql_2)
                            db.commit()

                        cursor.execute('SET NAMES utf8mb4;')
                        print(review_id,movie_id,keyId);
                        sql_3 = "insert into tb_review_keyword_rel(review_id,origin_movie_id,key_id) values('" + review_id + "','" + movie_id + "','" + keyId + "')"
                        print(sql_3);
                        cursor.execute(sql_3)
                        db.commit()
                # 更新为已处理
                sql_update = "update tb_douban_review set status =1  WHERE review_id = '%s'" % (review_id)
                cursor.execute(sql_update)
                db.commit()
    except Exception:
        pass
        print("Unexpected error:" + Exception);

if __name__ == '__main__':
    main()
