# -*- coding = utf-8 -*-
# @Time : 2021/2/20 18:21
# @Author : ocean G
# @File : WeiBoComments.py
# @Software : PyCharm
import datetime
import requests
import time
import os
import csv
import sys
import json
from bs4 import BeautifulSoup
import importlib
import sqlite3

importlib.reload(sys)

url = 'https://m.weibo.cn/comments/hotflow?id=4596224256651210&mid=4596224256651210&max_id='
header = {
    'Cookie': 'WEIBOCN_FROM=1110006030; SUB=_2A25NK9QwDeRhGeNL6FYX-SzIwj-IHXVu1_x4rDV6PUJbktAfLW7xkW1NSPT4rj0dy37B20UbcSw72S8Rg3VnpphH; SUBP=0033WrSXqPxfM725Ws9jqgMF55529P9D9W5guRnRuoNUSITNy02M0Jdb5NHD95QfSKeXSo.ESh.0Ws4DqcjDKXS8B-RLxKBLBonL12BLxK-LB.eL1h5LxK-LB.qL1heLxKBLB.2L1hqt; SSOLoginState=1613735008; _T_WM=96477264906; MLOGIN=1; XSRF-TOKEN=0bbe0b; M_WEIBOCN_PARAMS=uicode%3D20000061%26fid%3D4596224256651210%26oid%3D4596224256651210',
    'Referer': 'https://m.weibo.cn/u/1624923463?uid=1624923463&t=0&luicode=10000011&lfid=100103type%3D1%26q%3D%25E5%258D%258E%25E6%2599%25A8%25E5%25AE%2587',
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.66 Safari/537.36',
    'X-Requested-With': 'XMLHttpRequest'
}


def main():
    dbPath = "WeiBoComments.db"
    maxPage = 50  # 爬取的数量
    m_id = 0
    id_type = 0
    init_db(dbPath)
    try:
        for page in range(0, maxPage):
            print(page)
            jsonData = getPage(m_id, id_type)
            dataList = getData(jsonData)
            # print(dataList)
            saveData(dataList, dbPath)
            results = parse_page(jsonData)
            time.sleep(1)
            m_id = results['max_id']
            id_type = results['max_id_type']

    except AttributeError as e:
        print("反爬的来了！", e.args)


def getPage(max_id, id_type):
    params = {
        'max_id': max_id,
        'max_id_type': id_type

    }
    try:
        request = requests.get(url, params=params, headers=header)
        if request.status_code == 200:
            return request.json()
    except requests.ConnectionError as e:
        print('error', e.args)


def parse_page(jsonData):
    if jsonData:
        items = jsonData.get('data')
        item_max_id = {'max_id': items['max_id'], 'max_id_type': items['max_id_type']}
        return item_max_id


# 获取数据
def getData(jsonData):
    dataList = []

    datas = jsonData.get('data').get('data')
    for data in datas:
        dataList0 = []
        userId = str(data.get("id"))
        dataList0.append(userId)

        username = str(data.get("user").get("screen_name"))
        dataList0.append(username)

        create_time = data.get("created_at")
        std_transfer = '%a %b %d %H:%M:%S %z %Y'  # 转换的一个格式
        std_create_time = str(datetime.datetime.strptime(create_time, std_transfer))
        dataList0.append(std_create_time)

        like_count = str(data.get("like_count"))
        dataList0.append(like_count)

        total_number = str(data.get("total_number"))
        dataList0.append(total_number)

        comment = data.get("text")
        comment = str(BeautifulSoup(comment, 'lxml').get_text())
        dataList0.append(comment)

        dataList.append(dataList0)

    return dataList


# 创建数据库
def init_db(dbPath):
    sql = '''
        create table WeiBoComments(
            userId integer primary key ,
            username varchar ,
            create_time time ,
            like_count numeric ,
            total_number numeric ,
            comments varchar 
             
        )
    '''
    conn = sqlite3.connect(dbPath)
    cursor = conn.cursor()  # Cursor 是每行的集合
    cursor.execute(sql)
    conn.commit()
    conn.close()


# 数据存放到SQLite数据库
def saveData(dataList, dbPath):
    conn = sqlite3.connect(dbPath)
    cur = conn.cursor()
    for data in dataList:
        for index in range(len(data)):
            data[index] = '"' + str(data[index]) + '"'
            data0 = ",".join(data)

        # sql语句
        sql = '''
            insert into WeiBoComments(
                userId,
                username  ,
                create_time ,
                like_count ,
                total_number ,
                comments 
            )
            
            values (%s)''' % data0
        cur.execute(sql)
        conn.commit()
    cur.close()
    conn.close()


if __name__ == '__main__':  # 当程序执行时，调用函数,协调函数执行组织的流程(相当于主函数)
    main()
