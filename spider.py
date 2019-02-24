# -*- coding: utf-8 -*-
# @Time    : 2019/2/9 16:36
# @Author  : tangshao
# @File    : spider.py

from urllib.parse import urlencode
from urllib3.exceptions import RequestError
import requests
from pyquery import PyQuery as pq
import random
import time
import sqlite3


def get_one_page(url):
    try:
        headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Cache-Control': 'max-age=0',
            'Connection': 'keep-alive',
            'Cookie': 'll="108288"; bid=ewfWiqJcE_o; _vwo_uuid_v2=D6C1F959087B5D18EFABCD8FC85063547|bf13f44307f97013348ffc52182c77d0; douban-fav-remind=1; gr_user_id=c336349c-0706-44bd-aed5-b7987f121ea0; viewed="26657923_26297606_30173648_30190201_27092894"; CNZZDATA1272964020=916575408-1530418839-https%253A%252F%252Fwww.baidu.com%252F%7C1543126825; __utmz=30149280.1549598975.24.23.utmcsr=baidu|utmccn=(organic)|utmcmd=organic; __utmz=223695111.1549700126.10.10.utmcsr=baidu|utmccn=(organic)|utmcmd=organic|utmctr=%E8%B1%86%E7%93%A3; push_noty_num=0; push_doumail_num=0; __utmv=30149280.17489; ct=y; __utmc=30149280; __utmc=223695111; _pk_ref.100001.4cf6=%5B%22%22%2C%22%22%2C1551001876%2C%22https%3A%2F%2Fwww.baidu.com%2Fs%3Fwd%3D%25E8%25B1%2586%25E7%2593%25A3%26rsv_spt%3D1%26rsv_iqid%3D0xd22734520007181a%26issp%3D1%26f%3D8%26rsv_bp%3D0%26rsv_idx%3D2%26ie%3Dutf-8%26tn%3Dbaiduhome_pg%26rsv_enter%3D1%26rsv_sug3%3D7%26rsv_sug1%3D4%26rsv_sug7%3D100%26rsv_sug2%3D0%26inputT%3D719%26rsv_sug4%3D720%22%5D; _pk_ses.100001.4cf6=*; __utma=30149280.1821080304.1530422191.1550997710.1551001876.30; __utma=223695111.1450071852.1530422191.1550997710.1551001876.14; __utmb=223695111.0.10.1551001876; dbcl2="174896975:TLGAIt5Dys4"; ck=AjeU; _pk_id.100001.4cf6=85b6b69924774563.1530422191.14.1551004939.1550998567.; __utmt_douban=1; __utmb=30149280.53.9.1551004283697; ap_v=0,6.0',
            'Host': 'movie.douban.com',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36'
        }
        response = requests.get(url=url, headers=headers)
        if response.status_code == 200:
            return response.text
        else:
            return None
    except RequestError as e:
        return None

def parse_one_page(html):
    infos = []
    html = pq(html)
    comments = html('#comments').children()
    for comment in comments.items():
        data_cid = comment('.comment-item').attr('data-cid')
        nick_name = comment('.comment-info').text()
        write_date = comment('.comment-time ').attr('title')
        score = comment('.comment-info .rating').attr('title')
        votes = comment('.votes').text()
        short = comment('.short').text()
        info = {
            'data_cid':data_cid,
            'nick_name':nick_name,
            'write_date':write_date,
            'score':score,
            'votes':votes,
            'short':short
        }
        if data_cid == None:
            continue
        else:
            infos.append(info)
    return infos

def create_table():
    try:
        conn=sqlite3.connect('earth_data.db')
        print('连接数据库成功！')
        cur = conn.cursor()
        create_sql = '''
        CREATE TABLE earth_data (
        id  integer  primary key  autoincrement not null,
        data_cid   integer,
        nick_name  character,
        write_date  character,
        score  character,
        votes  integer,
        short character
        );
        '''
        cur.execute(create_sql)
        print('创建表成功！')
        conn.commit()
    except sqlite3.Error as e:
        print('创建表出错！')
    finally:
        cur.close()
        conn.close()
        print('数据库已关闭！')

def write_data(infos):
    try:
        conn = sqlite3.connect('earth_data.db')
        cur = conn.cursor()
        print('写入连接数据库成功！')
        write_sql = "insert into earth_data (data_cid, nick_name, write_date, score, votes, short) values (?, ?, ?, ?, ?, ?);"
        for info in infos:
            cur.execute(write_sql, (info['data_cid'], info['nick_name'], info['write_date'], info['score'], info['votes'], info['short']))
        conn.commit()
        print('写入数据成功！')
    except sqlite3.Error as e:
        print(e)
    finally:
        cur.close()
        conn.close()
        print('写入关闭数据库成功！')

def main():
    create_table()
    base_url = 'https://movie.douban.com/subject/26266893/comments?'
    for page in range(0, 481, 20):
        queries = {
            'start': page,
            'limit': '20',
            'sort': 'new_score',
            'status': 'P'
        }
        url = base_url + urlencode(query=queries)
        html = get_one_page(url=url)
        infos = parse_one_page(html=html)
        print(infos)
        write_data(infos=infos)
        time.sleep(random.random()*15)
        print('正在打印{}页'.format(page))

if __name__ == '__main__':
    main()