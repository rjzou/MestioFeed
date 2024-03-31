# -*- coding: utf-8 -*-
import requests
import logging

pool_connections = 2
pool_maxsize = 5

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_http_pool(pool_connections, pool_maxsize):
    # 实例化会话对象
    session = requests.Session()
    session.trust_env = False
    # 创建适配器
    adapter = requests.adapters.HTTPAdapter(pool_connections=pool_connections, pool_maxsize=pool_maxsize)
    # 告诉requests，http协议和https协议都使用这个适配器
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session


http_pool = get_http_pool(pool_connections=pool_connections, pool_maxsize=pool_maxsize)


def http_req(url, headers=None, data='', timeout=5, method="POST", max_retries=3):
    """发送HTTP请求, 不是所有的连接异常都重试"""
    res_data = ''
    if method not in ["GET", "POST"]:
        raise Exception('post requests method error!!!')
    while max_retries > 0:
        try:
            if method == "GET":
                res_data = http_pool.get(url, headers=headers, params=data, timeout=timeout)
                if res_data.ok==True:
                  break
            else:
                res_data = http_pool.post(url, headers=headers, data=data, timeout=timeout)
                if res_data.ok == True:
                  break
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
            max_retries -= 1
        except Exception as e:
            max_retries = 0
            logging.error('requests error is %s', e)

    if not res_data:
        raise Exception('post requests failed')
    return res_data
