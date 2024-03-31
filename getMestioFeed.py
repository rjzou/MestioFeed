
import logging
import json
from pymysql.converters import escape_string
from MySQLEngine import DBInterface
from RedisInterface import RedisInterface
from HttpRequestPool import *



# 配置日志记录
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
  mysql_db = DBInterface()
  mysql_connection = mysql_db.result_engine
  redis_db = RedisInterface()
  redis_client=redis_db.fraud

  _page = 1
  while True:
    # 接口URL和请求头
    url = 'https://api.mest.io/api/v1/feeds'
    params = {
      'page': _page,
      'per_page': 10,
      'count': 'true',
      'sortBy': 'createdAt',
      'sort': 'desc'
    }

    headers = {
      'accept': 'application/json',
      'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8,zh-TW;q=0.7',
      'authorization': '替换认证信息',
      'cache-control': 'no-cache',
      'content-type': 'application/json',
      'origin': 'https://app.mest.io',
      'pragma': 'no-cache',
      'priority': 'u=1, i',
      'referer': 'https://app.mest.io/',
      'sec-ch-ua': '"Google Chrome";v="125", "Chromium";v="125", "Not.A/Brand";v="24"',
      'sec-ch-ua-mobile': '?0',
      'sec-ch-ua-platform': '"macOS"',
      'sec-fetch-dest': 'empty',
      'sec-fetch-mode': 'cors',
      'sec-fetch-site': 'same-site',
      'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36'
    }
    # 获取接口数据
    # session = requests.Session()
    # session.trust_env = False
    # response = session.get(url, headers=headers, params=params)

    response=http_req(url=url, headers=headers,data=params,method='GET')
    data = response.json()

    _offset=1000
    if redis_client.get("feeds_offset")!=None:
      _offset = int(redis_client.get("feeds_offset"))

    _isStop = False
    try:
      for item in data:
        if _offset >= int(item["id"]):
          _isStop = True
          break
        name = 'feeds_' + str(item['id'])
        for key in item:
          print(str(key) + '=' + str(item[key]))
          # 将数据存储到Redis
          redis_client.hset(name, str(key), str(item[key]))
          redis_client.sadd("get_feeds_list_page" + str(_page), name)


    except Exception as e:
      print(f"执行redis过程中出现异常: {e}")
    if _isStop == True:
      break
    redis_client.set("feeds_page", _page)
    _page = _page + 1

  _page = int(redis_client.get("feeds_page"))
  for i in range(_page, 0, -1):
    try:
      _scard = redis_client.scard("get_feeds_list_page" + str(i))
      _members = redis_client.smembers("get_feeds_list_page" + str(i))
      _members = list(set(_members))
      _members.sort()
      for member in _members:
        print(member)
        _entity = redis_client.hgetall(member)
        item = {}
        for key, value in _entity.items():
          item[key.decode('utf-8')] = value.decode('utf-8')

        table="TT_FEEDS_DATA"
        data={'id':int(item['id']),'authorId':int(item['authorId']),'title':escape_string(str(item['title'])),'url':escape_string(str(item['url'])),'description':escape_string(str(item['description']))
          ,'addresses':escape_string(str(item['addresses'])),'transactionHashes':escape_string(str(item['transactionHashes'])),'viewOptions':escape_string(str(item['viewOptions'])),'lang':escape_string(str(item['lang']))
          ,'status':escape_string(str(item['status'])),'rejectedReason':escape_string(str(item['rejectedReason'])),'pin':escape_string(str(item['pin'])),'archived':escape_string(str(item['archived']))
          ,'createdAt':escape_string(str(item['createdAt'])),'updatedAt':escape_string(str(item['updatedAt'])),'likes':escape_string(str(item['likes'])),'connects':escape_string(str(item['connects']))
              ,'author':escape_string(str(item['author'])),'_count':escape_string(str(item['_count']))}
        mysql_connection.update(table,data)
        table="TT_FEEDS_AUTHOR"

        author_json=json.loads(item['author'].replace('\'', '"'))
        data={'id':author_json['id'],'role':author_json['role'],'address':author_json['address'],'nickname':author_json['profile']['nickname'],'username':author_json['profile']['username'],'avatarType':author_json['profile']['avatarType']}
        mysql_connection.update(table, data)

        redis_client.set("feeds_offset", item["id"])
        redis_client.set("feeds_page", i)
        print("数据已成功存储到Redis和MySQL数据库中。")
    except Exception as e:
      print(f"执行mysql过程中出现异常: {e}")


if __name__ == "__main__":
  main()
