# -*- coding: utf-8 -*-
"""数据库连接池的封装"""
import pymysql
from dbutils.pooled_db import PooledDB
import multiprocessing
from config import mysql_config

CPU = multiprocessing.cpu_count()


class MySQLEngine(object):
    """MYSQL引擎封装"""

    __tablename__ = None
    placeholder = '%s'

    def connect(self, **kwargs):
        db_host = kwargs.get('db_host', 'localhost')
        db_port = kwargs.get('db_port', 3306)
        db_user = kwargs.get('db_user', 'root')
        db_pwd = kwargs.get('db_pwd', '')
        db = kwargs.get('db', '')
        cursor = kwargs.get('cursor', pymysql.cursors.Cursor)

        self.pool = PooledDB(pymysql, mincached=1, maxcached=CPU,
                             maxconnections=0, blocking=False, host=db_host,
                             user=db_user, passwd=db_pwd, db=db, port=db_port,
                             cursorclass=cursor,autocommit=True, charset='utf8mb4')

    def _execute(self, sql, values, **kwargs):
        conn = self.pool.connection()
        cur = conn.cursor()
        cur.execute(sql, values)
        # conn.commit()
        return cur, conn

    def select(self, sql, values=[], **kwargs):
        cur, conn = self._execute(sql, values, **kwargs)
        conn.close()
        for row in cur:
            yield row

    def execute(self, sql, values=[], **kwargs):
        cur, conn = self._execute(sql, values, **kwargs)
        conn.close()

    def update(self, table, data, **kwargs):
        """当数据不存在的时候，执行插入，如果存在则进行修改。(更新的参数必须存在唯一的值)
        :param table: 表名
        :param data: 字典数据
        :param kwargs: 其他数据
        :return:
        """
        assert isinstance(data, dict)

        sql = '''INSERT INTO {0} ({1}) VALUES ({2}) ON DUPLICATE KEY UPDATE {3}'''
        p0 = '`' + table + '`'  # 表名
        p1 = list()  # 字段名
        p3 = list()  # 字段的值
        values = list()
        for k, v in data.items():
            if v is None:
                continue

            k = '`' + k + '`'
            p1.append(k)
            p3.append('%s=VALUES(%s)' % (k, k))
            values.append(v)

        p2 = ['%s'] * len(p1)
        sql = sql.format(p0, ', '.join(p1), ', '.join(p2), ', '.join(p3))

        self.execute(sql, values, **kwargs)


class DBInterface(object):

    def __init__(self):
        self.result_engine = MySQLEngine()
        self.result_engine.connect(**mysql_config)






