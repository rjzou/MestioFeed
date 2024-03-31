from dotenv import dotenv_values

env_vars = dotenv_values('.env')
# print(env_vars)
_dbhost=env_vars["dbhost"]
_dbuser=env_vars["dbuser"]
_dbpass=env_vars["dbpass"]
_redishost=env_vars["redishost"]
_redisport=env_vars["redisport"]
_redisdb=env_vars["redisdb"]
_redispass=env_vars["redispass"]


mysql_config = {
    'db_host': _dbhost,
    'db_user': _dbuser,
    'db_pwd': _dbpass,
    'db': 'crypto_db',
    'db_port': 3306,
}
redis_config = {
    'host': _redishost,
    'port': _redisport,
    'password': _redispass,
    'db': _redisdb,
}

