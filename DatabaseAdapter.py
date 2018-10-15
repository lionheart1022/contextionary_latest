import config
from psycopg2 import connect , sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

class connectHost(object):
  def __init__(self):
      self.connection = connect(user = config.DATABASE['user'], host = config.DATABASE['host'], password = config.DATABASE['password'])
      self.connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

  def dropDB(self):
    cur = self.connection.cursor() 
    cur.execute("""SELECT EXISTS(SELECT datname FROM pg_catalog.pg_database WHERE datname = %s)""", (config.DATABASE['dbname'],))
    fetch_exists = cur.fetchone()

    if fetch_exists[0]:
      try:    
        strSQL = sql.SQL('''SELECT pid, pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = %s AND pid <> pg_backend_pid()''')
        cur.execute(strSQL, ([config.DATABASE['dbname']]))
      finally:
        cur.close()

      # # (2) drop database
      cur = self.connection.cursor()
      try:
        strSQL = sql.SQL("DROP DATABASE {}")
        cur.execute(strSQL.format(sql.Identifier(config.DATABASE['dbname'])))
      finally:
        # print("Dropped {} database".format(self.dbname))
        cur.close()

  def __del__(self):
      self.connection.close()

class connectDB(object):
  def __init__(self):
      self.connection = connect(user = config.DATABASE['user'], host = config.DATABASE['host'], password = config.DATABASE['password'], dbname = config.DATABASE['dbname'])  
      self.connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

  # def query(self, query, params):
  #     return self._db_cur.execute(query, params)

  def __del__(self):
      self.connection.close()