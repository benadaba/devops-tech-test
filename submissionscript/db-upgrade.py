import json
import mysql.connector


HOST = "localhost"
PORT = 3306
USER = "root"
PASSWD = ""
DATABASE = "devopstt"
DBSCRIPTS = "dbscripts"


db_connection = mysql.connector.connect(
  host=HOST,
  port=PORT,
  user=USER,
  passwd=PASSWD,
  database=DATABASE
)
