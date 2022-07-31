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


def execute_seed_version_data():
  """
  Function to input seed data into sql database
  """
  #pick seed script from the relevant folder
  current_file = Path(f'../{DBSCRIPTS}/seed_data/seeddata.sql')
  # print(current_file)
  with open(current_file, 'r') as f:
      with db_connection.cursor() as cursor:
        #set multi to True to read multiple sql statemetns from the script
          cursor.execute(f.read(), multi=True)
      db_connection.commit()

#run function
execute_seed_version_data()