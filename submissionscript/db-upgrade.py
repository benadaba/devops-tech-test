import json
import mysql.connector
from pathlib import Path
import argparse
import glob
import re
import collections
from contextlib import contextmanager


# Construct the argument parser
parser = argparse.ArgumentParser()

# Add the arguments to the parser
parser.add_argument("directory_with_sql_scripts", nargs='?',  help="directory with sql scripts")
parser.add_argument("username_for_the_db", nargs='?',  help="username for the db db-host")
parser.add_argument("db_host", nargs='?', default="localhost", help="db-host")
parser.add_argument("db_name", nargs='?',  default="devopstt" ,help="db name")
parser.add_argument("db_password", nargs='?',  help="db password", default=123456)
parser.add_argument("port", nargs='?',  default=3306,help="port")

args = parser.parse_args()



# get the passed arguments
HOST = args.db_host
PORT = args.port
USER = args.username_for_the_db
PASSWD = args.db_password
DATABASE = args.db_name
DBSCRIPTS = args.directory_with_sql_scripts

def get_list_of_sql_files(scripts_folder=DBSCRIPTS):
  """get list of sql files to execute
  :param scripts_folder- folder contain sql scripts
  :return list of sql files
  """
  list_of_files = glob.glob(f"../{scripts_folder}/*.sql")
  return list_of_files


def get_ordered_scripts_to_update(scripts_folder=DBSCRIPTS):
  """
  Get the ordered list of updates which are due to be done
  :param scripts_folder - the folder which contains the sql scripts
  :return ordered_updates - updates to be effected which is ordered
  """
  list_of_files= get_list_of_sql_files(scripts_folder)
  update_numbers = {}
  for file in list_of_files:
    #split actual filename from path
    filename = str(file.split("/")[-1])
    try:
      #extract only first number from file name
      num = re.search(r'\d+', filename).group()
    except:
      num = None
    # add to dictionary
    if num != None:
      update_numbers[num]=filename
  #order the dictionary keys in ascending order  
  ordered_updates = collections.OrderedDict(sorted(update_numbers.items()))
  return ordered_updates



 #implement database connection in a context manager
@contextmanager
def _connect():
  db_connection = mysql.connector.connect(
    host=HOST,
    port=PORT,
    user=USER,
    passwd=PASSWD,
    database=DATABASE
  )
  try:
    yield db_connection
  finally:
    db_connection.close()



def execute_seed_version_data():
  """
  Function to input seed data into sql database
  """
  #pick seed script from the relevant folder
  current_file = Path(f'../{DBSCRIPTS}/seed_data/seeddata.sql')
  # print(current_file)
  with open(current_file, 'r') as f:
      with _connect() as conn:
        cursor = conn.cursor()
        #set multi to True to read multiple sql statemetns from the script
        results = cursor.execute(f.read(), multi=True)
        try:
          for result in results:
            result.execute(result.statement)
        except RuntimeError:
          pass
      with _connect() as conn_commit:
        conn_commit.commit()


#run function
execute_seed_version_data()


def get_latest_version():
    """
    Get the latest update from the versionTable
    :return version of the latest update
    """
    mycursor = db_connection.cursor()
    mycursor.execute("SELECT max(version) as version FROM versionTable;")
    resultVersion = mycursor.fetchone()
    return resultVersion[0]

print(f"test_version: {get_latest_version()}")



def execute_db_upgrade_script():
  #whether or not execute multi-line code
  multiline= False
  #get the ordered list of sql files to be executed
  ordered_scripts_to_run = get_ordered_scripts_to_update()
  latest_version = get_latest_version()
  print(f"num_to_update_latest_version_to: {list(ordered_scripts_to_run.keys())[-1]}")
  #get the number to update as the very latest version
  num_to_update_latest_version_to =  list(ordered_scripts_to_run.keys())[-1]
  #loop through the list of sql scripts to run to update the database
  for k, v in ordered_scripts_to_run.items():
    print(f"currenlty running: {k}:{v}")
    #if script number is newer than last update version in DB then update the DB
    if int(k) >  int(latest_version):
      print(f"UPDATING!! file key :{k} with file value: {v}")
      current_file = Path(f'../{DBSCRIPTS}/{v}')
      with open(current_file, 'r') as f:
          with db_connection.cursor() as cursor:
              cursor.execute(f.read(), multi=multiline)
          db_connection.commit()
      #if the script number is the last update file to run, then update that in the versionTable
      if int(k) == int(num_to_update_latest_version_to):
        mycursor = db_connection.cursor()
        mycursor.execute(f"INSERT INTO versionTable (version) VALUES ({num_to_update_latest_version_to});")
        db_connection.commit()
    else:
      print(f"NOT updating this file key {k}: value: {v}")

execute_db_upgrade_script()


if __name__ == '__main__':
   pass