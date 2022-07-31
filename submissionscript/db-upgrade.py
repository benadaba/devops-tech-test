import json
import mysql.connector
from pathlib import Path
import argparse
import glob
import re
import collections


# Construct the argument parser
parser = argparse.ArgumentParser()

# Add the arguments to the parser
parser.add_argument("-d", "--directory_with_sql_scripts", required=True, help="directory with sql scripts")
parser.add_argument("-u", "--username_for_the_db", required=True, help="username for the db db-host")
parser.add_argument("-ht", "--db_host", required=True, default="localhost", help="db-host")
parser.add_argument("-db", "--db_name", required=True, default="devopstt" ,help="db name")
parser.add_argument("-pw", "--db_password", required=True, help="db password", default=123456)
parser.add_argument("-pt", "--port", required=False, default=3306,help="port")

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