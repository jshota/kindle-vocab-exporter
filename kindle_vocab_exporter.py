import pandas as pd
import pathlib
import sqlite3
from datetime import datetime 
from shutil import copy2
from os import walk

date_format = "%m:%d:%y-%H%M%S"
date = datetime.now().strftime(date_format)

dst = str(pathlib.Path(__file__).parent.absolute())
dst_db = dst + "/" + date + ".db"
dst_csv = dst + "/" + date + ".csv"
src = "/Volumes/Kindle/system/vocabulary/vocab.db"

def load_db(path):
    cnx = sqlite3.connect(path)
    df = pd.read_sql_query("SELECT stem FROM WORDS", cnx)
    return df

def copy_db():
    copy2(src, dst_db)
    return

def output_df_to_csv(df):
    df.to_csv(dst_csv, index = False, header = False)
    return

def sort_by_dates(dates):
    try:
        dates.sort(key = lambda date: datetime.strptime(date, date_format + ".db"), reverse = True)
    except:
        print("File name format error: Please follow the specific time format in the script")
    return

def remove_duplicates(data):
    data.drop_duplicates(subset ="stem", keep=False, inplace=True)
    return

def load_files():
    f = []
    for (dirpath, dirnames, filenames) in walk(pathlib.Path(__file__).parent.absolute()):
        f = [s for s in filenames if ".db" in s]
        
    # if no previous db copy, copy the src db and directly output all words
    if (len(f) == 0):
        copy_db()
        output_df_to_csv(load_db(dst_db))
        return

    # otherwise, output the difference between the latest db copy and src db
    sort_by_dates(f)
    df = pd.concat([load_db(dst + "/" + f[0]), load_db(src)])
    df['stem'].str.lower
    
    remove_duplicates(df)
    output_df_to_csv(df)
    
    # make a latest copy
    copy_db()
    
    return
    
def main():
    load_files()
    
if __name__ == "__main__":
    main()