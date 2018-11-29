# -*- coding: utf-8 -*-
"""
Created on Wed Nov 28 14:20:58 2018

@author: jiangsheng
"""

import numpy as np
import psycopg2
import json
import os
import re
import sys
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import MetaData
import dask.dataframe as dd
import cStringIO
ipaddress="127.0.0.1"

engine=create_engine('postgresql+psycopg2://username:password@%s:5432/ticks_timescale'%ipaddress)

def littleDF_to_sql(df,item,engine):
    try:
        print "before insert"
        df=df.drop_duplicates('datetime')
        pd.io.sql.to_sql(df,item,con=engine,if_exists='append',index=False)
        print "%s data are inserted to %s ...."%(new_num,item)
    except Exception as e:
        print "error:%s"%e
        print "this csv have duplicate keys , insert one by one"
        df.loc[:,"exchange"]="null"
        conn=engine.connect()
        for lineNum in df.index:
            sqlcommand="""INSERT INTO "%s" VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s','%s','%s','%s','%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s') ON CONFLICT (datetime) DO NOTHING;"""%(item,df.iloc[lineNum,0],df.iloc[lineNum,1],df.iloc[lineNum,2],df.iloc[lineNum,3],df.iloc[lineNum,4],df.iloc[lineNum,5],df.iloc[lineNum,6],df.iloc[lineNum,7],df.iloc[lineNum,8],df.iloc[lineNum,9],df.iloc[lineNum,10],df.iloc[lineNum,11],df.iloc[lineNum,12],df.iloc[lineNum,13],df.iloc[lineNum,14],df.iloc[lineNum,15],df.iloc[lineNum,16],df.iloc[lineNum,17],df.iloc[lineNum,18],df.iloc[lineNum,19],df.iloc[lineNum,20],df.iloc[lineNum,21],df.iloc[lineNum,22],df.iloc[lineNum,23],df.iloc[lineNum,24],df.iloc[lineNum,25],df.iloc[lineNum,26],df.iloc[lineNum,27],df.iloc[lineNum,28])
            print "sqlcommand%s "%sqlcommand
            conn.execute(sqlcommand)        
        conn.close()

def largeCSV_to_sql(filepath,item,engine):
    reader=pd.read_csv(filepath,chunksize=150000,low_memory=False)
    for i, df in enumerate(reader):
        print "now%s item %s"%(i,item)
        df=df.drop_duplicates('datetime')
        try:
            output = cStringIO.StringIO()
            # ignore the index
            df.to_csv(output, sep='\t',index = False, header = False)
            output.getvalue()
            # jump to start of stream
            output.seek(0)

            connection = engine.raw_connection() #engine 是 from sqlalchemy import create_engine
            cursor = connection.cursor()
            # null value become ''
            cursor.copy_from(output,item,null='')
            connection.commit()
            cursor.close()

            # pd.io.sql.to_sql(df,item,con=engine,if_exists='append',index=False)
        except Exception as e:
            print "error:%s" % e
            print "this csv have duplicate keys , insert one by one"
            df.loc[:,"exchange"]="null"
            conn=engine.connect()
            for lineNum in df.index:
                sqlcommand="""INSERT INTO "%s" VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s','%s','%s','%s','%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s') ON CONFLICT (datetime) DO NOTHING;"""%(item,df.iloc[lineNum,0],df.iloc[lineNum,1],df.iloc[lineNum,2],df.iloc[lineNum,3],df.iloc[lineNum,4],df.iloc[lineNum,5],df.iloc[lineNum,6],df.iloc[lineNum,7],df.iloc[lineNum,8],df.iloc[lineNum,9],df.iloc[lineNum,10],df.iloc[lineNum,11],df.iloc[lineNum,12],df.iloc[lineNum,13],df.iloc[lineNum,14],df.iloc[lineNum,15],df.iloc[lineNum,16],df.iloc[lineNum,17],df.iloc[lineNum,18],df.iloc[lineNum,19],df.iloc[lineNum,20],df.iloc[lineNum,21],df.iloc[lineNum,22],df.iloc[lineNum,23],df.iloc[lineNum,24],df.iloc[lineNum,25],df.iloc[lineNum,26],df.iloc[lineNum,27],df.iloc[lineNum,28])
                print "sqlcommand%s "%sqlcommand
                conn.execute(sqlcommand)
            conn.close()