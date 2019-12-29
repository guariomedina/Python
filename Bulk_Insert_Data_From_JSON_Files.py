'''
    Developer: Guarionex Medina
    Email: guario.medina@gmail.com
    Purpose: Progam to take a total listing of locally stored JSON files and pull data from each file to be bulk inserted into a SQL DB.
'''

import requests
import pandas as pd
import pyodbc
import sys

import fnmatch
import os
import click
from sqlalchemy import create_engine
import urllib
import tarfile
import json

location = r'C:\Users\filePath'

username = 'loginID'
password = 'loginPassword'

server = 'SQL Server'
database = 'SQL DB'

#message to user to identify where the Program is connecting to before ingesting.
print('Server connecting to: ', server, '\nDatabase being accessed: ', database)

#set the driver of SQL server to the latest neeed for connection as well as change the current working directory to where the files are stored.
driver = 'SQL Server Native Client 11.0'
os.chdir(location)
file_names = []

#Collect all files in the directory and store only the json copies into the array
file = os.listdir()
for i in file:
    if i.endswith(".json"):
        file_names.append(i)

# connect to the database
control = pyodbc.connect(Driver=driver, Server=server, DATABASE=database, Trusted_Connection='Yes'
                         , UID=username, PWD=password)
cur = control.cursor()

params = urllib.parse.quote_plus(
    'DRIVER={SQL Server Native Client 11.0};' +
    'SERVER=' + server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password)

engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)

# Checking Connection
connected = pd.io.sql._is_sqlalchemy_connectable(engine)
#used to validate connection was successfull
print(connected)


for i in file_names:
    #pull the date format from the file name to be used to identify which records came from which file.  Then import the data from the worksheet into a dataset for each file and bulk insert into the SQL DB.
    file_date = i[8:18]
    data = pd.read_json(i)
    print(i)
    print(data['title'].shape[0])
    row_count = data['title'].shape[0]
    # i = 0
    j = 0
    k = 0
    total_count = 0
    data2 = []

    while j <= row_count - 1:
        title = data['title'][j]
        brand = data['specs'][j]['brand']
        series = data['specs'][j]['series']
        model = data['specs'][j]['model']
        device_type = data['specs'][j]['device_type']
        printer_type = data['specs'][j]['printer_type']
        color_type = data['specs'][j]['color_type']
        speed = data['specs'][j]['speed']
        format = data['specs'][j]['format']
        resolution = data['specs'][j]['resolution']
        duty_cycle = data['specs'][j]['duty_cycle']
        double_sided = data['specs'][j]['double_sided']
        scanner_resolution = data['specs'][j]['scanner_resolution']

        offer_ct = len(data['offers'][j])
        

        while k <= offer_ct - 1:
            company = data['offers'][j][k]['company']
            company_id = data['offers'][j][k]['company_id']
            country = data['offers'][j][k]['country']
            country_id = data['offers'][j][k]['country_id']
            url = data['offers'][j][k]['url']
            price = data['offers'][j][k]['price']
            promo = data['offers'][j][k]['promo']
            stock = data['offers'][j][k]['stock']
            timestamp = data['offers'][j][k]['timestamp']

            data2.append([file_date, title, brand, series, model, device_type, printer_type, color_type, speed, format,
                          resolution, duty_cycle,
                          double_sided, scanner_resolution, company, company_id, country, country_id, url, price, promo,
                          stock, timestamp])
            k += 1
            total_count += 1
        

        j += 1
        k = 0

    titles = ['File_Date', 'Title', 'Brand', 'Series', 'Model', 'Device_Type', 'Printer_Type', 'Color_Type', 'Speed',
              'Format',
              'Resolution', 'Duty_Cycle', 'Double_Sided', 'Scanner_Resolution', 'Company', 'Company_ID', 'Country',
              'Country_ID', 'URL', 'Price', 'Promo', 'Stock', 'Timestamp']

    #Load the dataset with the titles to be ready for bulk insert
    df = pd.DataFrame(data2, columns=titles)

    tsql_chunksize = 10000

    df.to_sql('TableName', con=engine, if_exists='append', index=False, chunksize=tsql_chunksize)

    #Print record count of each file to validate everthing was inserted as expected.
    print(f"Total number of records in the database is {total_count}.")


