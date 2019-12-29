'''
    Developer: Guarionex Medina
    Email: guario.medina@gmail.com
    Purpose: Program to look at a specific file location for 3 different type of xlsx files that contain different format for each.  The total number of files may vary per file type but the format between them will not change.  The 3 formats are AMS, EMEA, and APJ.
    Modified: This program was initially created to drop the tales and recreate them for specific fields.  This has been modified to do a bulk insert of the data based on what is present in the file for columns as this may adapt to any changes in the future needed.  All data processing is done on the SQL Server for analysis.
'''

import pandas as pd
import pyodbc
import fnmatch
import os
import click
from sqlalchemy import create_engine
import urllib

# connect to sql server

server = 'SQL Server'
database = 'SQL DB'


driver = 'SQL Server Native Client 11.0'

username = 'loginID'
password = 'loginPassword'
location = r'\\FilePath'

control = pyodbc.connect(Driver=driver, Server=server, DATABASE=database, Trusted_Connection='Yes')
cur = control.cursor()

params = urllib.parse.quote_plus(
'DRIVER={SQL Server Native Client 11.0};'+
'SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)

engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)

#Checking Connection
connected = pd.io.sql._is_sqlalchemy_connectable(engine)

# set of static variables

tables = ['ChannelDNA_AMS', 'ChannelDNA_APJ','ChannelDNA_EMEA']


file_names = []
workbook_names = ['Data']

print("Inserting data into: " + server)

# change location then get list of xlsx files and add them to empty array
os.chdir(location)
file = os.listdir()
for i in file:
    if i.endswith(".xlsx"):
        file_names.append(i)



def progress(j, total):
    #Quick method to determine the total amount remaining to be inserted per file
    click.clear()
    percent = str(int(round((j / total) * 100, 0))) + ' % Complete'

    return percent


# beginning of functions to be used later
def logger(shredder, download_status, extraction_status, loading_status, file_names):
    _shredder = shredder
    _download_status = download_status
    _extraction_status = extraction_status
    _loading_status = loading_status
    _file_name = file_names

    cur.execute(
        "Insert into dbo.datashreddervalidationreport(DataShredder, DownloadStatus, ExtractionStatus, LoadingStatus,"
        + "RunTime, FileName, FileDate, ReportValidationVersion)"
        + "VALUES('" + _shredder + "','" + _download_status + "','" + _extraction_status + "','" + _loading_status + "', CURRENT_TIMESTAMP,"
        + "'" + _file_name + "' , CURRENT_TIMESTAMP, '0')"
    )
    cur.execute("COMMIT")


def del_tables(table_name):
    _table_name = table_name
    try:
        cur.execute(
            "Drop table IF EXISTS " + _table_name
        )
        cur.execute("COMMIT")
        print(table_name + " TABLE DELETED")
    except:
        logger('ChannelDNA', 'Failed to delete ' + _table_name + ' table', 'Fail', 'Table not removed', _table_name)
        pass


def ams_data(files, workbooks, tables):
    _files = files
    _workbooks = workbooks
    _tables = tables

    tbl = _tables[0]
    del_tables(tbl)

    init = 0

    for i in _files:
        # print(i)
        # print(init)

        # set index for worksheet names and tables to be inserted into
        # sheet = _workbooks[init]
        # tbl = _tables[init]
        sheet = _workbooks[0]
        #tbl = _tables[0]

        # _xl_data = pd.read_excel(i, sheet_name= sheet, header = 12, skipfooter=2)
        _xl_data = pd.read_excel(i, sheet_name=sheet)

        # replace null values in dataframes
        _xl_data = _xl_data.fillna("")
        _xl_data = _xl_data.replace(to_replace="'", value="_", regex=True)

        tsql_chunksize = 10000
        # cap at 1000 (limit for number of rows inserted by table-value constructor)
        #tsql_chunksize = 1000 if tsql_chunksize > 1000 else tsql_chunksize
        print(tsql_chunksize)

        _xl_data.to_sql(tbl, con=engine, if_exists='append', index=False, chunksize=tsql_chunksize)

        print(f"Completed loading from ", i)

        logger('ChannelDNA_AMS', 'DownloadSucceeded', 'ExtractionSucceeded', 'LoadingSucceeded', i)


def apj_data(files, workbooks, tables):
    _files = files
    _workbooks = workbooks
    _tables = tables

    tbl = _tables[0]
    del_tables(tbl)

    init = 0

    for i in _files:
        # print(i)
        # print(init)

        # set index for worksheet names and tables to be inserted into
        # sheet = _workbooks[init]
        # tbl = _tables[init]
        sheet = _workbooks[0]
        tbl = _tables[0]

        # _xl_data = pd.read_excel(i, sheet_name= sheet, header = 12, skipfooter=2)
        _xl_data = pd.read_excel(i, sheet_name=sheet)

        # replace null values in dataframes
        _xl_data = _xl_data.fillna("")
        _xl_data = _xl_data.replace(to_replace="'", value="_", regex=True)

        tsql_chunksize = 10000
        # cap at 1000 (limit for number of rows inserted by table-value constructor)
        #tsql_chunksize = 1000 if tsql_chunksize > 1000 else tsql_chunksize
        print(tsql_chunksize)

        _xl_data.to_sql(tbl, con=engine, if_exists='append', index=False, chunksize=tsql_chunksize)

        print(f"Completed loading from ", i)

        logger('ChannelDNA_APJ', 'DownloadSucceeded', 'ExtractionSucceeded', 'LoadingSucceeded', i)



def emea_data(files, workbooks, tables):
    _files = files
    _workbooks = workbooks
    _tables = tables

    tbl = _tables[0]
    del_tables(tbl)

    init = 0
    # print(_files[0:2])

    for i in _files:
        # print(i)
        # print(init)

        # set index for worksheet names and tables to be inserted into
        # sheet = _workbooks[init]
        # tbl = _tables[init]
        sheet = _workbooks[0]
        tbl = _tables[0]

        # _xl_data = pd.read_excel(i, sheet_name= sheet, header = 12, skipfooter=2)
        _xl_data = pd.read_excel(i, sheet_name=sheet)

        # replace null values in dataframes
        _xl_data = _xl_data.fillna("")
        _xl_data = _xl_data.replace(to_replace="'", value="_", regex=True)

        tsql_chunksize = 10000
        # cap at 1000 (limit for number of rows inserted by table-value constructor)
        #tsql_chunksize = 1000 if tsql_chunksize > 1000 else tsql_chunksize
        print(tsql_chunksize)

        _xl_data.to_sql(tbl, con=engine, if_exists='append', index=False, chunksize=tsql_chunksize)

        print(f"Completed loading from ", i)

        logger('ChannelDNA_APJ', 'DownloadSucceeded', 'ExtractionSucceeded', 'LoadingSucceeded', i)

def create_tables(table_name):
    _table_name = table_name
    stmt = "SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '" + _table_name + "'"
    result = cur.execute(stmt)
    response = result.fetchall()

    add_ams = (
            "CREATE TABLE " + _table_name +
            " ( " +
            "[Calendar Week Ending] [nvarchar](255) NULL, " +
            "[Reporter HP Organization] [nvarchar](255) NULL, " +
            "[Product Line] [nvarchar](255) NULL, " +
            "[Product Sub Group] [nvarchar](255) NULL, " +
            "[Product Identifier {PH Web}] [nvarchar](255) NULL, " +
            "[Product Description] [nvarchar](255) NULL, " +
            "[Product Type Desc] [nvarchar](255) NULL, " +
            "[Rep SRC Channel Category] [nvarchar](255) NULL, " +
            "[Rep SRC Channel Segment] [nvarchar](255) NULL, " +
            "[Rep SRC Company Name] [nvarchar](255) NULL, " +
            "[SellThru Product Units] [nvarchar](255) NULL, " +
            "[SellTo Product Units] [nvarchar](255) NULL, " +
            "[Partner Aggregated SellTo Product Units] [nvarchar](255) NULL, " +
            "[Shipments Product Units] [nvarchar](255) NULL, " +
            "[Total Inventory Product Units] [nvarchar](255) NULL, " +
            "[Calendar Year] [nvarchar](255) NULL, " +
            "[Calendar Month] [nvarchar](255) NULL, " +
            "[Calendar Week] [nvarchar](255) NULL, " +
            "[Product Category Desc] [nvarchar](255) NULL, " +
            "[Product Category Code] [nvarchar](255) NULL, " +
            "[Product Option Code] [nvarchar](255) NULL, " +
            "[Original Product Identifier] [nvarchar](255) NULL, " +
            "[Product Type] [nvarchar](255) NULL, " +
            "[Product Model] [nvarchar](255) NULL, " +
            "[Product Model Desc] [nvarchar](255) NULL, " +
            "[Product Family] [nvarchar](255) NULL, " +
            "[Product Family Desc] [nvarchar](255) NULL, " +
            "[Product Sub Family] [nvarchar](255) NULL, " +
            "[Product Sub Family Desc] [nvarchar](255) NULL, " +
            "[Historical Product Line] [nvarchar](255) NULL, " +
            "[Product Line Source Flag] [nvarchar](255) NULL, " +
            "[Business Unit Desc] [nvarchar](255) NULL, " +
            "[Sales Channel Prod Seg Desc] [nvarchar](255) NULL, " +
            " ) COMMIT"
    )

    add_apj = (
            "CREATE TABLE " + _table_name +
            " ( " +
            "[Fiscal Week] [nvarchar](255) NULL, " +
            "[Seller Country Name] [nvarchar](255) NULL, " +
            "[Sub Group] [nvarchar](255) NULL, " +
            "[Product Name] [nvarchar](255) NULL, " +
            "[Product Number] [nvarchar](255) NULL, " +
            "[Product Type Name] [nvarchar](255) NULL, " +
            "[Product Line ID] [nvarchar](255) NULL, " +
            "[Seller English Company Name] [nvarchar](255) NULL, " +
            "[Seller Partner Type] [nvarchar](255) NULL, " +
            "[Seller Retail Sub Segment] [nvarchar](255) NULL, " +
            "[SellThru Qty] [nvarchar](255) NULL, " +
            "[Sell To Qty] [nvarchar](255) NULL, " +
            "[Total Inventory Quantity] [nvarchar](255) NULL, " +
            "[Total Shipments Quantity] [nvarchar](255) NULL, " +
            "[Product Base Name] [nvarchar](255) NULL, " +
            "[Product Base Number] [nvarchar](255) NULL, " +
            "[Product Category] [nvarchar](255) NULL, " +
            "[Product Line Name] [nvarchar](255) NULL, " +
            "[Product Type] [nvarchar](255) NULL, " +
            "[Seller Hierarchy Type] [nvarchar](255) NULL, " +
            "[Seller Channel Touch] [nvarchar](255) NULL, " +
            "[Fiscal Month] [nvarchar](255) NULL, " +
            "[Non T1 Shipments Quantity] [nvarchar](255) NULL, " +
            "[T1 Shipments Quantity] [nvarchar](255) NULL, " +

            " ) COMMIT"
    )

    add_emea = (
            "CREATE TABLE " + _table_name +
            " ( " +
            "[Week Ending] [nvarchar](255) NULL, " +
            "[Reporter Country] [nvarchar](255) NULL, " +
            "[Current Product Segment] [nvarchar](255) NULL, " +
            "[Current PL] [nvarchar](255) NULL, " +
            "[Product Type] [nvarchar](255) NULL, " +
            "[Product Number] [nvarchar](255) NULL, " +
            "[Product Number Label] [nvarchar](255) NULL, " +
            "[Rep PType] [nvarchar](255) NULL, " +
            "[Rep Ch.Seg] [nvarchar](255) NULL, " +
            "[Rep Name] [nvarchar](255) NULL, " +
            "[T1 Sell-thru Product Units] [nvarchar](255) NULL, " +
            "[Sell-to Product Units] [nvarchar](255) NULL, " +
            "[Shipments Product Units] [nvarchar](255) NULL, " +
            "[Inventory Product Units] [nvarchar](255) NULL, " +
            "[Month] [nvarchar](255) NULL, " +
            "[Week] [nvarchar](255) NULL, " +
            "[Year] [nvarchar](255) NULL, " +
            "[Current Business Unit] [nvarchar](255) NULL, " +
            "[Current Product Group] [nvarchar](255) NULL, " +
            "[Current Product Category] [nvarchar](255) NULL, " +
            "[Product Family] [nvarchar](255) NULL, " +
            "[Product Option] [nvarchar](255) NULL, " +
            "[Product Option Label] [nvarchar](255) NULL, " +
            "[T1 Direct Retail Sell-to Product Units] [nvarchar](255) NULL, " +
            "[GTM Sell-thru Product Units] [nvarchar](255) NULL, " +
            "[Sell-thru Product Units] [nvarchar](255) NULL, " +
            " ) COMMIT "
    )

    try:
        if len(response) == 0:
            tblList = _table_name
            if fnmatch.fnmatch(tblList, '*AMS*'):
                cur.execute(add_ams)
                print(_table_name + ' TABLE DOES NOT EXIST')
            elif fnmatch.fnmatch(tblList, '*APJ*'):
                cur.execute(add_apj)
                print(_table_name + ' TABLE DOES NOT EXIST')
            elif fnmatch.fnmatch(tblList, '*EMEA*'):
                cur.execute(add_emea)
                print(_table_name + ' TABLE DOES NOT EXIST')

        else:
            del_tables(_table_name)
            tblList = _table_name
            if fnmatch.fnmatch(tblList, '*AMS*'):
                cur.execute(add_ams)
                print(_table_name + ' TABLE WAS RECREATED')
            elif fnmatch.fnmatch(tblList, '*APJ*'):
                cur.execute(add_apj)
                print(_table_name + ' TABLE WAS RECREATED')
            elif fnmatch.fnmatch(tblList, '*EMEA*'):
                cur.execute(add_emea)
                print(_table_name + ' TABLE WAS RECREATED')
    except:
        logger('ChannelDNA', 'Failed to create ' + _table_name + ' table', 'Fail', 'Table not added', _table_name)
        pass


def insert_data(files, workbooks, table_names):
    try:
        _files = files
        _workbooks = workbooks
        _tables = table_names

        _ams = []
        _apj = []
        _emea = []

        _tbl1 = []
        _tbl2 = []
        _tbl3 = []

        for file in _files:
            if fnmatch.fnmatch(file, '*AMS*.xlsx'):
                _ams.append(file)
            elif fnmatch.fnmatch(file, '*APJ*.xlsx'):
                _apj.append(file)
            elif fnmatch.fnmatch(file, '*EMEA*.xlsx'):
                _emea.append(file)

        for tblList in _tables:
            if fnmatch.fnmatch(tblList, '*AMS*'):
                _tbl1.append(tblList)
            elif fnmatch.fnmatch(tblList, '*APJ*'):
                _tbl2.append(tblList)
            elif fnmatch.fnmatch(tblList, '*EMEA*'):
                _tbl3.append(tblList)


        ams_data(_ams,_workbooks,_tbl1)
        apj_data(_apj, _workbooks, _tbl2)
        emea_data(_emea, _workbooks, _tbl3)



    except:
        logger('ChannelDNA', 'Failed to insert data', 'Fail', 'ChannelDNA not imported/completed', 'ChannelDNA')
        pass


# sequence of events to run


# step 1 test if tables exist, if they do remove them and recreate.  if they don't then just add them.
# This used to be the method at first until the program was altered to use bulk insert that would essentially take care of this process and be built upon what is actually in the files.
'''
for i in tables:
    create_tables(i)
'''

# step 2 attempt to insert the data from the tables
insert_data(file_names, workbook_names, tables)

# finally close the db connection
cur.close()
