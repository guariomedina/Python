'''
    Developer: Guarionex Medina
    Email: guario.medina@gmail.com
    Purpose: Pull 4 xlsx files and import them into seperate tables for further analysis.
    '''

import pandas as pd
import pyodbc
import os

# connect to sql server

server = 'SQL Server'
database = 'SQL DB'


driver = 'SQL Server Native Client 11.0'

control = pyodbc.connect(Driver=driver, Server=server, DATABASE=database, Trusted_Connection='Yes')
cur = control.cursor()

# set of static variables

tables = ['tbl1', 'tbl2','tbl3', 'tbl4']
file_names = ['file1', 'file2','file3', 'file4']
workbook_names = ['workbook1', 'workbook2','workbook3', 'workbook4']

location = r'\\FilePath

def progress(j,total):
    percent = str(int(round((j/total)*100,0))) + ' % Complete'
    return percent

# beginning of functions to be used later
def logger(shredder, download_status, extraction_status, loading_status, file_names ):
    _shredder = shredder
    _download_status = download_status
    _extraction_status = extraction_status
    _loading_status  = loading_status
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
            "Drop table " + _table_name
            )
        cur.execute("COMMIT")
        print(table_name + " TABLE DELETED")
    except:
        logger('Plans','Failed to delete Plans tables','Fail','Table not removed', _table_name)
        pass


def create_tables(table_name):
    _table_name = table_name
    stmt = "SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '" + _table_name + "'"
    result = cur.execute(stmt)
    response = result.fetchall()

    try:
        add = (
                "CREATE TABLE " + _table_name +
                " ( Country nvarchar(128), " +
                "Business nvarchar(128), " +
                "Platform nvarchar(128), " +
                "Brand	nvarchar(128), " +
                "Product nvarchar(128), " +
                "KeyCompBrand nvarchar(128), " +
                "KeyCompProduct nvarchar(128), " +
                "Comp2Brand nvarchar(128), " +
                "Comp2Product nvarchar(128), " +
                "Comp3Brand nvarchar(128), " +
                "Comp3Product nvarchar(128), " +
                "TargetPFV nvarchar(128), " +
                "UpdatedTimeStamp nvarchar(128), " +
                "NPITargetPriceLocal nvarchar(128), " +
                "[Plan Type] nvarchar(128), " +
                "Notes nvarchar(128), " +
                "Flag nvarchar(128), )"
        )

        if len(response) == 0:
            cur.execute(add)
            cur.execute("COMMIT")
            print(_table_name + ' TABLE DOES NOT EXIST')
        else:
            del_tables(_table_name)
            cur.execute(add)
            cur.execute("COMMIT")
            print(_table_name + ' TABLE WAS RECREATED')
    except:
        logger('Plans','Failed to create Plans tables','Fail','Tables not added', _table_name)
        pass

def insert_data(files , workbooks, table_names):
    try:
        _files = files
        _workbooks = workbooks
        _tables = table_names

        init = 0



        for i in _files:

            #set index for worksheet names and tables to be inserted into
            sheet = _workbooks[init]
            tbl = _tables[init]

            _xl_data = pd.read_excel(i, sheet_name= sheet)

            #replace null values in dataframes
            _xl_data = _xl_data.fillna("")



            _country = _xl_data['Country']
            _business = _xl_data['Business']
            _platform = _xl_data['Platform']
            _brand = _xl_data['Brand']
            _product = _xl_data['Product']
            _keycompbrand = _xl_data['KeyCompBrand']
            _keycompproduct = _xl_data['KeyCompProduct']
            _comp2brand = _xl_data['Comp2Brand']
            _comp2product = _xl_data['Comp2Product']
            _comp3brand = _xl_data['Comp3Brand']
            _comp3product = _xl_data['Comp3Product']
            _targetpfv = _xl_data['TargetPFV']
            _updatedtimestamp = _xl_data['UpdatedTimeStamp']
            _npitargetpricelocal = _xl_data['NPI Target Price (LC)']
            _plantype = _xl_data['Plan Type']



            total = len(_xl_data)
            _current_count = 1

            for j in _country.index:
            #for j in range(0,403):


                cur.execute(
                            "Insert into dbo." + tbl + " (Country, Business, Platform, Brand, Product, KeyCompBrand,"
                            + " KeyCompProduct, Comp2Brand, Comp2Product, Comp3Brand, Comp3Product, TargetPFV, "
                            + " UpdatedTimestamp, [Plan Type], NPITargetPriceLocal) "
                            + "VALUES('"
                            + str(_country[j]) + "','" + str(_business[j]) + "','" + str(_platform[j])
                            + "','" + str(_brand[j]) + "','" + str(_product[j]) + "','" + str(_keycompbrand[j])
                            + "','" + str(_keycompproduct[j]) + "','" + str(_comp2brand[j]) + "','" + str(_comp2product[j])
                            + "','" + str(_comp3brand[j]) + "','" + str(_comp3product[j]) + "','" + str(_targetpfv[j])
                            + "','" + str(_updatedtimestamp[j]) + "','" + str(_plantype[j]) + "','" + str(_npitargetpricelocal[j])

                            + "')")

                #commit sql statement before moving to a new file
                cur.execute('COMMIT')

                result = progress(_current_count, total)
                print(result)
                _current_count += 1


            print("Total records inserted: ")
            print(j + 1)

            print(f"Completed loading file from {i}")
            logger('Plans', 'DownloadSucceeded', 'ExtractionSucceeded', 'LoadingSucceeded', i)
            #increment to next worksheet for new file and table name
            init += 1




    except:
        logger('Plans','Failed to insert data','Fail','Plans not imported','Plans')
        pass




#change location to local drive need to modify to shared marden one drive
os.chdir(location)

#sequence of events to run

print("Inserting data into: " + server)

#used to create/delete tables in DB
#step 1 test if tables exist, if they do remove them and recreate.  if they don't then just add them.
for i in tables:
    create_tables(i)

#step 2 attempt to insert the data from the tables
insert_data(file_names,workbook_names,tables)

# finally close the db connection
cur.close()
