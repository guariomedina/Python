'''
    Developer: Guarionex Medina
    Email: guario.medina@gmail.com
    Purpose: Progam to take data from a 3rd party API and ingest into a SQL DB for data processing.  Also there was a need to add web proxy to retrieve the information through the company firewall when inside.
    '''

import requests
import pandas as pd
import pyodbc
import sys


# set the url for the api as well as the specific values for the arguments
url = 'API_URL'
x = {'api_session_id': 'idValue', 'product': 'productFilter', 'format': 'json'}
proxy = {
    'http': 'webProxy',
    'https': 'webProxy',
    'ftp': 'webProxy'
}


#check to see if a connection can be made whether outside or inside of the HP Firewall
try:

    r = requests.get(url, params=x)
    print("Request to API outside of Firewall. ")

except:
    r = requests.get(url, params=x, proxies=proxy)
    print("Request to API within Firewall. ")


#take the data from the api and place it within a dataframe object for only the columns currently needed
data = pd.DataFrame(r.json(), columns=['title', 'specs', 'offers'])


#set initial value for total records received
row_count = data['title'].shape[0]
i = 0
j = 0
total_count = 0



#set variables to easily change database and server from testing to production
server = 'SQL Server'

database = 'SQL DB'


user = 'loginID'
password = 'loginPassword'

driver = 'SQL Server Native Client 11.0'

#connect to the database
control = pyodbc.connect(Driver=driver, Server=server, DATABASE=database,  Trusted_Connection='Yes'
                         , UID = user, PWD = password)
cur = control.cursor()

#test to see if request code came back as expected otherwise log error, however if db connection failed can't log
if r.status_code != 200:
    print("Connection to API failed.")
    cur.execute(
        "Insert into dbo.datashreddervalidationreport(DataShredder, DownloadStatus, ExtractionStatus, LoadingStatus,"
        + "RunTime, FileName, FileDate, ReportValidationVersion)"
        + "VALUES('BrowsWave','DownloadFailed','ExtractionUnneeded','APIConnectionFailed', CURRENT_TIMESTAMP,"
        + "'BrowsWave_API' , CURRENT_TIMESTAMP, '0')"
    )
    cur.execute('COMMIT')
    sys.exit()
elif not cur:
    print("Connection to DB failed")
    sys.exit()
else:
    print(f"Number of records imported is {row_count}. ")
    print("Connecting to DB.")


try:
    #truncate the table before inserting new records
    cur.execute("Truncate table dbo.TableName")
    print("Truncating Table dbo.TableName.")
    print("Inserting records to database.")

    #run through the loop of records as there are only 1 title and set of specs within the API
    while i <= row_count:
        title = data['title'][i]
        brand = data['specs'][i]['brand']
        series = data['specs'][i]['series']
        model = data['specs'][i]['model']
        device_type = data['specs'][i]['device_type']
        printer_type = data['specs'][i]['printer_type']
        color_type = data['specs'][i]['color_type']
        speed = data['specs'][i]['speed']
        format = data['specs'][i]['format']
        resolution = data['specs'][i]['resolution']
        duty_cycle = data['specs'][i]['duty_cycle']
        double_sided = data['specs'][i]['double_sided']
        scanner_resolution = data['specs'][i]['scanner_resolution']

        #run through iteration of the offers field to determine if there is more than just 1 company offering listed
        offer_ct = len(data['offers'][i])



        while j <= offer_ct-1:
            company = data['offers'][i][j]['company']
            company_id = data['offers'][i][j]['company_id']
            country = data['offers'][i][j]['country']
            country_id = data['offers'][i][j]['country_id']
            url = data['offers'][i][j]['url']
            price = data['offers'][i][j]['price']
            promo = data['offers'][i][j]['promo']
            stock = data['offers'][i][j]['stock']
            timestamp = data['offers'][i][j]['timestamp']



            cur.execute("Insert into dbo.BrowsWave(Title, Brand, Series, Model, Device_Type, Printer_Type, Color_Type,"
                    + "Speed, Format, Resolution, Duty_Cycle, Double_Sided, Scanner_Resolution, Company, Company_ID,"
                    + "Country, Country_ID, URL, Price, Promo, Stock, Timestamp) "
                    + "VALUES('" + title + "','" + brand + "','" + series
                    + "','" + model + "','" + device_type + "','" + printer_type + "','" + color_type + "','"
                    + str(speed) + "','" + format + "','" + resolution + "','" + str(duty_cycle) + "','" + double_sided
                    + "','" + scanner_resolution + "','" + company + "','" + str(company_id) + "','" + country
                    + "','" + country_id + "','" + url + "','" + str(price) + "','" + promo + "','" + stock
                    + "','" + timestamp

                        +"')") 

            cur.execute('COMMIT')

            j += 1
            total_count += 1



        i += 1
        j = 0



except:

    cur.execute(
        "Insert into dbo.datashreddervalidationreport(DataShredder, DownloadStatus, ExtractionStatus, LoadingStatus,"
        + "RunTime, FileName, FileDate, ReportValidationVersion)"
        + "VALUES('API','DownloadSucceeded','ExtractionUnneeded','LoadingSucceeded', CURRENT_TIMESTAMP,"
        + "'API' , CURRENT_TIMESTAMP, '0')"
    )
    pass

cur.execute('COMMIT')
print(f"Total number of records in the database is {total_count}.")
print("Insertion completed closing connection to database. ")
control.close()
