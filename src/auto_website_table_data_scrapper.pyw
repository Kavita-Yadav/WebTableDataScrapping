import requests
import pandas as pd
import yamlparam
import numpy as np
from apscheduler.schedulers.blocking import BlockingScheduler
from clickhouse_driver import Client
import logging

def ScrapCovidDataFromGoogle():
    logging.info('Started scrapping covid 19 data from google website')
    # load yaml configuration
    param = yamlparam.load_yaml_to_dict()
    # clickhouse connection
    client = Client(param.get('CLICKHOUSE_HOST'))
    TableName = param.get('TableName')
    # set the logging level of the handler as per parameter
    logging.getLogger().handlers[0].setLevel(param.get('vLoggingLevel', 'INFO').upper())
    # website url to scrap data
    covid_url = 'https://news.google.com/covid19/map?hl=en-US&mid=%2Fm%2F02j71&gl=US&ceid=US%3Aen'
    # requesting data from given url in html format using GET 
    covid_html_data = requests.get(covid_url).content
    # read html table data and store it into list
    covid_data_list = pd.read_html(covid_html_data)
    # convert list into dataframe
    covid_dataframe = covid_data_list[0]
    # convert 'No Data' column value into 0
    covid_dataframe['New cases (1 day*)'] = covid_dataframe['New cases (1 day*)'].replace('No data', 0)
    # convert NaN column value into 0
    covid_dataframe['New cases (last 60 days)'] = covid_dataframe['New cases (last 60 days)'].replace(np.NaN, 0)
    # rename columns name
    covid_dataframe = covid_dataframe.rename(columns={'Total cases':'TotalCases'})
    covid_dataframe = covid_dataframe.rename(columns={'New cases (1 day*)':'NewCasesOneday'})
    covid_dataframe = covid_dataframe.rename(columns={'New cases (last 60 days)':'NewCaseslastSixtyday'})
    covid_dataframe = covid_dataframe.rename(columns={'Cases per 1M people':'CasesPerOneMllionPeople'})
    # change column data type to int
    covid_dataframe['NewCasesOneday'] = covid_dataframe['NewCasesOneday'].astype(int)
    covid_dataframe['NewCaseslastSixtyday'] = covid_dataframe['NewCaseslastSixtyday'].astype(int)
    # add current date column
    covid_dataframe['Date'] = pd.to_datetime('today').strftime("%m-%d-%Y")
    # convert string to date datatypes
    covid_dataframe['Date']  = covid_dataframe['Date'].astype('datetime64[ns]')
    # create clickhouse table
    client.execute(
        '''
        CREATE TABLE IF NOT EXISTS COVID.'''+TableName+''' 
        (
            Location String,
            TotalCases UInt64,
            NewCasesOneday UInt64,
            NewCaseslastSixtyday UInt64,
            CasesPerOneMllionPeople UInt64,
            Deaths UInt64,
            Date Date
        ) Engine=MergeTree ORDER BY (Location)
        '''
    )
    # first, delete duplicate data
    client.execute('''ALTER TABLE COVID.'''+TableName+''' 
                DELETE WHERE (Date) in (select distinct Date from COVID.'''+TableName+''' );''')
    # insert data into clickhouse database
    client.execute("INSERT INTO COVID."+TableName+" VALUES", covid_dataframe.to_dict('records'))
    # store covid dataframe data into csv file
    #covid_dataframe.to_csv(param.get('DataFile'))

scheduler = BlockingScheduler()
scheduler.add_job(ScrapCovidDataFromGoogle, 'interval', days=1)
scheduler.start()

# to stop schedular script, run in command line
#ps aux | grep python
#kill PID