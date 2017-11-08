import sys
import os
import json

''' psycopg2 can be used  to load  data  directly to redshift  '''
import psycopg2
from datetime import datetime, timedelta

''' apscheduler can be used to execute python function on timely basis'''
from apscheduler.schedulers.blocking import BlockingScheduler
import logging

''' FuelSDK can be used to connect marketing cloud.'''
import FuelSDK

''' boto3 can be used to connect AWS services'''
import boto3

import logging

logging.basicConfig()


con = psycopg2.connect(dbname='',
                       host='',
                       port='', user='', password='')

cur = con.cursor()


def audit_log_extrcts(start_time, endtime, flag):
    try:

        extract_type = 'campaign'
        extract_start_ts = start_time
        extract_end_ts = endtime
        print extract_type
        print extract_start_ts
        print extract_end_ts

        if flag == 1:
            print "1"

            sql = "select max(extract_end_ts) from marketingcloud.extract_control  where extract_type = '" + extract_type + "';"
            print sql

            # cur = con.cursor()
            cur.execute(sql)
            data = cur.fetchone()
            last_dt = data[0].strftime('%Y-%m-%dT%H:%M:%S.000')
            cur.execute('commit')
            return last_dt


            # cur = con.close()

        else:

            sql = "INSERT INTO marketingcloud.extract_control values('" + extract_type + "','" + extract_start_ts + "','" + extract_end_ts + "');"
            print sql

            # cur = con.cursor()
            cur.execute(sql)
            cur.execute('commit')
            # cur = con.close()

    except Exception as e:
        print 'Caught exception' + e.message
        print e


def truncate_data_dw(tablename):
    try:


        sql = """ truncate """ + tablename + """ ;
           commit;
           """
        cur = con.cursor()
        # cur.execute('commit')
        cur.execute(sql)
        cur.execute('commit')
        cur.close()

    except Exception as e:
        print 'Caught exception' + e.message
        print e



def load_data_dw(filename):
    try:


        outputDir = 's3://Datalake/MarketingCloud/' + filename

        sql = """ COPY marketingcloud.campaign
           FROM '""" + outputDir + """'
           JSON 'auto'
           CREDENTIALS
           '';
           commit;
           """

        cur = con.cursor()
        # cur.execute('commit')
        cur.execute(sql)
        cur.execute('commit')


    except Exception as e:
        print 'Caught exception' + e.message
        print e


def upload_file_s3(filename):
    try:

        s3_bucket = ''
        s3_path = 'Datalake/MarketingCloud/'
        s3_filename = s3_path + filename
        s3 = boto3.resource('s3')
        s3.meta.client.upload_file(filename, s3_bucket, s3_filename)

    except Exception as e:
        print 'Caught exception: ' + e.message
        print e


''' Function to convert date object in the required format'''


def myconverter(o):
    if isinstance(o, datetime):
        return o.__str__()


def run_job():
    print "Job starts "
    print 'current time ' + str(datetime.now())

    time = datetime.now()

    filename = 'Data/Campaign/' + 'campaign_' + str(time.strftime('%Y-%m-%d-%H-%M-%S')) + '.json'

    tablename = 'marketingcloud.campaign'

    # retrieveDate = audit_log_extrcts(None, None, 1)

    retrieveDate = time.strftime('%Y-%m-%dT%H:%M:%S.000')

    try:
        debug = False
        stubObj = FuelSDK.ET_Client(False, debug)

        print '>>> Retrieve Filtered CampaignEvents with GetMoreResults'

        getCamp = FuelSDK.ET_Campaign()
        getCamp.auth_stub = stubObj
        getResponse = getCamp.get()

        file = open(filename, 'w')
        for x in getResponse.results['items']:
            print x
            res = dict(x)
            res = dict((k.lower(), v) for k, v in res.iteritems())
            result = json.dumps(res, default=myconverter, sort_keys=True, indent=4, separators=(',', ': '))
            file.write(str(result))

        # file.close()
        # upload_file_s3(filename)

        while getResponse.more_results:
            for x in getResponse.results['items']:
                res = dict(x)
                res = dict((k.lower(), v) for k, v in res.iteritems())
                result = json.dumps(res, default=myconverter, sort_keys=True, indent=4, separators=(',', ': '))
                file.write(str(result))

        file.close()

        endtime = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.000')

        audit_log_extrcts(retrieveDate, endtime, 0)

        upload_file_s3(filename)

        truncate_data_dw(tablename)

        load_data_dw(filename)

        os.remove(filename)

        cur = con.close()

        print "Job Ends "
        print 'current time ' + str(datetime.now())


    except Exception as e:
        print 'Caught exception: ' + e.message
        print e


# scheduler = BlockingScheduler()
# scheduler.add_job(run_job,'interval',minutes=15)
# scheduler.start()

run_job()
