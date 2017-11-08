import os
import sys
import json

''' psycopg2 can be used  to load  data  directly to redshift  '''
import psycopg2
from datetime import datetime, timedelta

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


def audit_log_extracts(start_time, endtime, flag):
    try:

        extract_type = 'click_event'
        extract_start_ts = start_time
        extract_end_ts = endtime
        print extract_type
        print extract_start_ts
        print extract_end_ts

        if flag == 1:

            sql = "select max(extract_end_ts) from marketingcloud.extract_control  where extract_type = '" + extract_type + "';"
            print sql

            cur.execute(sql)
            data = cur.fetchone()
            last_dt = data[0].strftime('%Y-%m-%dT%H:%M:%S.000')
            cur.execute('commit')
            return last_dt

        else:

            sql = "INSERT INTO marketingcloud.extract_control values('" + extract_type + "','" + extract_start_ts + "','" + extract_end_ts + "');"
            print sql
            cur.execute(sql)
            cur.execute('commit')

    except Exception as e:
        print 'Caught exception' + e.message
        print e


def load_data_dw(filename):
    try:

        outputDir = 's3:///Datalake/MarketingCloud/' + filename

        sql = """ COPY marketingcloud.click_event
           FROM '""" + outputDir + """'
           JSON 'auto'
           gzip
           CREDENTIALS
           '';
           commit;
           """
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
    filename = 'Data/Click_Event/' + 'click_event_' + str(time.strftime('%Y-%m-%d-%H-%M-%S')) + '.json'

    # Create cut-off from and to dates
    fromDate = audit_log_extracts(None, None, 1)
    toDate = time.strftime('%Y-%m-%dT%H:%M:%S')

    try:
        debug = False
        stubObj = FuelSDK.ET_Client(False, debug)

        print '>>> Retrieve Filtered BounceEvents with GetMoreResults'
        getClickEvent = FuelSDK.ET_ClickEvent()
        getClickEvent.auth_stub = stubObj
        getClickEvent.props = ["SendID", "SubscriberKey", "EventDate", "Client.ID", "EventType", "BatchID",
                               "TriggeredSendDefinitionObjectID", "PartnerKey"]
        from_filter = {'Property': 'EventDate', 'SimpleOperator': 'greaterThan', 'DateValue': fromDate}
        to_filter = {'Property': 'EventDate', 'SimpleOperator': 'lessThan', 'DateValue': toDate}
        getClickEvent.search_filter = {'LeftOperand': from_filter, 'LogicalOperator': 'AND', 'RightOperand': to_filter}
        getResponse = getClickEvent.get()

        file = open(filename, 'w')
        for x in getResponse.results:
            res = dict(x)
            y = dict(res['Client'])
            del res['Client']
            res['Client'] = str(y['ID'])
            res = dict((k.lower(), v) for k, v in res.iteritems())
            result = json.dumps(res, default=myconverter, sort_keys=True, indent=4, separators=(',', ': '))
            file.write(str(result))

        while getResponse.more_results:
            for x in getResponse.results:
                res = dict(x)
                y = dict(res['Client'])
                del res['Client']
                res['Client'] = str(y['ID'])
                result = json.dumps(res, default=myconverter, sort_keys=True, indent=4, separators=(',', ': '))
                file.write(str(result))

        file.close()

        toDate = str(datetime.strptime(toDate, '%Y-%m-%dT%H:%M:%S') + timedelta(seconds=-1))
        fromDate = str(datetime.strptime(fromDate, '%Y-%m-%dT%H:%M:%S.%f') + timedelta(seconds=1))

        audit_log_extracts(fromDate, toDate, 0)

        size = os.path.getsize(filename)

        if size == 0:
            print "0 byte file "
            os.remove(filename)

        else:

            check_call(['gzip', filename])
            upload_file_s3(filename + '.gz')
            load_data_dw(filename + '.gz')
            print "S3  upload and Redshift integration successful "
            os.remove(filename + '.gz')

        print "Job Ends "
        print 'current time ' + str(datetime.now())

    except Exception as e:
        print 'Caught exception: ' + e.message
        print e


run_job()
cur.close()

