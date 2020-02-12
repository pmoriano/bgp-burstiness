from __future__ import division
import igraph as ig
from datetime import datetime, timedelta
import itertools
import time
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as dates
import os
import csv
import random
import glob
import re
import time
import matplotlib.dates as mdates
from matplotlib.dates import YearLocator, MonthLocator, DateFormatter, HourLocator, DayLocator
import matplotlib.ticker as ticker
import random
import json
import cPickle as pickle
import cairo
from collections import defaultdict
import pyqt_fit.nonparam_regression as smooth
from pyqt_fit import npr_methods
import pyqt_fit.bootstrap as bs
from _pybgpstream import BGPStream, BGPRecord, BGPElem
from IPython.display import display
import sys
import fnmatch
import urllib, urllib2
from lxml import etree
from multiprocessing import Pool, cpu_count, Manager, Process
import csv
import json
from urllib import urlencode, urlopen
from os import mkdir
from shutil import rmtree
import ijson
import bigjson
import sqlite3
import scipy.stats as stats

def calculate_unix_time(date_and_time):

    '''
    Calculate unix time elapsed from a datetime object
    @param: date_and_time (datetime): datetime object
    @return: Seconds elapsed using unix reference (int)
    '''
    return int((date_and_time - datetime.utcfromtimestamp(0)).total_seconds())



def create_connection(db_file):

    '''
    Create a database connection to the SQLite database specified by db_file
    @param db_file: database file (str)
    @return: Connection object or None
    '''

    try:
        conn = sqlite3.connect(db_file, timeout=100.0)
        return conn
    except Error as e:
        print e

    return None


def compute_update_evolution(arguments):

     '''
    Fill out a data base with all the details about the announcements during dates of NO incidents. Collection is done in parallel for each of the incidents.
    @param: arguments (array): incident name (str), collector_name (str), array_of_date_time (str), asn_top_dic (dict), perpetrator_asn (str)
    @return: None
    '''

    incident = arguments[0]
    collector_name = arguments[1]
    array_of_date_times = arguments[2]
    asn_top_dic = arguments[3]
    perpetrator_asn = arguments[4]

    # Database setup
    # Connect with the database
    db_file = incident + '_' + 'null_interarrival_times_database.db'

    # Create connection to the database
    con = create_connection(db_file)

    # Create a cursor
    cur = con.cursor()

    # Extract collector name
    table_name = re.search(r'route-views(.*)', collector_name).group(1)

    if table_name[0] == ".":
        table_name = table_name[1:]

    # # Add '_' to be a valid table name
    table_name = "_" + table_name

    # Drop table if exist
    cur.execute('DROP TABLE IF EXISTS ' + table_name)

    # SQL create table statement
    create_table_query = 'CREATE TABLE ' + table_name + '(datetime TEXT, collector_name TEXT, asn TEXT, timestamps TEXT)'

    # Create table
    cur.execute(create_table_query)

    # SQL insert element query
    insert_element_query = 'INSERT INTO ' + table_name + '(datetime, collector_name, asn, timestamps) VALUES(?,?,?,?)'

    # print array_of_date_times

    # Do this for everydate
    experiment_number = 1

    for date in array_of_date_times:

        print date, type(date)

        # Filter our the dates
        start_time = date[0]
        end_time = date[1]

        # print start_time, type(start_time), end_time, type(start_time)

        ############################
        # BGPStream configuration
        ############################

        # Create a new bgpstream instance and a reusable bgprecord instance
        stream = BGPStream()
        rec = BGPRecord()

        # Consider only one collector
        stream.add_filter('collector', collector_name)

        # Consider updates only
        stream.add_filter('record-type','updates')

        date_and_time = datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')

        # Get the previous RIB time
        prev_rib_time = calculate_unix_time(date_and_time)

        date_and_time = datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S')

        # Get the next RIB time
        next_rib_time = calculate_unix_time(date_and_time)

        # Setup time collection
        stream.add_interval_filter(prev_rib_time, next_rib_time)

        # Start the stream
        stream.start()

        # Dic of dics representing the routing table
        internal_dic = defaultdict(list)

        count = 0

        # List with the ases to compare
        asn_top_options = asn_top_dic[collector_name]
        asn_top_options.append(perpetrator_asn)
        asn_top_options = set(asn_top_options)
        # print asn_top_options

        # Get next record
        while(stream.get_next_record(rec)):

            if rec.status == 'valid':

                element = rec.get_next_elem()

                while element:

                    if 'prefix' not in element.fields:  # Ignore if no prefix already in the element. Move on
                       continue

                    # Compute the time that the element represents (int)
                    announcement_time = element.time

                    # Look for announcements
                    if element.type == 'A':
                        ases = element.fields['as-path'].split(' ')
                        if len(ases) > 0:
                            origin_asn = ases[-1]
                            if origin_asn in asn_top_options:
                                # if announcement_time not in internal_dic[origin_asn]:
                                internal_dic[origin_asn].append(announcement_time)

                    element = rec.get_next_elem()
                    count += 1

                    if count % 10**6 == 0:
                        print collector_name, experiment_number, datetime.utcfromtimestamp(announcement_time).strftime('%Y-%m-%d %H:%M:%S')

        # Insert rows for every possible asn in the list
        for asn in asn_top_options:
            # print internal_dic[asn]
            data_tuple = (start_time, collector_name, asn, json.dumps(internal_dic[asn]))

            # Execute the query
            cur.execute(insert_element_query, data_tuple)

            # Commit local changes
            con.commit()

        print 'Done:', collector_name, experiment_number, date[0]
        experiment_number += 1


    # Close the cursor
    cur.close()

    # Close the local connection
    con.close()




def generate_random_datetime():

    '''
    Generate random start and end datetimes (for dates between 2014 and 2017)
    '''

    # First shot
    year = random.randint(2013, 2017)
    month = random.randint(1, 12)
    day = random.randint(1, 28)
    hour = random.randint(0, 23)
    minute = random.randint(0, 59)
    second = random.randint(0, 59)

    # Check is any of the days of the incidents (Indonesia, Malaysia, India, Iceland, Belarus, Russia)
    while (year == 2014 and month == 4 and day == 2) or (year == 2015 and month == 6 and day == 12) or (year == 2015 and month == 11 and day == 6) or \
        (year == 2013 and month == 7 and day == 31) or (year == 2013 and month == 2 and day == 27) or (year == 2017 and month == 4 and day == 26):
        year = random.randint(2013, 2017)
        month = random.randint(1, 12)
        day = random.randint(1, 28)
        hour = random.randint(0, 23)
        minute = random.randint(0, 59)
        second = random.randint(0, 59)


    ####################################
    # Only with respect to Iceland
    ####################################

    # year = 2013
    # month = random.randint(1, 12)
    # day = random.randint(1, 28)
    # hour = random.randint(0, 23)
    # minute = random.randint(0, 59)
    # second = random.randint(0, 59)

    # while (month == 7 and day == 31):
    #     month = random.randint(1, 12)
    #     day = random.randint(1, 28)
    #     hour = random.randint(0, 23)
    #     minute = random.randint(0, 59)
    #     second = random.randint(0, 59)


    start_random_datetime = datetime(year, month, day, hour, minute, second)

    # Add 24 hours to be comparable with the actual case studies
    end_random_datetime = start_random_datetime + timedelta(hours=24)

    return (start_random_datetime.strftime("%Y-%m-%d %H:%M:%S"), end_random_datetime.strftime("%Y-%m-%d %H:%M:%S"))
    # return start_random_datetime, end_random_datetime


def main():

    '''
    Collect 100 instances of about 1 day of announcements needed to run the Monte Carlo test on the null hypothesis. Collection is done in parallel for each of the events
    @param: None
    @return: None
    '''

    # How to run it
    # python understanding-interevent-time-null.py "incident"

    # Location of the incident
    incident = sys.argv[1]
    print "Incident: ", incident

    # Change directory
    os.chdir('/home/pmoriano/Research/Hijacks/Graph-Analysis/BGPParser/New-Analysis/random_new/data/')

    # Define collectors to get the data
    if incident == 'Indonesia':
        # 16 collectors
        collector_names = ["route-views.eqix", "route-views.isc", "route-views.jinx", "route-views.kixp", "route-views.linx", "route-views.nwax", "route-views.perth",
        "route-views.saopaulo", "route-views.soxrs", "route-views.sydney", "route-views.telxatl", "route-views.wide", "route-views2", "route-views3", "route-views4", "route-views6"]
        # collector_names = ["route-views.kixp", "route-views.perth"]
        # asn_list = ['4761', '3356', '1299', '174', '2914', '3257']

        # Load the dic with the info about the most bursty ASes
        with open('/your_directory_path/data/dic_of_tops_burstiness_indonesia.json', 'r') as fp:
            asn_top_dic = json.load(fp)

        print asn_top_dic

        perpetrator_asn = '4761'

        date_time_start = "2014-04-02 06:00:00"
        date_time_end = "2014-04-03 06:00:00"

    elif incident == 'Malaysia':
        # 18 collectors
        collector_names = ["route-views.eqix", "route-views.isc", "route-views.jinx", "route-views.kixp", "route-views.linx", "route-views.nwax", "route-views.perth",
        "route-views.saopaulo", "route-views.sfmix", "route-views.sg", "route-views.soxrs", "route-views.sydney", "route-views.telxatl", "route-views.wide", "route-views2", "route-views3", "route-views4", "route-views6"]
        # collector_names = ["route-views.kixp", "route-views.perth"]
        # asn_list = ['4788', '3356', '1299', '174', '2914', '3257']

        # Load the dic with the info about the most bursty ASes
        with open('/your_directory_path/data/dic_of_tops_burstiness_malaysia.json', 'r') as fp:
            asn_top_dic = json.load(fp)

        print asn_top_dic

        perpetrator_asn = '4788'

        date_time_start = "2015-06-11 20:00:00"
        date_time_end = "2015-06-12 20:00:00"

    elif incident == 'India':
        # 18 collectors
        collector_names = ["route-views.eqix", "route-views.isc", "route-views.jinx", "route-views.kixp", "route-views.linx", "route-views.nwax", "route-views.perth",
        "route-views.saopaulo", "route-views.sfmix", "route-views.sg", "route-views.soxrs", "route-views.sydney", "route-views.telxatl", "route-views.wide", "route-views2", "route-views3", "route-views4", "route-views6"]
        # collector_names = ["route-views.kixp", "route-views.perth"]
        # asn_list = ['9498', '3356', '1299', '174', '2914', '3257']

        # Load the dic with the info about the most bursty ASes
        with open('/your_directory_path/data/dic_of_tops_burstiness_india.json', 'r') as fp:
            asn_top_dic = json.load(fp)

        print asn_top_dic

        perpetrator_asn = '9498'

        date_time_start = "2015-11-05 18:00:00"
        date_time_end = "2015-11-06 18:00:00"

    elif incident == "Iceland":
        # 13 collectors
        collector_names = ["route-views.eqix", "route-views.isc", "route-views.jinx", "route-views.kixp", "route-views.linx", "route-views.perth", "route-views.saopaulo",
        "route-views.sydney", "route-views.telxatl", "route-views.wide", "route-views2", "route-views4", "route-views6"]
        # collector_names = ["route-views.kixp", "route-views.perth"]
        # asn_list = ['48685', '3356', '1299', '174', '2914', '3257']


        # Load the dic with the info about the most bursty ASes
        with open('/your_directory_path/data/dic_of_tops_burstiness_iceland.json', 'r') as fp:
            asn_top_dic = json.load(fp)

        print asn_top_dic

        perpetrator_asn = '48685'

        date_time_start = "2013-07-31 12:00:00"
        date_time_end = "2013-08-01 12:00:00"


    elif incident == "Belarus":
        # 13 collectors
        collector_names = ["route-views.eqix", "route-views.isc", "route-views.jinx", "route-views.kixp", "route-views.linx", "route-views.perth",
        "route-views.saopaulo", "route-views.sydney", "route-views.telxatl", "route-views.wide", "route-views2", "route-views4", "route-views6"]
        # collector_names = ["route-views.kixp", "route-views.perth"]
        # asn_list = ['28849', '3356', '1299', '174', '2914', '3257']


        # Load the dic with the info about the most bursty ASes
        with open('/your_directory_path/data/dic_of_tops_burstiness_belarus.json', 'r') as fp:
            asn_top_dic = json.load(fp)

        print asn_top_dic

        perpetrator_asn = '28849'

        date_time_start = "2013-02-26 20:00:00"
        date_time_end = "2013-02-27 20:00:00"


    elif incident == "Russia":
        # 18 collectors excluding Chicago
        collector_names = ["route-views.eqix", "route-views.isc", "route-views.jinx", "route-views.kixp", "route-views.linx", "route-views.nwax", "route-views.perth",
        "route-views.saopaulo", "route-views.sfmix", "route-views.sg", "route-views.soxrs", "route-views.sydney", "route-views.telxatl", "route-views.wide", "route-views2", "route-views3", "route-views4", "route-views6"]
        # collector_names = ["route-views.kixp", "route-views.perth"]
        # asn_list = ['12389', '3356', '1299', '174', '2914', '3257']


        # Load the dic with the info about the most bursty ASes
        with open('/your_directory_path/data/dic_of_tops_burstiness_russia.json', 'r') as fp:
            asn_top_dic = json.load(fp)

        print asn_top_dic

        perpetrator_asn = '12389'

        date_time_start = "2017-04-26 10:00:00"
        date_time_end = "2017-04-27 10:00:00"


    array_of_date_times = []
    number_of_events = 100
    for event_index in range(number_of_events):
        array_of_date_times.append(generate_random_datetime())

    # Insert the datetime of the attack
    array_of_date_times.append((date_time_start, date_time_end))
    # print array_of_date_times, len(array_of_date_times), asn_top_dic, perpetrator_asn

    # Aggregate parameters to do parallel computing
    arguments = []

    # Folders in which the data is contained
    for collector_name in collector_names:

        # print collector_name
        arguments.append([incident, collector_name, array_of_date_times, asn_top_dic, perpetrator_asn])


    start_time = time.time()

    print "Start parallel process"

    pool = Pool(processes=len(arguments))
    pool.map(compute_update_evolution, arguments)

    print "Finish parallel process"
    print "--- %s seconds ---" % (time.time() - start_time)


if __name__ == "__main__":

    main()
