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
        conn = sqlite3.connect(db_file, timeout=500.0)
        return conn
    except Error as e:
        print e

    return None


def compute_update_evolution(arguments):

    '''
    Fill out a data base with all the details about the announcement revieved by the collectors during the incident. Collection is done in parallel for each of the incidents.
    @param: arguments (array): incident name (str), collector_name (str), array_of_date_time (str)
    @return: None
    '''

    incident = arguments[0]
    collector_name = arguments[1]
    array_of_date_times = arguments[2]

    # Database setup
    # Connect with the databse
    db_file = incident + '_' + 'routing_history_database.db'

    # Create connection to the database
    con = create_connection(db_file)
    # Set autocommit on
    con.isolation_level = None

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
    create_table_query = 'CREATE TABLE ' + table_name + '(time INT, peer_address TEXT, peer_asn TEXT, prefix TEXT, origin_asn TEXT, as_path TEXT)'

    # Create table
    cur.execute(create_table_query)

    # SQL insert element query
    insert_element_query = 'INSERT INTO ' + table_name + '(time, peer_address, peer_asn, origin_asn, prefix, as_path) VALUES(?,?,?,?,?,?)'

    # print array_of_date_times

    ############################
    # Start the data collection
    ############################

    # Filter our the dates
    start_time = array_of_date_times[0][0]
    end_time = array_of_date_times[0][1]

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
    stream.add_filter('record-type','updates') # updates, ribs

    date_and_time = datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
    # print date_and_time

    # Compute start time
    prev_rib_time = calculate_unix_time(date_and_time)

    date_and_time = datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S')
    # print date_and_time

    # Compute end time
    next_rib_time = calculate_unix_time(date_and_time)

    # Setup time collection
    stream.add_interval_filter(prev_rib_time, next_rib_time)

    # Start the stream
    stream.start()

    # Update count
    count = 0

    # Get next record
    while(stream.get_next_record(rec)):

        if rec.status == 'valid':

            element = rec.get_next_elem()

            while element:

                if 'prefix' not in element.fields:  # Ignore if no prefix already in the element. Move on
                  continue

                # print element.type, element.peer_address, element.peer_asn, element.fields

                # Look for announcements
                if element.type == 'A':

                    as_path = element.fields['as-path']
                    ases = as_path.split(' ')

                    if len(ases) > 0:
                        # Extract the time that the element represents (int)
                        time = element.time

                        # Extract the ip address of the peer
                        peer_address = element.peer_address

                        # Extract the asn of the peer
                        peer_asn = element.peer_asn

                        # Extract the origin asn
                        origin_asn = ases[-1]

                        # Extract the prefix
                        prefix = element.fields['prefix']

                        # schema
                        # (time INT, peer_address TEXT, peer_asn TEXT, origin_asn TEXT, prefix TEXT, as_path TEXT)
                        data_tuple = (time, peer_address, peer_asn, origin_asn, prefix, as_path)

                        # Execute the query
                        cur.execute(insert_element_query, data_tuple)

                        # Commit local changes
                        con.commit()


                element = rec.get_next_elem()
                count += 1

                if count % 10**4 == 0:
                    print collector_name, datetime.utcfromtimestamp(time).strftime('%Y-%m-%d %H:%M:%S')
                    # break

    print 'Done:', collector_name, start_time, end_time


    # Close the cursor
    cur.close()

    # Close the local connection
    con.close()




def main():

    '''
    Create a sqlite database with the times of the updates for analysing if announcmenets are due to reset tables. Collection is done in parallel for each of the events
    @param: None
    @return: None
    '''

    # How to run it
    # python construct-routing-history-incidents-db.py "incident"

    # Location of the incident
    incident = sys.argv[1]
    print "Incident: ", incident

    # Change directory
    os.chdir('/home/pmoriano/Research/Hijacks/Graph-Analysis/BGPParser/New-Analysis/' + incident + '/data/')
    print "Directory: ", os.getcwd()

    # Define collectors to get the data
    if incident == 'Indonesia':
        # 16 collectors
        collector_names = ["route-views.eqix", "route-views.isc", "route-views.jinx", "route-views.kixp", "route-views.linx", "route-views.nwax", "route-views.perth",
        "route-views.saopaulo", "route-views.soxrs", "route-views.sydney", "route-views.telxatl", "route-views.wide", "route-views2", "route-views3", "route-views4", "route-views6"]
        # collector_names = ["route-views.kixp", "route-views.perth"]
        #collector_names = ["route-views.kixp"]

        # Seven days of data
        date_time_start = "2014-03-30 18:00:00"
        date_time_end = "2014-04-05 22:00:00"

        # One day of data
        date_time_start = "2014-04-02 06:00:00"
        date_time_end = "2014-04-03 06:00:00"

    elif incident == 'Malaysia':
        # 18 collectors
        collector_names = ["route-views.eqix", "route-views.isc", "route-views.jinx", "route-views.kixp", "route-views.linx", "route-views.nwax", "route-views.perth",
        "route-views.saopaulo", "route-views.sfmix", "route-views.sg", "route-views.soxrs", "route-views.sydney", "route-views.telxatl", "route-views.wide", "route-views2", "route-views3", "route-views4", "route-views6"]
        # collector_names = ["route-views.kixp", "route-views.perth"]

        # Seven days of data
        date_time_start = "2015-06-09 08:00:00"
        date_time_end = "2015-06-15 12:00:00"

        # One day of data
        date_time_start = "2015-06-11 20:00:00"
        date_time_end = "2015-06-12 20:00:00"

    elif incident == 'India':
        # 18 collectors
        collector_names = ["route-views.eqix", "route-views.isc", "route-views.jinx", "route-views.kixp", "route-views.linx", "route-views.nwax", "route-views.perth",
        "route-views.saopaulo", "route-views.sfmix", "route-views.sg", "route-views.soxrs", "route-views.sydney", "route-views.telxatl", "route-views.wide", "route-views2", "route-views3", "route-views4", "route-views6"]
        # collector_names = ["route-views.kixp", "route-views.perth"]

        # Seven days of data
        date_time_start = "2015-11-03 04:00:00"
        date_time_end = "2015-11-09 16:00:00"

        # One day of data
        date_time_start = "2015-11-05 18:00:00"
        date_time_end = "2015-11-06 18:00:00"

    elif incident == "Belarus":
        # 13 collectors
        collector_names = ["route-views.eqix", "route-views.isc", "route-views.jinx", "route-views.kixp", "route-views.linx", "route-views.perth", "route-views.saopaulo",
        "route-views.sydney", "route-views.telxatl", "route-views.wide", "route-views2", "route-views4", "route-views6"]
        # collector_names = ["route-views.kixp", "route-views.perth"]
        # collector_names = ["route-views.linx"]

        # Seven days of data
        date_time_start = "2013-02-24 08:00:00"
        date_time_end = "2013-03-01 06:00:00"

        # One day of data
        date_time_start = "2013-02-27 01:00:00"
        date_time_end = "2013-02-28 01:00:00"


    elif incident == "Iceland":
        # 13 collectors
        collector_names = ["route-views.eqix", "route-views.isc", "route-views.jinx", "route-views.kixp", "route-views.linx", "route-views.perth", "route-views.saopaulo",
        "route-views.sydney", "route-views.telxatl", "route-views.wide", "route-views2", "route-views4", "route-views6"]
        # collector_names = ["route-views.kixp", "route-views.perth"]
        # collector_names = ["route-views.linx"]

        # Seven days of data
        date_time_start = "2013-07-28 07:00:00"
        date_time_end = "2013-08-03 19:00:00"

        # One day of data
        date_time_start = "2013-07-31 01:00:00"
        date_time_end = "2013-08-01 01:00:00"


    elif incident == "Russia":
        # 19 collectors
        collector_names = ["route-views.eqix", "route-views.isc", "route-views.jinx", "route-views.kixp", "route-views.linx", "route-views.nwax", "route-views.perth", "route-views.chicago",
        "route-views.saopaulo", "route-views.sfmix", "route-views.sg", "route-views.soxrs", "route-views.sydney", "route-views.telxatl", "route-views.wide", "route-views2", "route-views3", "route-views4", "route-views6"]
        # collector_names = ["route-views.kixp", "route-views.perth"]
        # collector_names = ["route-views.linx"]

        # Seven days of data
        date_time_start = "2017-04-23 10:00:00"
        date_time_end = "2017-04-30 10:00:00"

        # One day of data
        date_time_start = "2017-04-26 10:00:00"
        date_time_end = "2017-04-27 10:00:00"


    array_of_date_times = []

    # Insert the datetime of the attack
    array_of_date_times.append((date_time_start, date_time_end))
    print array_of_date_times, len(array_of_date_times)

    # Aggregate parameters to do parallel computing
    arguments = []

    # Folders in which the data is contained
    for collector_name in collector_names:

        print collector_name
        arguments.append([incident, collector_name, array_of_date_times])


    start_time = time.time()

    print "Start parallel process"

    pool = Pool(processes=len(arguments))
    pool.map(compute_update_evolution, arguments)

    print "Finish parallel process"
    print "--- %s seconds ---" % (time.time() - start_time)


if __name__ == "__main__":

    main()
