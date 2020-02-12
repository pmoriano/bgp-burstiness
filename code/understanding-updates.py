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


def retrieve_previous_RIB(cid, date):

    '''
    Fetch the previous dump time of a RIB
    @param: cid (str): collector name
    @param: date (str): date to start the analysis in format "YYY-MM-DD HH:MM:SS"
    @return: Previous timestamp of the RIB (datetime)
    '''

    if type(date) == int:
        date = datetime.utcfromtimestamp(date)

    if cid == "route-views2.oregon-ix.net":
        cid = ""
    # RouteViews weird url for route-views2
    if cid == "route-views2":
        url = "http://routeviews.org/bgpdata/%d.%02d/RIBS/"%(date.year, date.month)
    else:
        if cid:
            cid = cid + "/"
            url = "http://routeviews.org/%sbgpdata/%d.%02d/RIBS/"%(cid, date.year, date.month)

    # print "Fetching:", url
    content = urllib.urlopen(url).read()
    try:
        content = content[content.index("<table>"):]
        content = content[:content.index("</table>") + 9]
    except:
        print "Unable to retrieve info for:", cid, date
        raise

    parser = etree.HTMLParser()
    doc = etree.fromstring(content, parser)
    p = lambda x: datetime.strptime(x, 'rib.%Y%m%d.%H%M.bz2')
    times = [p(x.text) for x in doc.findall(".//a") if "rib" in x.text and "bz2" in x.text]
    # print filter(lambda x: x < date, sorted(times))[-1]
    return filter(lambda x: x < date, sorted(times))[-1]



def retrieve_next_RIB(cid, date):

    '''
    Fetch the next dump time of a RIB
    @param: cid (str): collector name
    @param: date (str): date to start the analysis in format "YYY-MM-DD HH:MM:SS"
    @return: Next timestamp of the RIB (datetime)
    '''

    if type(date) == int:
        date = datetime.utcfromtimestamp(date)

    if cid == "route-views2.oregon-ix.net":
        cid = ""
    # RouteViews weird url for route-views2
    if cid == "route-views2":
        url = "http://routeviews.org/bgpdata/%d.%02d/RIBS/"%(date.year, date.month)
    else:
        if cid:
            cid = cid + "/"
            url = "http://routeviews.org/%sbgpdata/%d.%02d/RIBS/"%(cid, date.year, date.month)

    # print "Fetching:", url
    content = urllib.urlopen(url).read()
    try:
        content = content[content.index("<table>"):]
        content = content[:content.index("</table>") + 9]
    except:
        print "Unable to retrieve info for:", cid, date
        raise

    parser = etree.HTMLParser()
    doc = etree.fromstring(content, parser)
    p = lambda x: datetime.strptime(x, 'rib.%Y%m%d.%H%M.bz2')
    times = [p(x.text) for x in doc.findall(".//a") if "rib" in x.text]
    # print filter(lambda x: x >= date, sorted(times))[0]
    return filter(lambda x: x >= date, sorted(times))[0]


def compute_update_evolution(collector_name, collector_dic, start_time, end_time):

    '''
    Collect the TOTAL number of announcements per collector, date, and origin asn in a shared dictionary. Collection is done in parallel for each of the incidents.
    @param: collector_name (str): name of the collector
    @param: collector_dic (dict): shared directory to store the data
    @param: start_time (str): start of the data collection in format "YYY-MM-DD HH:MM:SS"
    @param: end_time (str): end of the data collection in format "YYY-MM-DD HH:MM:SS"
    @return: None
    '''

    # Create a new bgpstream instance and a reusable bgprecord instance
    stream = BGPStream()
    rec = BGPRecord()

    # Consider only one collector
    stream.add_filter('collector', collector_name)

    # Consider updates only
    stream.add_filter('record-type','updates')

    date_and_time = datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
    # print date_and_time

    # Get the previous RIB time
    print retrieve_previous_RIB(collector_name, date_and_time)
    prev_rib_time = calculate_unix_time(retrieve_previous_RIB(collector_name, date_and_time))

    date_and_time = datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S')
    # print date_and_time

    # Get the next RIB time
    print retrieve_next_RIB(collector_name, date_and_time)
    next_rib_time = calculate_unix_time(retrieve_next_RIB(collector_name, date_and_time))

    # Setup time collection
    stream.add_interval_filter(prev_rib_time, next_rib_time)

    # Start the stream
    stream.start()

    # # Dic of dic representing the routing table
    internal_dic = defaultdict(lambda: defaultdict(list))

    # Updates messages
    # Get next record

    count = 0

    while(stream.get_next_record(rec)):

        if rec.status == 'valid':

            element = rec.get_next_elem()

            while element:

                if 'prefix' not in element.fields:  #Ignore if no prefix already in the table. Move on
                    continue

                # Use the local timestamps as a reference
                date_time = datetime.utcfromtimestamp(element.time).strftime('%Y-%m-%d %H:%M:%S')

                # Update messages
                if element.type.lower() in ('announcement', 'a'):
                    if ' ' in element.fields['as-path']:
                        as_paths = element.fields['as-path'].split(' ')
                        origin_asn = as_paths[-1]
                    else:
                        origin_asn = element.fields['as-path']


                    if origin_asn not in internal_dic[date_time]:
                        internal_dic[date_time][origin_asn] = [1, 0]
                    else:
                        internal_dic[date_time][origin_asn][0] += 1

                # Deletion of routes
                elif element.type.lower() in ('withdrawal', 'w'):
                    peer_asn = str(element.peer_asn)
                    if peer_asn not in internal_dic[date_time]:
                        internal_dic[date_time][peer_asn] = [0, 1]
                    else:
                        internal_dic[date_time][peer_asn][1] += 1

                if count % 10**5 == 0:
                    print collector_name, date_time

                element = rec.get_next_elem()
                count += 1


    collector_dic[collector_name] = dict(internal_dic)

    print "Done:", collector_name


def compute_update_evolution_unique_prefixes(collector_name, collector_dic, start_time, end_time):

    '''
    Collect the number of UNIQUE announcements per collector, date, and origin asn in a shared dictionary. Collection is done in parallel for each of the incidents.
    @param: collector_name (str): name of the collector
    @param: collector_dic (dict): shared directory to store the data
    @param: start_time (str): start of the data collection in format "YYY-MM-DD HH:MM:SS"
    @param: end_time (str): end of the data collection in format "YYY-MM-DD HH:MM:SS"
    @return: None
    '''

    # Create a new bgpstream instance and a reusable bgprecord instance
    stream = BGPStream()
    rec = BGPRecord()

    # Consider only one collector
    stream.add_filter('collector', collector_name)

    # Consider updates only
    stream.add_filter('record-type','updates')

    date_and_time = datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
    # print date_and_time

    # Get the previous RIB time
    print retrieve_previous_RIB(collector_name, date_and_time)
    prev_rib_time = calculate_unix_time(retrieve_previous_RIB(collector_name, date_and_time))

    date_and_time = datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S')
    # print date_and_time

    # Get the next RIB time
    print retrieve_next_RIB(collector_name, date_and_time)
    next_rib_time = calculate_unix_time(retrieve_next_RIB(collector_name, date_and_time))

    # Setup time collection
    stream.add_interval_filter(prev_rib_time, next_rib_time)

    # Start the stream
    stream.start()

    # # Dic of dics representing the prefixes announced by an AS at a certain time
    timestamp_as_prefix_dic = defaultdict(lambda: defaultdict(set))

    # # Dic of dics representing the routing table
    internal_dic = defaultdict(lambda: defaultdict(list))

    # Updates messages
    # Get next record

    count = 0

    while(stream.get_next_record(rec)):

        if rec.status == 'valid':

            element = rec.get_next_elem()

            while element:

                if 'prefix' not in element.fields:  #Ignore if no prefix already in the table. Move on
                    continue

                date_time = datetime.utcfromtimestamp(element.time).strftime('%Y-%m-%d %H:%M:%S')

                # Update messages
                if element.type.lower() in ('announcement', 'a'):
                    if ' ' in element.fields['as-path']:
                        as_paths = element.fields['as-path'].split(' ')
                        origin_asn = as_paths[-1]
                    else:
                        origin_asn = element.fields['as-path']


                    # Get the prefix
                    prefix = element.fields['prefix']

                    # Check if the prefic is already in
                    if prefix not in timestamp_as_prefix_dic[date_time][origin_asn]:
                        if origin_asn not in internal_dic[date_time]:
                            internal_dic[date_time][origin_asn] = [1, 0]
                        else:
                            internal_dic[date_time][origin_asn][0] += 1

                    # Insert the prefix in the dic
                    timestamp_as_prefix_dic[date_time][origin_asn].add(prefix)


                # Deletion of routes
                elif element.type.lower() in ('withdrawal', 'w'):
                    peer_asn = str(element.peer_asn)

                    # Get the prefix
                    prefix = element.fields['prefix']

                    # Check if the prefic is already in
                    if prefix not in timestamp_as_prefix_dic[date_time][peer_asn]:
                        if peer_asn not in internal_dic[date_time]:
                            internal_dic[date_time][peer_asn] = [0, 1]
                        else:
                            internal_dic[date_time][peer_asn][1] += 1

                    # Insert the prefix in the dic
                    timestamp_as_prefix_dic[date_time][peer_asn].add(prefix)

                element = rec.get_next_elem()
                count += 1

                if count % 10**6 == 0:
                    print collector_name, date_time


    collector_dic[collector_name] = dict(internal_dic)

    print "Done:", collector_name



def main():

    '''
    Start the announcement collection procedure based on the name of the event to analyze. Collection is done in parallel for each of the events
    @param: None
    @return: None
    '''

    # How to run it:
    # python understanding-updates.py incident unique

    # Location of the incident
    incident = sys.argv[1]
    type_route = sys.argv[2]
    print "Incident: ", incident

    # Change directory
    os.chdir("/your_directory_path/" + incident + "/data/")


    # Define collectors to get the data
    if incident == "Indonesia":
        # 16 collectors
        collector_names = ["route-views.eqix", "route-views.isc", "route-views.jinx", "route-views.kixp", "route-views.linx", "route-views.nwax", "route-views.perth",
        "route-views.saopaulo", "route-views.soxrs", "route-views.sydney", "route-views.telxatl", "route-views.wide", "route-views2", "route-views3", "route-views4", "route-views6"]
        # collector_names = ["route-views.kixp", "route-views.perth"]

        date_time_start = "2014-03-30 18:00:00"
        date_time_end = "2014-04-05 22:00:00"

    elif incident == "Malaysia":
        # 18 collectors
        collector_names = ["route-views.eqix", "route-views.isc", "route-views.jinx", "route-views.kixp", "route-views.linx", "route-views.nwax", "route-views.perth",
        "route-views.saopaulo", "route-views.sfmix", "route-views.sg", "route-views.soxrs", "route-views.sydney", "route-views.telxatl", "route-views.wide", "route-views2", "route-views3", "route-views4", "route-views6"]
        # collector_names = ["route-views.kixp", "route-views.perth"]

        date_time_start = "2015-06-09 08:00:00"
        date_time_end = "2015-06-15 12:00:00"

    elif incident == "India":
        # 18 collectors
        collector_names = ["route-views.eqix", "route-views.isc", "route-views.jinx", "route-views.kixp", "route-views.linx", "route-views.nwax", "route-views.perth",
        "route-views.saopaulo", "route-views.sfmix", "route-views.sg", "route-views.soxrs", "route-views.sydney", "route-views.telxatl", "route-views.wide", "route-views2", "route-views3", "route-views4", "route-views6"]
        # collector_names = ["route-views.kixp", "route-views.perth"]

        date_time_start = "2015-11-03 04:00:00"
        date_time_end = "2015-11-09 16:00:00"


    elif incident == "Belarus":
        # 13 collectors
        collector_names = ["route-views.eqix", "route-views.isc", "route-views.jinx", "route-views.kixp", "route-views.linx", "route-views.perth",
        "route-views.saopaulo", "route-views.sydney", "route-views.telxatl", "route-views.wide", "route-views2", "route-views4", "route-views6"]
        # collector_names = ["route-views.kixp", "route-views.perth"]

        date_time_start = "2013-02-24 08:00:00"
        date_time_end = "2013-03-01 06:00:00"


    elif incident == "Iceland": # No route-views3
        # 13 collectors
        collector_names = ["route-views.eqix", "route-views.isc", "route-views.jinx", "route-views.kixp", "route-views.linx", "route-views.perth",
        "route-views.saopaulo", "route-views.sydney", "route-views.telxatl", "route-views.wide", "route-views2", "route-views4", "route-views6"]
        # collector_names = ["route-views.kixp", "route-views.perth"]

        date_time_start = "2013-07-28 07:00:00"
        date_time_end = "2013-08-03 19:00:00"


    elif incident == "Russia":
        # 18 collectors excluding Chicago
        collector_names = ["route-views.eqix", "route-views.isc", "route-views.jinx", "route-views.kixp", "route-views.linx", "route-views.nwax", "route-views.perth",
        "route-views.saopaulo", "route-views.sfmix", "route-views.sg", "route-views.soxrs", "route-views.sydney", "route-views.telxatl", "route-views.wide", "route-views2", "route-views3", "route-views4", "route-views6"]
        # collector_names = ["route-views.kixp", "route-views.perth"]
        # collector_names = ["route-views.linx"]

        # Seven days of data
        date_time_start = "2017-04-23 10:00:00"
        date_time_end = "2017-04-30 10:00:00"


    manager = Manager()
    collector_dic = manager.dict()

    start_time = time.time()

    print "Start parallel process"
    print "Start_time: ", date_time_start
    print "End_time: ", date_time_end

    jobs = []

    # Catch is you are looking for unqiue prefixes
    if type_route == 'unique':

        for collector_name in collector_names:

            p = Process(target=compute_update_evolution_unique_prefixes, args=(collector_name, collector_dic, date_time_start, date_time_end))
            jobs.append(p)
            p.start()

        # Wait for this [thread/process] to complete
        for proc in jobs:
            proc.join()

        print collector_dic.keys()

        # Save the dict
        with open("updates_dic_unique_" + incident +  ".json", "wb") as fp:
            json.dump(dict(collector_dic), fp)


    else:
        for collector_name in collector_names:

            p = Process(target=compute_update_evolution, args=(collector_name, collector_dic, date_time_start, date_time_end))
            jobs.append(p)
            p.start()

        # Wait for this [thread/process] to complete
        for proc in jobs:
            proc.join()

        print collector_dic.keys()

        # Save the dict
        with open("updates_dic_" + incident +  ".json", "wb") as fp:
            json.dump(dict(collector_dic), fp)


    print "Finish parallel process"
    print "--- %s seconds ---" % (time.time() - start_time)




if __name__ == "__main__":

    main()


