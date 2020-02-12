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
from datetime import datetime
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



def calculate_unix_time(date_and_time):

    '''
    Calculate unix time elapsed from a datetime object
    @param: date_and_time (datetime): datetime object
    @return: Seconds elapsed using unix reference (int)
    '''
    return int((date_and_time - datetime.utcfromtimestamp(0)).total_seconds())




def compute_collector_evolution_time_series(collector, collector_dic, start_time, end_time):

    '''
    Collect the number of feeders (ASes and routers) for each colelctor. Collection is done in parallel for each of the incidents.
    @param: collector (str): name of the collector
    @param: collector_dic (dict): shared directory to store the data
    @param: start_time (str): start of the data collection in format "YYY-MM-DD HH:MM:SS"
    @param: end_time (str): end of the data collection in format "YYY-MM-DD HH:MM:SS"
    @return: None
    '''

    start = time.time()

    # Dic of dics representing the collector evolution
    # collector_evolution = defaultdict(lambda: defaultdict(list))

    internal_dic = defaultdict(list)

    # Select the period of interest
    date_range = pd.date_range(start=start_time, end=end_time, freq="2H")
    # print date_range
    # print len(date_range)

    # Consider number named collectors (they have length 2)
    if len(collector) == 2:
        collector_name = "route-views" + collector[1:]
    else:
        collector_name = "route-views." + collector[1:]


    for date in date_range:

        print collector[1:], collector_name, date

        # Create a new bgpstream instance and a reusable bgprecord instance
        stream = BGPStream()
        rec = BGPRecord()

        stream.add_filter("collector", collector_name)

        # Consider RIBs dumps only
        stream.add_filter("record-type","ribs")

        # Calculate unix time seconds
        center_time = calculate_unix_time(date)

        # Consider this time interval:
        stream.add_interval_filter(center_time - 300, center_time + 300) # /pm 5 minutes

        # Start the stream
        stream.start()

        # Get next record
        while(stream.get_next_record(rec)):

            if rec.status == "valid":

                element = rec.get_next_elem()

                while element:

                    if (str(element.peer_asn), element.peer_address) not in internal_dic[date.to_pydatetime().strftime("%Y-%m-%d %H:%M:%S")]:
                        internal_dic[date.to_pydatetime().strftime("%Y-%m-%d %H:%M:%S")].append((str(element.peer_asn), element.peer_address))

                    # collector_evolution[rec.collector][date.to_pydatetime().strftime("%Y-%m-%d %H:%M:%S")] = (str(element.peer_asn), element.peer_address)
                    element = rec.get_next_elem()

    print collector[1:], internal_dic.keys()
    collector_dic[collector[1:]] = internal_dic

    print "Done:", collector[1:], "time:", time.time() - start



def main():

    '''
    Compute the number of feeders (ASes and routers) per collector during it lifetime until 2018-01. Collection is done in parallel for each of the events
    @param: None
    @return: None
    '''

    # How to run it
    # python compute_collector_evolution "incident"

    # Location of the incident
    incident = sys.argv[1]
    print "Incident: ", incident

    # Change directory
    os.chdir("/your_directory_path/" + incident + "/data/")


    # Define collectors to get the data
    if incident == "Indonesia":
        # 16 collectors
        collector_names = ["_eqix", "_isc", "_jinx", "_kixp", "_linx", "_nwax", "_perth", "_saopaulo", "_soxrs", "_sydney", "_telxatl", "_wide", "_2", "_3", "_4", "_6"]
        # collector_names = ["_kixp", "_perth"]
        date_time_start = "2014-03-30 18:00:00"
        date_time_end = "2014-04-05 22:00:00"

    elif incident == "Malaysia":
        # 18 collecotrs
        collector_names = ["_eqix", "_isc", "_jinx", "_kixp", "_linx", "_nwax", "_perth", "_saopaulo", "_sfmix", "_sg", "_soxrs", "_sydney", "_telxatl", "_wide", "_2", "_3", "_4", "_6"]
        # collector_names = ["_kixp", "_perth"]
        date_time_start = "2015-06-09 08:00:00"
        date_time_end = "2015-06-15 12:00:00"

    elif incident == "India":
        # 18 collecotrs
        collector_names = ["_eqix", "_isc", "_jinx", "_kixp", "_linx", "_nwax", "_perth", "_saopaulo", "_sfmix", "_sg", "_soxrs", "_sydney", "_telxatl", "_wide", "_2", "_3", "_4", "_6"]
        # collector_names = ["_kixp", "_perth"]
        date_time_start = "2015-11-03 04:00:00"
        date_time_end = "2015-11-09 16:00:00"

    elif incident == "Belarus":
        # 13 collectors
        collector_names = ["_eqix", "_isc", "_jinx", "_kixp", "_linx", "_perth", "_saopaulo", "_sydney", "_telxatl", "_wide", "_2", "_4", "_6"]
        # collector_names = ["route-views.kixp", "route-views.perth"]
        date_time_start = "2013-02-24 08:00:00"
        date_time_end = "2013-03-01 06:00:00"


    elif incident == "Iceland":
        # 14 collectors
        collector_names = ["_eqix", "_isc", "_jinx", "_kixp", "_linx", "_perth", "_saopaulo", "_sydney", "_telxatl", "_wide", "_2", "_3", "_4", "_6"]
        # collector_names = ["_kixp", "_perth"]
        date_time_start = "2013-07-28 06:00:00"
        date_time_end = "2013-08-03 18:00:00"


    elif incident == "Russia":
        # 18 collectors excluding Chicago
        collector_names = ["_eqix", "_isc", "_jinx", "_kixp", "_linx", "_nwax", "_perth", "_saopaulo", "_sfmix", "_sg", "_soxrs", "_sydney", "_telxatl", "_wide", "_2", "_3", "_4", "_6"]
        # collector_names = ["_kixp", "_perth"]
        date_time_start = "2017-04-23 10:00:00"
        date_time_end = "2017-04-29 10:00:00"

    manager = Manager()
    collector_dic = manager.dict()

    start_time = time.time()

    print "Start parallel process"
    print "Start_time: ", date_time_start
    print "End_time: ", date_time_end

    jobs = []
    for collector in collector_names:

        p = Process(target=compute_collector_evolution_time_series, args=(collector, collector_dic, date_time_start, date_time_end))
        jobs.append(p)
        p.start()
        #args.append([collector, similarity_dic])

    # Wait for this [thread/process] to complete
    for proc in jobs:
        proc.join()

    print collector_dic.keys()

    # Save the dict

    with open("dic_feeders_evolution_" + incident + ".p", "wb") as fp:
        pickle.dump(dict(collector_dic), fp)


    print "Finish parallel process"
    print "--- %s seconds ---" % (time.time() - start_time)



if __name__ == "__main__":

    main()


