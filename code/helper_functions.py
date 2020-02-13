from __future__ import division
from datetime import datetime, timedelta
import itertools
import time
import numpy as np
import pandas as pd
import sqlite3


def partition_df(start_date, end_date, window_length, offset):

    '''
    Generate the the start and end dates stream to partition the data frames
    @param start_date: Beginning date (str)
    @param end_date: Final date (srt)
    @param window_length: Number of hours of the window (float)
    @param offset: Offset of the window in hours (float)
    '''

    # List of tuples to store the partition
    partition_list = []

    # Get the range of dates, beginning and end dates
    range_of_dates = pd.date_range(start=start_date, end=end_date, freq='H')
    # print 'range of dates', range_of_dates
    start_date_datetime = datetime.strptime(start_date, '%Y-%m-%d %H:%M:%S')
    end_date_datetime = range_of_dates[-1]

    # print range_of_dates[0], range_of_dates[-1]

    # Check if reach the last date
    while start_date_datetime + timedelta(hours=window_length) < end_date_datetime:
        partition_list.append((start_date_datetime.strftime('%Y-%m-%d %H:%M:%S'), (start_date_datetime + timedelta(hours=window_length)).strftime('%Y-%m-%d %H:%M:%S')))
        start_date_datetime = start_date_datetime + timedelta(hours=offset)

    # Add the last set of dates
    if start_date_datetime != end_date_datetime:
        partition_list.append((start_date_datetime.strftime('%Y-%m-%d %H:%M:%S'), end_date_datetime.strftime('%Y-%m-%d %H:%M:%S')))

    return partition_list


def map_random_algorithm_to_regular_espace(date_time_start, date_time_end, m):

    '''
    Map the list of ramdonly selected intervals to the original space
    @param start_date: date and time of the start of the incident (str)
    @param end_date: date and time of the end of the incident (str)
    @param m: resolution parameter in days (int)
    '''

     # Create the partition in the new time domain (array of arrays of strings)
    date_range_list_new_space = np.array(partition_df(date_time_start, date_time_end, m, m))
    indicator_new_space = np.array([np.random.binomial(1, p=0.5) for i in range(len(date_range_list_new_space))])

    return indicator_new_space


def map_ground_truth_to_regular_espace(start_date, end_date, list_of_events_regular_space, m):

    '''
    Map the list of regular events (datetimes) into the new vector od espaces
    @param start_date: date and time of the start of the incident (str)
    @param end_date: date and time of the end of the incident (str)
    @param list_of_events_regular_space (list of datetimes)
    @param m: resolution parameter in days (int)
    '''

    # Create the partition in the new time domain (array of arrays of strings)
    date_range_list_new_space = np.array(partition_df(start_date, end_date, m, m))

    # Initialize the indicator vector in the new space
    indicator_new_space = np.array([0 for index in range(len(date_range_list_new_space))])

    # Map events in the regular space to an indicator vector in the new space
    for interval in list_of_events_regular_space:
        for new_index in range(0, len(indicator_new_space)):
            # print interval, type(interval)
            if interval > datetime.strptime(date_range_list_new_space[new_index][0], '%Y-%m-%d %H:%M:%S') and interval <= datetime.strptime(date_range_list_new_space[new_index][1], '%Y-%m-%d %H:%M:%S'):
                # print 'entre', datetime.strptime(date_range_list_new_space[new_index][0], '%Y-%m-%d %H:%M:%S'), datetime.strptime(date_range_list_new_space[new_index][1], '%Y-%m-%d %H:%M:%S')
                indicator_new_space[new_index] = 1

    return indicator_new_space


def map_signal_to_regular_space(signal_x, signal_y, date_time_start, date_time_end, m):

    '''
    Calculate ML performance metric for every possible thereshold within the domain of the signal
    @param signal_x: array containing the x-axis of the signal (int)
    @param signal_y: array containing the y-axis of the signal (float)
    @param date_time_start: date and time of the start of the incident (str)
    @param date_time_end: date and time of the end of the incident (str)
    @param m: Resolution parameter in days (float)
    '''

    indicator_new_space = []

    # Compute the time interval partition in the new space
    date_range_list_new_space = np.array(partition_df(date_time_start, date_time_end, m, m))
    #print date_range_list_new_space
    #print date_range_list_new_space[0], date_range_list_new_space[-1]
    # print date_range_list_new_space[:,0], len(date_range_list_new_space[:,0]), type(date_range_list_new_space[:,0])
    # print date_range_list_new_space[:,1], len(date_range_list_new_space[:,1]), type(date_range_list_new_space[:,1])

    for threshold in np.linspace(min(signal_y), max(signal_y), num=100, endpoint=False):

        # print  threshold, max(signal_y)

        positions_original_space = signal_y >= threshold
        # print len(positions_original_space), positions_original_space
        # print len(signal_x), len(signal_y)
        # positions_original_space = positions_original_space.astype(int)
        # print positions_original_space
        # indicator_new_space.append(positions_original_space)

        # Get the compromised intervals
        compromised_intervals = signal_x[positions_original_space]
        # print compromised_intervals
        # print compromised_intervals[0], type(compromised_intervals[0])

        # Array of ocurrence of events
        indicator_aux = np.array([0 for index in range(len(date_range_list_new_space))])

        # break

        # Look at the date of the interval to correlate
        pivot_dates = np.array([datetime.strptime(date, '%Y-%m-%d %H:%M:%S') for date in date_range_list_new_space[:,0]])

        for interval in compromised_intervals:

            # print interval
            differences = pivot_dates - interval

            delta = [np.abs(difference.total_seconds()) for difference in differences]
            index_with_anomaly = np.argmin(delta)
            indicator_aux[index_with_anomaly] = 1

        #     for new_index in range(0, len(indicator_aux)):
        #         # print datetime.strptime(date_range_list_new_space[new_index][0], '%Y-%m-%d %H:%M:%S')
        #         if interval >= datetime.strptime(date_range_list_new_space[new_index][0], '%Y-%m-%d %H:%M:%S') and interval <= datetime.strptime(date_range_list_new_space[new_index][1], '%Y-%m-%d %H:%M:%S'):
        #             # print 'entre', datetime.strptime(date_range_list_new_space[new_index][0], '%Y-%m-%d %H:%M:%S'), datetime.strptime(date_range_list_new_space[new_index][1], '%Y-%m-%d %H:%M:%S')
        #             indicator_aux[new_index] = 1
        # print indicator_aux
        # break
        indicator_new_space.append(indicator_aux)


    # print 'predicted', len(indicator_new_space[-1]), indicator_new_space[-1], list(indicator_new_space[-1]).index(1)
    return indicator_new_space


def map_signal_to_regular_space_one_shot(signal_x, signal_y, array_mean, array_sd, date_time_start, date_time_end, m):

    '''
    Calculate ML performance metric for every possible thereshold within the domain of the signal
    @param signal_x: array containing the x-axis of the signal (int)
    @param signal_y: array containing the y-axis of the signal (float)
    @param array_mean: array with the moving average (float)
    @param array_sd: array with the moving sd (float)
    @param date_time_start: date and time of the start of the incident (str)
    @param date_time_end: date and time of the end of the incident (str)
    @param m: Resolution parameter in days (float)
    '''

    # Compute the time interval partition in the new space
    date_range_list_new_space = np.array(partition_df(date_time_start, date_time_end, m, m))
    # print "date_range_list_new_space", date_range_list_new_space
    #print date_range_list_new_space[0], date_range_list_new_space[-1]
    # print date_range_list_new_space[:,0], len(date_range_list_new_space[:,0]), type(date_range_list_new_space[:,0])
    # print date_range_list_new_space[:,1], len(date_range_list_new_space[:,1]), type(date_range_list_new_space[:,1])

    #for threshold in np.linspace(min(signal_y), max(signal_y), num=100, endpoint=False):

    # print  threshold, max(signal_y)

    sigma = 2
    positions_original_space = np.greater(signal_y, array_mean + sigma*array_sd)
    # print "anomalous positions", len(positions_original_space), positions_original_space
    # print len(signal_x), len(signal_y)
    # positions_original_space = positions_original_space.astype(int)
    # print positions_original_space
    # indicator_new_space.append(positions_original_space)

    # Get the compromised timestamps
    compromised_timestamps = signal_x[positions_original_space]
    # print "compromised timestamps", compromised_timestamps
    # print compromised_intervals[0], type(compromised_intervals[0])

    # Array of ocurrence of events
    indicator_aux = np.array([0 for index in range(len(date_range_list_new_space))])

    # Look at the start date of the interval to correlate
    pivot_dates = np.array([datetime.strptime(date, '%Y-%m-%d %H:%M:%S') for date in date_range_list_new_space[:,0]])

    for timestamp in compromised_timestamps:

        # print interval
        differences = pivot_dates - timestamp

        delta = [np.abs(difference.total_seconds()) for difference in differences]
        index_with_anomaly = np.argmin(delta)
        indicator_aux[index_with_anomaly] = 1

    # print 'predicted', len(indicator_new_space[-1]), indicator_new_space[-1], list(indicator_new_space[-1]).index(1)
    return indicator_aux


def compute_MCT(collector_name, asn_origin, conn):
    '''
    Verify if the updates received by the collector from asn_origin are likely to be from a BGP table transfer
    @param collector_name: Name of the collector (str)
    @param asn_origin: AS number of the perpetrator (str)
    @param conn: Database connector (obj)
    @return table_transfer: If it is likely to be a BGP table transfer (boolean)
    '''

    query = ("select time, peer_asn, count(distinct(prefix)) as volume "
       "from {} "
       "where origin_asn = {} "
       "group by time, peer_asn").format(collector_name, asn_origin)

    # Get a data frame with the required info
    df = pd.read_sql_query(query, conn)

    # Create a new column with datetimes
    df['datetime'] = pd.to_datetime(df['time'], unit='s')

    # Assign the datetime column as the index of the data frame
    df = df.set_index('datetime')


    # Check if there is a peer_asn that sends more than 50000 distint updates in an interval of 10 minutes
    # Cheng et al. Longitudinal analysis of BGP monitor session failures. SIGCOMM Communication Rev. 2010
    # Cheng et al. Identifying BGP routing table transfers. Computer Networks 2011.
    # Number of prefixes by feeder http://www.routeviews.org/peers/peering-status-by-collector.html
    # http://bgp.potaroo.net/
    if np.sum(df.groupby('peer_asn')['volume'].resample('10T').sum().values > 50000) == 0:
        return False

    else:
        return True


def compute_burstiness(inter_event_times_list):
    '''
    Compute the burstiness of a time series based on the definition of K.-I. Goh and A.-L. Barabasi, Burstiness and memory in complex systems, EPL, 2008
    @param inter_event_times_list: List with the times of the announcements in UNIX time (int)
    @return burstiness_parameter: The burstiness of the time series (float)
    '''

    time_array = np.array(inter_event_times_list)
    seconds_array = np.array([(time_array[index] - time_array[index-1]) for index in range(1, len(time_array))])

    mu = np.mean(seconds_array)
    sigma = np.std(seconds_array)

    return (sigma - mu)/(sigma + mu)


def compute_burstiness_modified(inter_event_times_list):
    '''
    Compute the burstiness of a time series based on the modified definition of E.-K. Kim and H.-H. Jo, Measuring burstiness for finite event sequences, PRE, 2016
    @param inter_event_times_list: List with the times of the announcements in UNIX time (int)
    @return burstiness_parameter: The burstiness of the time series (float)
    '''

    time_array = np.array(inter_event_times_list)
    seconds_array = np.array([(time_array[index] - time_array[index-1]) for index in range(1, len(time_array))])

    mu = np.mean(seconds_array)
    sigma = np.std(seconds_array)

    n = len(seconds_array)
    b = (sigma - mu)/(sigma + mu)

    numerator = (n + 1)**0.5 - (n - 1)**0.5 + ((n + 1)**0.5 + (n - 1)**0.5)*b
    denominator = (n + 1)**0.5 + (n - 1)**0.5 - 2 + ((n + 1)**0.5 - (n - 1)**0.5 - 2)*b

    return numerator/denominator


def create_connection(db_file):
    '''
    Create a database connection to the SQLite database specified by db_file
    @param db_file: database file (str)
    @return: Connection object or None
    '''

    try:
        conn = sqlite3.connect(db_file)
        return conn
    except:
        print "Error"

    return None



