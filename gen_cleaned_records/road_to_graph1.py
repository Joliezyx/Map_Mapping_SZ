__author__ = 'yzhu7, the big circle'
#import pydevd
import csv
import numpy as np
import sys

max_match = 5
raw_data_col = 5
info_match = 2


def read_interp_gps(filename):
    f = open(filename, 'rb')
    reader = csv.reader(f)
    headers = reader.next()
    #print headers
    raw_info = {}
    matched_info = {}
    row_count = 0
    for row in reader:
        if len(row) > raw_data_col:
            raw_info[row_count] = row[0:raw_data_col]
            matched_info[row_count] = []

            for i in range(raw_data_col,len(row),info_match):
                #print raw_data_col + i, raw_data_col + i+info_match
                matched_info[row_count].append(row[i: i+info_match])

            row_count += 1


    #remove the records without matching
    return raw_info, matched_info, row_count


def read_allcar(filename):
    data = np.genfromtxt(filename,skip_header = 1, usecols = ([0,2,4,5,3,6]), delimiter = ',')
    print "length all", len(data)
    #print data[:,1]
    for car_id in range(1,20000):
        print car_id
        mydata = data[data[:,1] == car_id,:]

        if len(mydata) == 0:
            continue
        print "length", len(mydata),mydata[0],"car_id",car_id
        mydata = mydata[~np.isnan(mydata).any(axis=1)]
        #print "length", len(data),data[0]
        np.savetxt('./Car/Car_' + str(car_id) + '.txt',mydata, delimiter = ',', fmt = "%d %d %f %f %d %f")
    #remove the records without matching



def read_interp_gps_allcar(filename):
    data = np.genfromtxt(filename,delimiter = ' ')
    raw_info = data[:,0:4]
    matched_info = data[:,4:6]
    #print len(raw_info),len(matched_info)
    #remove the records without matching
    return raw_info.tolist(), matched_info.tolist(), len(raw_info)




def read_matched_gps(filename, CAR_NUM):
    f = open(filename, 'rb')
    reader = csv.reader(f)
    headers = reader.next()
    #print headers
    car_data = {}
    row_count = {}
    for j in range(1,CAR_NUM):
        car_data[j] = []
        row_count[j] = 0

    cnt = 0
    for row in reader:
        if len(row) != raw_data_col+info_match:
            continue
        try:
            car_id = int(row[3])
        except:
            continue
        if car_id >= CAR_NUM:
            continue
        car_data[car_id].append(row[0:raw_data_col+info_match])
        row_count[car_id] += 1
        cnt += 1

        if cnt % 1000000 == 0:
            print '%s...' %cnt,
            sys.stdout.flush()
        """
        if cnt % 2000000 == 0:
            break
            """


    #remove the records without matching
    return car_data, row_count


