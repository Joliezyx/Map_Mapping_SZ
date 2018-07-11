#!/usr/bin/env python
# -*- encoding: utf-8 -*-
__author__ = 'yzhu7, Julie the big circle'


import csv
import os
import datetime
from gen_traffic_layer.proc_gps import proc
from gen_traffic_layer.road_matching import parse_csv_gps
from gen_traffic_layer.cal_speed import calculate_speed
from gen_traffic_layer.write_geojson import write_geojson, write_geojson_layout
from gen_cleaned_records.read_roadnets import read_roadnets_from_file, get_roads_string
#from prediction.LR_predict import prepare_datasets
from gen_cleaned_records.road_to_graph1 import read_matched_gps
import sys
import numpy as np

MATCH_PATH = "./NearestMatch/"
OUTPUT_PATH = "./Result/"
ROADNET_PATH = "./Roadnets/"
FILE_NAME_ROADNETS_SEG = ROADNET_PATH + 'roadnet_segments.csv'
FILE_NAME_ROADNETS = ROADNET_PATH +'roadnet1.csv'
ADJ_MATRIX = ROADNET_PATH +'adjacency_matrix1.txt'
SELECTED_GID = ROADNET_PATH +'selected_gids3.csv'
NUM_OF_SERVER = 6



def match(filename, CAR_NUM, MATCH_PATH):
    PSQL_URI = {}
    for i in range(0,NUM_OF_SERVER):
        if i == 5:
            PSQL_URI[i] = 'host=10.127.1.101 port=5432 dbname=testdb user=yzhu7 password=zhuyixuan1234A'
        else:
            PSQL_URI[i] = 'host=10.127.1.16'+ str(i + 1) + ' port=5432 dbname=testdb user=julie00' + str(i + 1) + ' password=zhuyixuan1234A'
    #print "PSQL_URI:\n", PSQL_URI
    FILE_NAME = filename #'./Car/Car_B948R7.txt'
    res = parse_csv_gps(PSQL_URI, FILE_NAME, CAR_NUM)
    res = sorted(res)
    headers = ['ABS_TIME', 'TM', 'CAR_ID',
               'Graph_ID',
               'LONG_interp', 'LAT_interp', 'Percent']

    with open(MATCH_PATH+'/result.txt','wb') as myfile:
       wr = csv.writer(myfile)
       wr.writerow(headers)
       wr.writerows(res)

def main(TARGET_FILE):
    #print TARGET_FILE
    weekday = datetime.datetime.strptime(TARGET_FILE[4:], "%Y_%m_%d").weekday()
    SPEED_PATH = OUTPUT_PATH+TARGET_FILE+"/speed/"
    GEOJSON_PATH = OUTPUT_PATH+TARGET_FILE+"/geojson/"
    GEOJSON_TRAJ_PATH = OUTPUT_PATH+TARGET_FILE+"/geojson_traj/"
    #print TARGET_FILE,OUTPUT_PATH, FILTER_PATH, MATCH_PATH, SPEED_PATH, GEOJSON_PATH

    if not os.path.exists(OUTPUT_PATH+TARGET_FILE):
        os.makedirs(OUTPUT_PATH+TARGET_FILE)
    if not os.path.exists(SPEED_PATH):
        os.makedirs(SPEED_PATH)
    if not os.path.exists(GEOJSON_PATH):
        os.makedirs(GEOJSON_PATH)
    if not os.path.exists(GEOJSON_TRAJ_PATH):
        os.makedirs(GEOJSON_TRAJ_PATH)

    #proc(DATA_PATH+TARGET_FILE, FILTER_PATH)
    #main(FILTER_PATH+"car_", CAR_NUM, MATCH_PATH)
    #calculate_speed(MATCH_PATH,SPEED_PATH,GEOJSON_TRAJ_PATH, graph, roadnet_data,roads, CAR_NUM, TARGET_FILE)
    #write_geojson(SPEED_PATH, GEOJSON_PATH, TIME_WINDOW,roadnet_data[:,[1,16]],roads, weekday)

    car_data, row_count = read_matched_gps(MATCH_PATH + TARGET_FILE,CAR_NUM)
    #print raw_info[1][:10], matched_info[1][:10]
    #data = data[~np.isnan(data).any(axis=1)]
    fout = open(MATCH_PATH + TARGET_FILE +"_pickup_dropoff.txt", "w")
    fout.write("Unix_Time,Lon,Lat,Car_ID,Pickup_Dropoff\n")
    for car_cnt in range(1,CAR_NUM):
        if row_count[car_cnt] == 0:
            continue
        data = np.array(sorted(car_data[car_cnt]))
        info = data[1:,0:4]
        pickupdropoff = np.diff(data[:,4].astype(int),axis = 0)
        new_info = np.concatenate((info,np.array([pickupdropoff]).T.astype(str)),axis = 1)
        pick_drop_list = new_info[np.logical_or(pickupdropoff == 1, pickupdropoff == -1)]
        for item in pick_drop_list:
            fout.write(item[0] + "," + item[1] + "," + item[2] + "," + item[3] + "," + item[4] + "\n")
            print item[0] + "," + item[1] + "," + item[2] + "," + item[3] + "," + item[4] + "\n"
    fout.close()




if __name__ == "__main__":
    #graph, roadnet_data, roadnet_data_linestring = read_roadnets_from_file(FILE_NAME_ROADNETS,FILE_NAME_ROADNETS_SEG,ADJ_MATRIX)
    #roads = get_roads_string(roadnet_data,roadnet_data_linestring)
    CAR_NUM = 25000
    TIME_WINDOW = 20
    #write_geojson_layout('./Result/', roadnet_data[:,[1,16]],roads)
    main('GPS_2016_01_12')

    #num_of_roads = 78296
    #prepare_datasets(OUTPUT_PATH, TIME_WINDOW, graph, num_of_roads)
    """
    #gids = np.loadtxt(SELECTED_GID,usecols=(0,),delimiter=' ')
    #train_LR_all(100,gids)
    """
