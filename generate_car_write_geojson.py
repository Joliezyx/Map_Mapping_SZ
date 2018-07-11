#!/usr/bin/env python
# -*- encoding: utf-8 -*-
__author__ = 'yzhu7, Julie the big circle'


import csv
import os
import datetime
from gen_traffic_layer.proc_gps import proc
from gen_traffic_layer.road_matching import parse_csv_gps
from gen_traffic_layer.cal_speed_with_geojson import calculate_speed_with_geojson
from gen_traffic_layer.write_geojson import write_geojson
from gen_cleaned_records.read_roadnets import read_roadnets_from_file, get_roads_string
import sys

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
    #calculate_speed_with_geojson(MATCH_PATH,SPEED_PATH,GEOJSON_TRAJ_PATH, graph, roadnet_data,roads, CAR_NUM, TARGET_FILE)
    write_geojson(SPEED_PATH, GEOJSON_PATH, TIME_WINDOW,roadnet_data[:,[1,16]],roads)

    """
    num_of_roads = 78296
    prepare_datasets(OUTPUT_PATH, TIME_WINDOW, SELECTED_GID, graph, num_of_roads)
    #gids = np.loadtxt(SELECTED_GID,usecols=(0,),delimiter=' ')
    #train_LR_all(100,gids)
    """

if __name__ == "__main__":
    graph, roadnet_data, roadnet_data_linestring = read_roadnets_from_file(FILE_NAME_ROADNETS,FILE_NAME_ROADNETS_SEG,ADJ_MATRIX)
    roads = get_roads_string(roadnet_data,roadnet_data_linestring)
    CAR_NUM = 25000
    TIME_WINDOW = 20
    main(sys.argv[1])
