#!/usr/bin/env python
# -*- encoding: utf-8 -*-
__author__ = 'yzhu7, Julie the big circle'


import csv
import os
import datetime
from gen_traffic_layer.proc_gps import proc
from gen_traffic_layer.road_matching import parse_csv_gps
from gen_traffic_layer.cal_speed import calculate_speed
from gen_traffic_layer.write_geojson import write_geojson
from gen_cleaned_records.read_roadnets import read_roadnets_from_file, get_roads_string
from multiprocessing import Pool
from datetime import timedelta

MATCH_PATH = "./NearestMatch/"
OUTPUT_PATH = "./Result/"
ROADNET_PATH = "./Roadnets/"
FILE_NAME_ROADNETS_SEG = ROADNET_PATH + 'roadnet_segments.csv'
FILE_NAME_ROADNETS = ROADNET_PATH +'roadnet1.csv'
ADJ_MATRIX = ROADNET_PATH +'adjacency_matrix1.txt'
SELECTED_GID = ROADNET_PATH +'selected_gids3.csv'
NUM_OF_SERVER = 6


def run(cmd):
    print cmd
    os.system(cmd)


if __name__ == "__main__":
    N = 4
    for month in range(6,0,-1):
        start_date = '2016-0' + str(month) + '-01'
        start_day = datetime.datetime.strptime(start_date, "%Y-%m-%d")
        cmds = []
        for i in range(0,31):
            TARGET_FILE = "GPS_"+ (start_day + timedelta(days=i)).strftime("%Y_%m_%d")
            if os.path.exists(MATCH_PATH + TARGET_FILE):
                cmds.append("python generate_car.py " + TARGET_FILE)
        mp = Pool(N)
        mp.map(run, cmds)
        mp.close()
        mp.join()
