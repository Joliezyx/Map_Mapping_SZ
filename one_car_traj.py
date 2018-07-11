__author__ = 'yzhu7, the big circle'


import csv
import os
import numpy as np
import networkx as nx
from gen_cleaned_records.write_geojson_by_gid import write_geojson_traj
from gen_cleaned_records.read_roadnets import read_roadnets_from_file, get_roads_string, write_matrix
from gen_cleaned_records.road_to_graph1 import read_interp_gps_allcar, read_allcar, read_interp_gps
from gen_cleaned_records.clean_records import clean_data_1car_from_candidates

CAR_ID = 1
DATA_PATH = "./Car/"
ROADNET_PATH = "./Roadnets/"
FILE_NAME_ROADNETS_SEG = ROADNET_PATH + 'roadnet_segments.csv'
FILE_NAME_ROADNETS = ROADNET_PATH +'roadnet1.csv'
ADJ_MATRIX = ROADNET_PATH +'adjacency_matrix1.txt'
TARGET_FILE = DATA_PATH + 'out_' + str(CAR_ID) + '.txt'
#TARGET_FILE = DATA_PATH + 'result.txt'
FILE_NAME_GPS_CLEAN = DATA_PATH + 'Clean_Car_' + str(CAR_ID) + '.txt'

date = '2014-12-01'
#date = '2016-06-29'

if __name__ == '__main__':
    #write_matrix(FILE_NAME_ROADNETS)
    graph, roadnet_data, roadnet_data_linestring = read_roadnets_from_file(FILE_NAME_ROADNETS,FILE_NAME_ROADNETS_SEG,ADJ_MATRIX)
    roads = get_roads_string(roadnet_data, roadnet_data_linestring)


    """
    # for testing ===================================================== ###
    gid = 39032
    print gid+1
    print repr(np.array(roads[gid]))
    shortest_path = nx.shortest_path(graph, source=str("39032"), target=str("66762"))
            #if gid_pre == 26634 and gid_current == 58537:
    print "shortest_path",repr(np.array(shortest_path))

    """

    #print roads[63614]
    GEOJSON_PATH = DATA_PATH+"/geojson/"
    if not os.path.exists(GEOJSON_PATH):
        os.makedirs(GEOJSON_PATH)

    #read_allcar(TARGET_FILE)

    raw_info, matched_info, row_count = read_interp_gps(TARGET_FILE)
    clean_data = clean_data_1car_from_candidates(raw_info,matched_info,row_count,graph)
    #print clean_data[0]

    headers = ['ABS_TIME', 'LONG', 'LAT', 'TM', 'Occupy', 'Graph_ID','Percent']

    with open(FILE_NAME_GPS_CLEAN,'wb') as myfile:
        wr = csv.writer(myfile)
        wr.writerow(headers)
        for list_records in clean_data:
            wr.writerow(list_records)



    #write_geojson_traj(FILE_NAME_GPS_CLEAN,GEOJSON_PATH, roadnet_data,roads, graph, CAR_ID, date)


