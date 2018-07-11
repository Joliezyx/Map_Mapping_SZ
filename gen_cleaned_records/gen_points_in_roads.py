__author__ = 'yzhu7'


import numpy as np
import pandas as pd
import networkx as nx
import csv
import sys
#from collections import defaultdict
from geojson import Feature, LineString, FeatureCollection
import geojson
import math

routing_thresh = 5
interval  = 5

CAR_ID = 'B550VX'
FILE_NAME_GPS_CLEAN = './Result/GPS_clean_B550VX2.txt'
FILE_NAME_GPS_INTERP = './Result/GPS_clean_B550VX1.txt'
FILE_NAME_ROADNETS = './psql_copy/roadnet.csv'
FILE_NAME_ROADNETS_SEG = './psql_copy/roadnet_segments.csv'
FILE_NAME_VERTICES = './psql_copy/vertices.csv'

def read_interp_gps(filename):
    mydata = pd.read_csv(filename,sep=',')
    #remove the records without matching
    mydata = mydata[pd.notnull(mydata['Graph_ID'])]
    return mydata

def read_vertices(filename):
    mydata = pd.read_csv(filename,sep=',')
    data = mydata[[u'id',u'lon',u'lat']]
    return data

def read_roadnets(filename1,filename2):
    mydata = pd.read_csv(filename1,sep=',')
    mydata_linestring = pd.read_csv(filename2,sep=',')
    mydata = mydata.merge(mydata_linestring, left_on=u'gid', right_on=u'gid', how='outer')

    #print mydata.head()
    #select useful data
    data = mydata[[u'gid',u'source',u'target',u'osm_id_x',u'source_osm',u'target_osm',u'st_astext']]#.sort(u'gid')
    #create graph
    edges1 = zip( mydata[u'source'].tolist(), mydata[u'target'].tolist(), mydata[u'length_m'].tolist())
    edges2 = zip( mydata[mydata[u'one_way'] == 2][u'target'].tolist(), mydata[mydata[u'one_way'] == 2][u'source'].tolist(),
                  mydata[mydata[u'one_way'] == 2][u'length_m'].tolist())
    gr = nx.DiGraph()
    for i, (fr, to, w) in enumerate(edges1):
      gr.add_edge(fr, to, weight=w)
    for i, (fr, to, w) in enumerate(edges2):
      gr.add_edge(fr, to, weight=w)
    #gr.add_edges_from(edges1)#, weight = mydata[u'length_m'])
    #gr.add_edges_from(edges2)#, weight = mydata[mydata[u'one_way'] == 2][u'length_m'])
    return gr, data

def get_segment_from_routing(data,routes):
    points = []
    for i in range(0,len(routes)-1):
        try:
            points_string = data.loc[data[u'source'] == routes[i],:].loc[data[u'target'] == routes[i+1],[u'st_astext']] \
                .values.tolist()
            if len(points_string) == 0:
                points_string = data.loc[data[u'target'] == routes[i],:].loc[data[u'source'] == routes[i+1],[u'st_astext']] \
                .values.tolist()
            points_string = points_string[0][0][11:-1]
            points_split = points_string.split(',')
            points.extend(points_split[:-1])
        except:
            e, t, tb = sys.exc_info()
            print "caught", e, t, tb

    #points.extend([points_split[-1]])
    return points

def get_points_from_gid(data, gid):

    points_string = data.loc[data[u'gid'] == gid,[u'st_astext']].values.tolist()[0][0]
    points = points_string[11:-1].split(',')

    return points


def get_closest_point_in_line(data, record):
    """headers = ['ABS_TIME', 'LONG', 'LAT', 'TM', 'Occupy', 'LONG_PRE', 'LAT_PRE', 'TM_DIFF',
               'Graph_ID','OSM_ID','Source','Target','Percent',
               'LONG_interp', 'LAT_interp', 'Dist(GPS)','One_way' , 'Degree' , 'Degree_diff'] """
    gid = record[8]
    LAT = record[14]
    LONG = record[13]
    list_index = 0
    index = 0
    min_dist = 9999
    points_list = data.loc[data[u'gid']==gid,[u'st_astext']].values.tolist()[0][0]
    points_list = points_list[11:-1]
    points = points_list.split(',')
    for point in points:
        long, lat = point.split(' ')
        dist = abs(float(lat) - LAT) + abs(float(long) - LONG)
        if dist < min_dist:
            min_dist = dist
            list_index = index
        index = index + 1
    return list_index


def gen_full_trajectory(gr, data, matched_records):
    headers = ['ABS_TIME', 'LONG', 'LAT', 'TM', 'Occupy', 'LONG_PRE', 'LAT_PRE', 'TM_DIFF',
               'Graph_ID','OSM_ID','Source','Target','Percent',
               'LONG_interp', 'LAT_interp', 'Dist(GPS)','One_way' , 'Degree' , 'Degree_diff']
    # add first segment
    i= 0
    list_records = matched_records[i]
    gid = list_records[0][8]
    points = get_points_from_gid(data, gid)
    cnt = 0
    list_index_pre = 0
    for row in list_records:
        list_index = get_closest_point_in_line(roadnet_data,row)
        points_str = ','.join(points[list_index_pre:list_index+1])
        matched_records[i][cnt].extend([points_str])
        list_index_pre = list_index
        cnt = cnt + 1

    i = 1
    while i > 0 and i < len(matched_records):
        temp_records = []
        print "row:",i
        list_records = matched_records[i]
        source_current = list_records[0][10]
        time_current = list_records[0][0]
        gid = list_records[0][8]
        points = get_points_from_gid(data, gid)
        j = i - 1
        list_index_delta = 0
        pop_j = 0
        while j >= 0:
            if pop_j == routing_thresh:
                for x in range(0,pop_j):
                    matched_records.insert(j+1,temp_records[x])
                    j = j+1
                    i = i+1
                print "pop:",i, len(matched_records)
                if i == len(matched_records):
                    break
                matched_records.pop(i)
                i = i - 1
                break
            print j
            target_pre = matched_records[j][0][11]
            time_pre = matched_records[j][-1][0]

            try:
                shortest_path = nx.shortest_path(gr, source=target_pre, target=source_current)
                if len(shortest_path) < 3*routing_thresh:
                    if len(shortest_path) > 1:
                        #print points
                        points_pre = get_segment_from_routing(data, shortest_path)
                        list_index_delta = len(points_pre)
                        points_pre.extend(points)
                        points = points_pre
                        #print "add routing:",points

                    list_index_pre = 0
                    cnt = 0
                    for row in list_records:
                        list_index = get_closest_point_in_line(roadnet_data,row) + list_index_delta
                        points_str = ','.join(points[list_index_pre:list_index+1])
                        matched_records[i][cnt].extend([points_str])
                        list_index_pre = list_index
                        cnt = cnt + 1
                    if list_index_pre < len(points) - 1 and len(matched_records[i][-1][-1]) > 0:
                        matched_records[i][-1][-1] = matched_records[i][-1][-1] + ',' + ','.join(points[list_index_pre:])
                    break
                elif time_current - time_pre > interval*10:
                    list_index_pre = 0
                    cnt = 0
                    for row in list_records:
                        list_index = get_closest_point_in_line(roadnet_data,row) + list_index_delta
                        points_str = ','.join(points[list_index_pre:list_index+1])
                        matched_records[i][cnt].extend([points_str])
                        list_index_pre = list_index
                        cnt = cnt + 1
                    if list_index_pre < len(points) - 1 and len(matched_records[i][-1][-1]) > 0:
                        matched_records[i][-1][-1] = matched_records[i][-1][-1] + ',' + ','.join(points[list_index_pre:])
                    break

                else:
                    print "path too long!"
                    temp_records.insert(0,matched_records.pop(j))
                    i = i - 1
                    j = j-1
                    pop_j = pop_j + 1
                    # connect to past path
                    """
                    if j < 0:
                        break
                    else:
                        target_pre = matched_records[j][0][11]
                        try:
                            shortest_path = nx.shortest_path(gr, source=target_pre, target=source_current)
                            points_pre = get_segment_from_routing(data, shortest_path)
                            points_str = ','.join(points_pre)
                            matched_records[i][0][-1] = points_str + ',' + matched_records[i][0][-1]
                        except:
                            break"""



            except:
                e, t, tb = sys.exc_info()

                print "caught", e, t, tb
                temp_records.insert(0,matched_records.pop(j))
                matched_records.pop(j)
                i = i - 1
                j = j-1
                pop_j = pop_j + 1
                print "no path!"

        i = i + 1
    return matched_records


if __name__ == '__main__':
    graph, roadnet_data = read_roadnets(FILE_NAME_ROADNETS,FILE_NAME_ROADNETS_SEG)
    records = read_interp_gps(FILE_NAME_GPS_INTERP)
    vertices = read_vertices(FILE_NAME_VERTICES)
    cnt = 2
    road_matches = []
    cnt = 0
    iter1 = 0
    """
    while iter1 < len(records.index):
        gid = records.loc[records.index[iter1],['Graph_ID']].values.tolist()[0]
        firstrow = records.loc[records.index[iter1],:]
        iter2 = iter1+1
        while iter2 < len(records.index) and records.loc[records.index[iter1],['Graph_ID']].values.tolist()[0] == records.loc[records.index[iter2],['Graph_ID']].values.tolist()[0]:
            iter2 = iter2 + 1
        road_matches.append(records.loc[records.index[iter1:iter2],:].values.tolist())
        iter1 = iter2
        #print iter1

    result = gen_full_trajectory(graph, roadnet_data, road_matches)
    headers = ['ABS_TIME', 'LONG', 'LAT', 'TM', 'Occupy', 'LONG_PRE', 'LAT_PRE', 'TM_DIFF',
               'Graph_ID','OSM_ID','Source','Target','Percent',
               'LONG_interp', 'LAT_interp', 'Dist(GPS)','One_way' , 'Degree' , 'Degree_diff','Points']

    with open(FILE_NAME_GPS_CLEAN,'wb') as myfile:
        wr = csv.writer(myfile)
        wr.writerow(headers)
        for list_records in result:
            wr.writerows(list_records)
    """
    #graph, roadnet_data = read_roadnets(FILE_NAME_ROADNETS,FILE_NAME_ROADNETS_SEG)
    records = read_interp_gps(FILE_NAME_GPS_CLEAN)
    #gid, list_index = get_gid_from_sourcetarget(roadnet_data,)
    points_in_lines = []
    lines = records.loc[:,['TM','Points','ABS_TIME','Occupy']].values.tolist()
    cnt = 0
    time_pre = records.loc[0,['ABS_TIME']].values.tolist()[0]
    occupy_pre = records.loc[0,['Occupy']].values.tolist()[0]
    pick_or_drop = 0
    for line in lines:
        time_current = line[0]
        occupy_current = line[3]
        if cnt == 0:
            cnt = cnt + 1
            continue
        cnt = cnt + 1
        #print line[0]
        points_in_line = []
        if isinstance(line[1], float):
            continue
        else:
            string_split = line[1].split(',')
            if len(string_split) == 1:
                continue
            if occupy_current == 1 and occupy_pre == 0:
                pick_or_drop = 2
            elif occupy_current == 0 and occupy_pre == 1:
                pick_or_drop = -2
            elif occupy_current ==1 and occupy_pre ==1:
                pick_or_drop = 1
            else:
                pick_or_drop = 0
            for item in string_split:
                long, lat = item.split(' ')
                points_in_line.append((float(long), float(lat)))
            points_in_lines.append(Feature(geometry=LineString(points_in_line), properties={'Time':line[0],'ABS_TIME':line[2],'ABS_TIME_PRE': time_pre,'Pick_or_drop':pick_or_drop}))
            time_pre = time_current
            occupy_pre = occupy_current

    geom_in_geojson = FeatureCollection(points_in_lines)  # doctest: +ELLIPSIS
    with open('./Result/one_car_traj.geojson', 'w') as outfile:
        geojson.dump(geom_in_geojson, outfile)