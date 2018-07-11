__author__ = 'Yiquan Wang'

from geojson import Feature, LineString, FeatureCollection
import geojson

import numpy as np
import networkx as nx
import csv
import collections
import time
import datetime



def write_geojson_traj(records,GEOJSON_PATH, roadnet_data,roads, graph,CAR_ID,date):
    routing_thres = 30

    """
    # separate data by gid
    with open(FILE_NAME_GPS_CLEAN, 'rb') as f:
        reader = csv.reader(f)
        headers = reader.next()
        records = list(reader)

    #print "cleaned records from last step", len(records)
    records_origin = np.array(records)
    records_compress = []
    iter1 = 0
    while iter1 < len(records_origin):
        gid = records_origin[iter1,5]
        iter2 = iter1+1
        while iter2 < len(records_origin) and gid == records_origin[iter2,5]:
            iter2 = iter2 + 1
        records_compress.append(records_origin[iter1:iter2,:].tolist())
        iter1 = iter2

    records = []
    for item in records_compress:
        records.append(item[0])

        if len(item) > 1:
            records.append(item[-1])
    #print "compressed records", len(records)
    records = records_origin.tolist()
    """

    points_in_lines = []

    time_pre = int(records[0][0])
    occupy_pre = int(records[0][4])
    gid_pre = int(records[0][5])
    percent_pre = float(records[0][6])

    cnt = 1
    while cnt < len(records):
        #current info

        time_current = int(records[cnt][0])
        occupy_current = int(records[cnt][4])
        gid_current = int(records[cnt][5])
        percent_current = float(records[cnt][6])

        points_in_line = []
        #print "==================================== new records =========================================================="
        #print "gid_current,gid_pre,percent_current,percent_pre,roadnet_data[int(gid_current) - 1][16],time_pre,time_current"
        #print gid_current,gid_pre,percent_current,percent_pre,roadnet_data[int(gid_current) - 1][16],time_pre,time_current

        routing_points, dist = gen_points_from_routing(gid_current,gid_pre,percent_current,percent_pre,roadnet_data, roads, graph,routing_thres)

        if occupy_current == 1 and occupy_pre == 0:
            pick_or_drop = 2
        elif occupy_current == 0 and occupy_pre == 1:
            pick_or_drop = -2
        elif occupy_current ==1 and occupy_pre ==1:
            pick_or_drop = 1
        else:
            pick_or_drop = 0
        time_diff = time_current - time_pre

        if dist < 40*time_diff and len(routing_points) > 0:

            for item in routing_points:
                points_in_line.append(tuple(item))
            time_string = datetime.datetime.utcfromtimestamp(time_current + 3600*8).strftime('%Y-%m-%d %H:%M:%S')
            points_in_lines.append(Feature(geometry=LineString(points_in_line),
                                           properties={'ABSTIME':time_current,'TIME':time_string,
                                                       'OSM_ID': int(roadnet_data[int(gid_current) - 1][16]), 'GID': gid_current,
                                                       'Percent':percent_current,'GID_pre': gid_pre,'Percent_pre':percent_pre,
                                                       'Pick_or_drop':pick_or_drop, "ABS_TIME_PRE":time_pre,"Dist":dist}))
            #points_in_line_pre = points_in_line
            #print time_current,time_string
            #print  repr(np.array(routing_points))


            """
            if time_current == 1417406075:
                ##print "gid_pre,percent_pre,gid_current,percent_current"
                ##print gid_pre,percent_pre,gid_current,percent_current
                ##print  repr(np.array(routing_points))
                return
            """

            #mark the pre info
            time_pre = time_current
            occupy_pre = occupy_current
            gid_pre = gid_current
            percent_pre = percent_current

        elif time_diff < 300 and cnt > 2:
            records.pop(cnt)
            records.pop(cnt - 1)
            if len(points_in_lines) > 0:
                points_in_lines.pop()
            time_pre = int(records[cnt - 2][0])
            occupy_pre = int(records[cnt - 2][4])
            gid_pre = int(records[cnt - 2][5])
            percent_pre = float(records[cnt - 2][6])

            cnt = cnt - 2
        elif cnt < len(records) - 1:

            time_pre = int(records[cnt + 1][0])
            occupy_pre = int(records[cnt + 1][4])
            gid_pre = int(records[cnt + 1][5])
            percent_pre = float(records[cnt + 1][6])
            cnt += 1
        else:
            break

        cnt += 1

    #print "len(points_in_lines)",len(points_in_lines)
    points_in_lines_new = reconnect_points_in_lines(points_in_lines, roadnet_data,roads, graph, date)
    geom_in_geojson = FeatureCollection(points_in_lines_new)  # doctest: +ELLIPSIS
    with open(GEOJSON_PATH + 'one_car_traj_' + str(CAR_ID) + '.geojson', 'w') as outfile:
        geojson.dump(geom_in_geojson, outfile)
    print "write geojson done!",",",GEOJSON_PATH + 'one_car_traj_' + str(CAR_ID) + '.geojson'
    return points_in_lines



def reconnect_points_in_lines(points_in_lines, roadnet_data,roads, graph,date):
    routing_thres_reconnect = 60
    date_zero_oclock = int(time.mktime(datetime.datetime.strptime(date, "%Y_%m_%d").timetuple()))
    #print datetime.datetime.utcfromtimestamp(date_zero_oclock + 3600*8).strftime('%Y-%m-%d %H:%M:%S')
    points_in_lines_final = []
    if len(points_in_lines) == 0:
        return []



    cnt_pre = 0
    while points_in_lines[cnt_pre]["properties"]["ABS_TIME_PRE"] < date_zero_oclock and cnt_pre < len(points_in_lines) - 1:
        cnt_pre += 1

    if len(points_in_lines) - cnt_pre < 2:
        return []

    gid_pre = points_in_lines[cnt_pre]["properties"]["GID"]
    percent_pre = points_in_lines[cnt_pre]["properties"]["Percent"]
    time_pre_current = points_in_lines[cnt_pre]["properties"]["ABSTIME"]
    points_in_lines_final.append(points_in_lines[cnt_pre])

    for cnt in range(1 + cnt_pre,len(points_in_lines)):
        gid_current = points_in_lines[cnt]["properties"]["GID_pre"]
        percent_current = points_in_lines[cnt]["properties"]["Percent_pre"]
        time_current_pre = points_in_lines[cnt]["properties"]["ABS_TIME_PRE"]
        time_current_current = points_in_lines[cnt]["properties"]["ABSTIME"]
        pick_or_drop = points_in_lines[cnt]["properties"]["Pick_or_drop"]

        if time_current_pre - time_pre_current > 0 and time_current_pre - time_pre_current < 1800:
            #print "time_pre_current,time_current_pre",time_pre_current,time_current_pre
            points_in_line = []
            routing_points, dist = gen_points_from_routing(gid_current,gid_pre,percent_current,percent_pre,roadnet_data, roads, graph,routing_thres_reconnect)
            time_diff = time_current_pre - time_pre_current
            if dist < 40*time_diff:
                for item in routing_points:
                    points_in_line.append(tuple(item))
                time_string = datetime.datetime.utcfromtimestamp(time_current_pre + 3600*8).strftime('%Y-%m-%d %H:%M:%S')
                points_in_lines_final.append(Feature(geometry=LineString(points_in_line),
                                               properties={'ABSTIME':time_current_pre,'TIME':time_string,
                                                           'OSM_ID': int(roadnet_data[int(gid_current) - 1][16]), 'GID': gid_current,
                                                           'Percent':percent_current,'GID_pre': gid_pre,'Percent_pre':percent_pre,
                                                           'Pick_or_drop':pick_or_drop, "ABS_TIME_PRE":time_pre_current, "IS_FILL":1}))
                #print points_in_lines_final[-1]
            else:
                #print points_in_lines_final[-1]["geometry"]["coordinates"][-1]
                time_string = datetime.datetime.utcfromtimestamp(time_current_pre + 3600*8).strftime('%Y-%m-%d %H:%M:%S')
                points_in_lines_final.append(Feature(geometry=LineString([points_in_lines_final[-1]["geometry"]["coordinates"][-1]]),
                                               properties={'ABSTIME':time_current_pre,'TIME':time_string,
                                                           'OSM_ID': int(roadnet_data[int(gid_current) - 1][16]), 'GID': gid_current,
                                                           'Percent':percent_current,
                                                       'Pick_or_drop':pick_or_drop, "ABS_TIME_PRE":time_pre_current, "IS_FILL":1}))
        elif time_current_pre != time_current_pre:
            #print points_in_lines_final[-1]["geometry"]["coordinates"][-1]
            time_string = datetime.datetime.utcfromtimestamp(time_current_pre + 3600*8).strftime('%Y-%m-%d %H:%M:%S')
            points_in_lines_final.append(Feature(geometry=LineString([points_in_lines_final[-1]["geometry"]["coordinates"][-1]]),
                                               properties={'ABSTIME':time_current_pre,'TIME':time_string,
                                                           'OSM_ID': int(roadnet_data[int(gid_current) - 1][16]), 'GID': gid_current,
                                                           'Percent':percent_current,
                                                       'Pick_or_drop':pick_or_drop, "ABS_TIME_PRE":time_pre_current, "IS_FILL":1}))



        points_in_lines_final.append(points_in_lines[cnt])

        if time_current_pre == time_pre_current and cnt > 1 and len(points_in_lines[cnt]["geometry"]["coordinates"]) == 0:
            points_in_lines_final.pop()
            time_string = datetime.datetime.utcfromtimestamp(time_current_pre  + 3600*8).strftime('%Y-%m-%d %H:%M:%S')
            points_in_lines_final.append(Feature(geometry=LineString([points_in_lines_final[-1]["geometry"]["coordinates"][-1]]),
                                               properties={'ABSTIME':time_current_pre,'TIME':time_string,
                                                           'OSM_ID': int(roadnet_data[int(gid_current) - 1][16]), 'GID': gid_current,
                                                           'Percent':percent_current,
                                                       'Pick_or_drop':pick_or_drop, "ABS_TIME_PRE":time_pre_current, "IS_FILL":1}))

        gid_pre = points_in_lines[cnt]["properties"]["GID"]
        percent_pre = points_in_lines[cnt]["properties"]["Percent"]
        time_pre_current = time_current_current




    if len(points_in_lines_final) > 0:
        if points_in_lines_final[0]["properties"]["ABS_TIME_PRE"] > date_zero_oclock:
            time_string = datetime.datetime.utcfromtimestamp(points_in_lines_final[0]["properties"]["ABS_TIME_PRE"] + 3600*8).strftime('%Y-%m-%d %H:%M:%S')
            points_in_lines_final.insert(0,Feature(geometry=LineString([points_in_lines_final[0]["geometry"]["coordinates"][0]]),
                                                   properties={'ABSTIME':points_in_lines_final[0]["properties"]["ABS_TIME_PRE"],'TIME':time_string,
                                                               'OSM_ID': points_in_lines_final[0]["properties"]["OSM_ID"], 'GID': points_in_lines_final[0]["properties"]["GID"],
                                                               'Percent':points_in_lines_final[0]["properties"]["Percent"],
                                                           'Pick_or_drop':points_in_lines_final[0]["properties"]["Pick_or_drop"], "ABS_TIME_PRE":date_zero_oclock}))
    return points_in_lines_final




def gen_points_from_routing(gid_current, gid_pre, percent_current, percent_pre, roadnet_data,roads, graph, routing_thres):

    #mydata: mydata[gid - 1][2] length_m; mydata[gid - 1][3] source; mydata[gid - 1][4] target; mydata[gid - 1][13] one_way;
    #mydata[gid - 1][16] osm_id
    points = []
    percent_current_temp = percent_current
    #print "======================================="
    #print "gid_pre, gid_current,  percent_pre, percent_current"
    #print gid_pre, gid_current,  percent_pre, percent_current
    if gid_current == gid_pre:
        ##print "gid_current == gid_pre"
        points_routing,dist = append_points(gid_pre,gid_current,percent_pre,percent_current,roadnet_data, roads)
        #print "--------------------------"
        #print "gid_current == gid_pre",gid_current
        for point in points_routing:
            points.append(point)
        #print points_routing,dist
        return unique_rows(np.array(points)),dist
    else:
        #print "not equal gid"
        distance_m = 0
        try:
            shortest_path = nx.shortest_path(graph, source=str(gid_pre), target=str(gid_current), weight='weight')

        except:

            #print "no routing!","source=",str(gid_pre), "target=",str(gid_current)
            return [], 99999

        # judge the direction of car on the first and last road segments, ie. for gid_pre and gid_current

        if len(shortest_path) >= 2 and len(shortest_path) <= routing_thres:
            #print "--------------------------"
            #print "shortest_path",shortest_path
            connection_tag = 0
            for i in range(0,len(shortest_path) -1):
                connection_tag = find_cross_index(roadnet_data[int(shortest_path[i]) - 1][0],
                                                              roadnet_data[int(shortest_path[i+1]) - 1][0],roadnet_data)
                #print "connection_tag:",connection_tag
                percent_current = percent_pre
                if connection_tag == 1 or connection_tag == 2:
                    percent_next = 1
                    if i > 0:
                        percent_current = 0
                else:
                    percent_next = 0
                    if i > 0:
                        percent_current = 1
                points_routing, dist = append_points(roadnet_data[int(shortest_path[i]) - 1][0],
                                                              roadnet_data[int(shortest_path[i]) - 1][0],
                                                             percent_current,percent_next,roadnet_data,roads)
                #print "routing_points_step_by_step",points_routing,dist
                distance_m = distance_m + dist
                for point in points_routing:
                    points.append(point)


            # add last segment
            percent_next = percent_current_temp
            #print "final_connection_tag:",connection_tag
            if connection_tag == 2 or connection_tag == 3:
                percent_current = 1
            else:
                percent_current = 0
            points_routing, dist = append_points(roadnet_data[int(shortest_path[len(shortest_path) -1]) - 1][0],
                                                              roadnet_data[int(shortest_path[len(shortest_path) -1]) - 1][0],
                                                             percent_current,percent_next,roadnet_data,roads)
            distance_m = distance_m + dist
            for point in points_routing:
                points.append(point)
            #print "routing points",points, distance_m
            return unique_rows(np.array(points)),distance_m
        else:
            return [], 99999






def find_cross_index(gid_pre,gid_current,roadnet_data):
    #return the direction
    #direction 1: normal, -1: reverse
    if roadnet_data[int(gid_pre) - 1][4] == roadnet_data[int(gid_current) - 1][3]:
        ##print gid_pre,gid_current,"target -> source"
        return 1
    elif roadnet_data[int(gid_pre) - 1][4] == roadnet_data[int(gid_current) - 1][4]:
        ##print gid_pre,gid_current,"target -> target"
        return 2
    elif roadnet_data[int(gid_pre) - 1][3] == roadnet_data[int(gid_current) - 1][4]:
        ##print gid_pre,gid_current,"source -> target"
        return 3
    elif roadnet_data[int(gid_pre) - 1][3] == roadnet_data[int(gid_current) - 1][3]:
        ##print gid_pre,gid_current,"source -> source"
        return 4
    else:
        ##print "not connected"
        return 1




def append_points(gid_pre,gid_current,percent_pre,percent_current, roadnet_data, roads):
    points = []

    length_current = roadnet_data[int(gid_current) - 1][2]
    ##print "gid:",gid_current,"osm_id:",roadnet_data[int(gid_current) - 1][16]
    ##print "length:",length_current
    #length_pre = roadnet_data[int(gid_pre) - 1][3]
    index_current = int(len(roads[int(gid_current) - 1])*percent_current)
    index_pre = int(len(roads[int(gid_pre) - 1])*percent_pre)
    #print "gid_pre,gid_current,percent_pre, percent_current, index_pre, index_current,len(roads[int(gid_current) - 1])"
    #print gid_pre,gid_current,percent_pre, percent_current, index_pre, index_current,len(roads[int(gid_current) - 1])


    #append the coordinates within two indices
    ##print len(roads[int(gid_pre) - 1]),index_pre,index_current,((index_current - index_pre) >= 0 )

    if (index_current - index_pre) >= 0:
        ##print range(index_pre,min(len(roads[int(gid_pre) - 1]) - 1,index_current),1), "normal"
        points_temp = roads[int(gid_current) - 1][index_pre : min(len(roads[int(gid_pre) - 1]) - 1,index_current): 1]
        index_end = min(len(roads[int(gid_pre) - 1]) - 1,index_current)
    else:
        ##print range(min(len(roads[int(gid_pre) - 1]) - 1,index_current),-1), "reverse"
        points_temp = roads[int(gid_current) - 1][min(index_pre,len(roads[int(gid_pre) - 1]) - 1):index_current: -1]
        index_end = index_current

    ##print "index_end",index_end

    for point in points_temp:
        points.append(point)
    points.append(roads[int(gid_current) - 1][index_end])

    dist = length_current*abs(percent_current - percent_pre)

    return points, dist

def unique_rows_unique(a):
    if len(a) == 0:
        return []
    b = np.ascontiguousarray(a).view(np.dtype((np.void, a.dtype.itemsize * a.shape[1])))
    _, indices = np.unique(b, return_index=True)

    return a[sorted(indices)]


#remove adjacent duplicates
def unique_rows(a):
    if len(a) == 0:
        return []
    if a.shape[1] != 2:
        print "wrong coordinates"
    c = np.insert(a,0,[0,0],axis=0)
    b = np.diff(c.transpose()).transpose()
    indices = []
    for i in range(0,len(b)):
        if b[i][0] == 0 and b[i][1] == 0:
            continue
        indices.append(i)
    return a[indices,:]
