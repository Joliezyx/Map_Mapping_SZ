__author__ = 'yzhu7, Julie the big circle'

import networkx as nx
import numpy as np

def cal_length_m_diff(gid1, gid2, one_way1, one_way2, percent1, percent2, length1, length2, graph, roadnet_data):
    # gid1: current gid? gid2: previous gid
    #print gid2, "--to--", gid1
    #print percent2, "--percent--", percent1
    #print length2, "--length--", length1
    if gid1 == gid2:
        return length1*abs(percent1 - percent2), [str(gid1)]
    else:
        bypath_delta = 0
        try:
            shortest_path = nx.shortest_path(graph, source=str(gid2), target=str(gid1))
            #print shortest_path
        except:
            return -1, 0

        if len(shortest_path) > 2:
            for i in range(1,len(shortest_path) - 1):
                bypath_delta = bypath_delta + roadnet_data[int(shortest_path[i]) - 1][2]

        if one_way1 == 1 and one_way2 == 1:
            return (1 - percent1) * length1 + percent2 * length2 + bypath_delta, shortest_path
        elif one_way1 == 1 and one_way2 == 2:
            return (1 - percent1) * length1 + min(percent2,1-percent2) * length2 + bypath_delta, shortest_path
        elif one_way1 == 2 and one_way2 == 1:
            return min(percent1, (1 - percent1)) * length1 + percent2 * length2 + bypath_delta, shortest_path
        else:
            return min(percent1, (1 - percent1)) * length1 + min(percent2,1-percent2) * length2 + bypath_delta, shortest_path



def clean_data_for_one_car(car_data, graph, roadnet_data):
    # car_data: time, car_id, gid, percent
    road_matches = []

    #print "number of records of car is:", len(car_data)
    if len(car_data) > 2:
        firstrow = car_data[0]
        secondrow = car_data[1]

        #add an assigned gid @@
        gid = secondrow[2]

        # mark previous record
        cnt = 2
        while cnt < len(car_data):
            thirdrow = car_data[cnt]
            #print "count:", cnt, "original_gid:",gid
            tag, res, gid = clean_for_record(firstrow, secondrow, thirdrow, gid, graph,roadnet_data)
            #print "count:", cnt, "gid:",gid
            if tag ==  1:
                road_matches.append(res)
                firstrow = secondrow
                secondrow = thirdrow
                cnt = cnt + 1

            elif tag == 0:
                road_matches.append(res)
                firstrow = thirdrow
                if cnt < len(car_data) - 1:
                    secondrow = car_data[cnt + 1]
                else:
                    break
                cnt = cnt + 2
            elif tag == 2:
                #print "not routable!"
                if cnt > 0:
                    firstrow = car_data[cnt - 1]
                    gid = car_data[cnt - 1][0]
                    #print "gid_pre:",gid
                    tag, res, gid = clean_for_record(firstrow, secondrow, thirdrow, gid, graph,roadnet_data)
                    #print "new_case_tag:", tag
                    if tag == 0:
                        road_matches.pop()
                        road_matches.append(res)
                        firstrow = thirdrow
                        if cnt < len(car_data) - 1:
                            secondrow = car_data[cnt + 1]
                            cnt = cnt + 2
                        else:
                            break


                    elif tag == 1:
                        road_matches.pop()
                        road_matches.append(res)
                        firstrow = secondrow
                        secondrow = thirdrow
                        cnt = cnt + 1

                    else:
                        if cnt < len(car_data) - 2:
                            firstrow = car_data[cnt + 1]
                            secondrow = car_data[cnt + 2]
                            gid = secondrow[2]
                            cnt = cnt + 3
                        else:
                            break

    #write the result
    #print "length_of_result", len(road_matches)
    return road_matches

def clean_for_record(firstrow, secondrow, thirdrow, gid, gr, data):
    firstrow = firstrow.tolist()
    secondrow = secondrow.tolist()
    thirdrow = thirdrow.tolist()

    try:
        shortest_path2 = nx.shortest_path(gr, source=str(gid), target=str(secondrow[2]))
    except:
        shortest_path2 = []
    try:
        shortest_path3 = nx.shortest_path(gr, source=str(gid), target=str(thirdrow[2]))
    except:
        shortest_path3 = []

    # if routable
    routing_thresh = 30
    if len(shortest_path2) == 0:
        routing2 = 9999
    else:
        routing2 = len(shortest_path2)

    if len(shortest_path3) == 0:
        routing3 = 9999
    else:
        routing3 = len(shortest_path3)

    # print secondrow[headers[3]]
    # over n minutes interval
    if (firstrow[0] + 60*5 <  secondrow[0]): # > n=5min
        #print "over 5 minutes interval"
        return 2, None, gid

    #routable
    elif  routing2 <= routing_thresh or routing3 <= routing_thresh:
        first_osm_id = data[int(firstrow[2]) - 1][16]
        second_osm_id = data[int(secondrow[2]) - 1][16]
        third_osm_id = data[int(thirdrow[2]) - 1][16]



        if first_osm_id ==  third_osm_id:
            #print "situation 1: 3 points along a line"
            if firstrow[2] ==  secondrow[2] and secondrow[2] ==  thirdrow[2]:
                if (firstrow[3] <  secondrow[3] and secondrow[3] < thirdrow[3]) or (firstrow[3] >  secondrow[3] and secondrow[3] > thirdrow[3]) :
                    return 1, secondrow, secondrow[2]
                else:
                    return 0, thirdrow, thirdrow[2]
            else:
                return 0, thirdrow, thirdrow[2]

        #situation 2, 1 connect with 2
        elif first_osm_id == second_osm_id and second_osm_id != third_osm_id:
            # gr.has_edge(str(firstrow[2]),str(secondrow[2])):
            #print "situation 2: 1 and 2 with same osm_id"
            if routing2 <= routing_thresh:
                return 1, secondrow, secondrow[2]
            else:
                return 0, thirdrow, thirdrow[2]

        #situation 3: 2 connect with 3
        elif first_osm_id != second_osm_id and second_osm_id == third_osm_id:
            #print "situation 3: 2 and 3 with same osmid"
            return 1, secondrow, secondrow[2]

        #situation 4: three different segments
        else:
            #print "situation 4: three road segments"
            if gr.has_edge(str(firstrow[2]),str(secondrow[2])) and gr.has_edge(str(secondrow[2]),str(thirdrow[2])):
                return 1, secondrow, secondrow[2]
            #elif routing > routing_thresh and routing3 <= routing_thresh:
                #return 4, res, secondrow[headers[8]]
            elif routing2 <= routing3:
                return 1, secondrow, secondrow[2]
            else:
                return 0, thirdrow, thirdrow[2]

    #not routable
    else:
        return 2,None,gid

def clean_data_1car_from_candidates(car_data,row_count, graph):
    car_data = sorted(car_data)

    road_matches = []
    if row_count > 2:
        firstrow = car_data[0][5:7]
        secondrow = car_data[1][5:7]
        cnt = 2
        while cnt < row_count:
            thirdrow = car_data[cnt][5:7]
            tag, res = get_target_gid(firstrow, secondrow, thirdrow, graph)
            #print tag, res
            #print raw_info[cnt-1].tolist(), res
            if tag ==  1:
                road_matches.append((car_data[cnt-1][0:5] + res))
                firstrow = secondrow
                secondrow = thirdrow
                cnt += 1
            elif tag == 0:
                road_matches.append((car_data[cnt-1][0:5] + res))
                firstrow = thirdrow
                if cnt < len(car_data) - 1:
                    secondrow = car_data[cnt + 1][5:7]
                else:
                    break
                cnt += 2
            elif tag == 2:
                #print "not routable!"
                if cnt < row_count - 2:
                    firstrow = car_data[cnt + 1][5:7]
                    secondrow =car_data[cnt + 2][5:7]
                    cnt += 3
                else:
                    break
    #write the result
    print "length_of_result", len(road_matches)
    return road_matches


def get_target_gid(firstrow, secondrow, thirdrow, gr):

    firstrow = [firstrow]
    secondrow = [secondrow]
    thirdrow = [thirdrow]
    #print firstrow, secondrow, thirdrow

    max_candidates = 1
    routing_thres = 10

    length1 = min(len(firstrow),max_candidates)
    length2 = min(len(secondrow),max_candidates)
    length3 = min(len(thirdrow),max_candidates)

    max_dist_routing = 9999
    indices = [0,0,0] #i,j,k indices


    # find the shortest distance by routing
    for i in range(0,length1):
        for j in range(0,length2):
            for k in range(0,length3):
                #"INFO(GID,LONG,LAT,Percent)--top 5 ordered by distance"
                gid1 = int(firstrow[i][0])
                gid2 = int(secondrow[j][0])
                gid3 = int(thirdrow[k][0])

                try:
                    shortest_path12 = nx.shortest_path(gr, source=str(gid1), target=str(gid2))
                except:
                    continue

                if len(shortest_path12) > max_dist_routing:
                    continue
                try:
                    shortest_path23 = nx.shortest_path(gr, source=str(gid2), target=str(gid3))
                except:
                    continue



                if len(shortest_path12) + len(shortest_path23) < max_dist_routing:
                    max_dist_routing = len(shortest_path12) + len(shortest_path23)

                    indices[0] = i
                    indices[1] = j
                    indices[2] = k

    try:
        shortest_path13 = nx.shortest_path(gr, source=str(firstrow[indices[0]][0]), target=str(thirdrow[indices[2]][0]))
    except:
        shortest_path13 = [0]*routing_thres

    #print indices
    if max_dist_routing < routing_thres:
        percent1 = float(firstrow[indices[0]][1])
        percent2 = float(secondrow[indices[1]][1])
        percent3 = float(thirdrow[indices[2]][1])
        if (((percent1 - percent2) > 0) != ((percent2 - percent3) > 0) and firstrow[indices[0]][0] == secondrow[indices[1]][0]
                and secondrow[indices[1]][0] == thirdrow[indices[2]][0]) or firstrow[indices[0]][0] == thirdrow[indices[2]][0] or \
                    shortest_path13 < max_candidates:
            return 0, [int(thirdrow[indices[2]][0]),percent3]
        else:
            return 1, [int(secondrow[indices[1]][0]),percent2]
            #print 0,firstrow,secondrow,thirdrow

    else:
        return 2,[0,0]




