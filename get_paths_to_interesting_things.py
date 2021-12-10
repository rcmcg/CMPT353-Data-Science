import sys
import pandas as pd 
import numpy as np
from math import sqrt

def haversine(lat1, lon1, lat2, lon2):
    R = 6371000 # radius of Earth in meters
    
    phi1 = np.radians(lat1)
    phi2 = np.radians(lat2)
    delta_phi = np.radians(lat2) - np.radians(lat1)
    delta_lambda = np.radians(lon2) - np.radians(lon1)
    
    a = np.sin(delta_phi/2.0)**2 + np.cos(phi1)*np.cos(phi2)*(np.sin(delta_lambda/2.0)**2)
    result = R * 2 *np.arcsin(sqrt(a))
    
    return result;


def get_nearest_destination(data, list_index): 
    num = len(data)
    
    index = 0;
    min = 0;
    current = len(list_index)
    first_occurence = 0
    
    for i in range(num): 
        if i not in list_index:
            if first_occurence == 0: 
                min = haversine(data['lat'][current], data['lon'][current], data['lat'][i], data['lon'][i])
                first_occurence = first_occurence + 1
                index = i;
            else: # if min > distance -> update min
                distance = haversine(data['lat'][current], data['lon'][current], data['lat'][i], data['lon'][i])
                if min > distance:
                    index = i
                    min = distance
    
    return index

def main(in_directory):
    data = pd.read_csv(in_directory)

    thepath = data.drop(columns = ['count', 'wikidata_id'])
    paths = thepath.copy()
    # print(paths)
    
    list_index = []
    
    result = []
    
    total_travel_distance = 0
    
    for i in range(len(data)):
        #https://stackoverflow.com/questions/59406045/convert-pandas-series-into-a-row
        list_index.append(i)
        the_row = paths.loc[i]
        the_row = pd.DataFrame([the_row.tolist()])
        the_row = the_row.drop(columns = [0])

        the_row.columns = ['lat', 'lon', 'amenity', 'name']
        
        for j in range(2): # Assume the max destination a person can go in 1 day is 3
            newindex = get_nearest_destination(paths, list_index)
            list_index.append(newindex)
            
            total_travel_distance = total_travel_distance + haversine(data['lat'][i], data['lon'][i], data['lat'][newindex], data['lon'][newindex])
            
            if j == 0:
                the_row['lat2'] = paths['lat'][newindex]
                the_row['lon2'] = paths['lon'][newindex]
                the_row['amenity2'] = paths['amenity'][newindex]
                the_row['name2'] = paths['name'][newindex]
            elif j == 1: 
                the_row['lat3'] = paths['lat'][newindex]
                the_row['lon3'] = paths['lon'][newindex]
                the_row['amenity3'] = paths['amenity'][newindex]
                the_row['name3'] = paths['name'][newindex]
                
        the_row['total_travel_distance'] = total_travel_distance    
                       
        if i == 0:
            # print(the_row)
            result = the_row
        else: 
            # if i == 2: 
            #     print(result)
            result = result.append(the_row)
        
        list_index = []
        
        total_travel_distance = 0
        
        # print(i)
    
    # print(result)
    # Write to csv 
    result.to_csv("paths.csv")    


if __name__=='__main__':
    in_directory = sys.argv[1]
    main(in_directory)
