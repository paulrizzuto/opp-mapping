# Dependencies
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import requests
from requests_toolbelt.threaded import pool
import time 
import urllib.parse
from multiprocessing.pool import ThreadPool
from time import time as timer
import csv

# Disable warning
pd.options.mode.chained_assignment = None 

# GoogleAPI Key
gkey = "AIzaSyAewc9-KdMydSRQd9lT0g9e9HpeOGnNVpI"

# Departure Time 5:00 PM Dec 14, 2017 (needs to be set in the future)
departure_time = 1527811200

# Read the Complete Dataset
print("Now reading csv...")
city_data = pd.read_csv("testing_distance.csv") 
#city_data = city_data.sort_values("Population", ascending=False)

# Starting Index / Ending Index
batch_no = 1
start_index = 0
end_index = 1000
increment = 1001
total_rows = len(city_data)

# Batch the Calls
while (end_index < total_rows):

    # Create a subset for testing
    print("-----------------------------------")
    print("Now creating subset # " + str(batch_no) + "...")
    subset = city_data[start_index:end_index]
    #del subset["Commute Time (Public Transport)"]
    #del subset["Commute Time (Solo)"]

    # Iterate through and store the URLs
    print("Now assembling URLs...")
    for index, row in subset.iterrows():
            
        # Create endpoint URL
        target_url = "https://maps.googleapis.com/maps/api/distancematrix/json?units=imperial&departure_time=%s&origins=%s,%s&destinations=%s,%s&key=%s" %(departure_time, row["City Lat"], row["City Lng"], row["Uni Lat"], row["Uni Lng"],gkey)
        subset.set_value(index, "Google_URL", target_url)

    # Setup Timer
    start = timer()

    # Initiate Pooling
    print("Now starting Timer...")
    p = pool.Pool.from_urls(subset["Google_URL"].values, num_processes=10)
    p.join_all()

    # Variables to hold results
    traffic_patterns = []
    row_count = 0

    # Loop through pooled results
    for response in p.responses():
        row_count = row_count + 1
        print("Now storing City #: " + str(row_count))
        print(response.request_kwargs['url'])

        # Store the retrieved data into list of dictionaries
        try:
            data = response.json()
            traffic =  {"Google_URL": response.request_kwargs['url'],
                        "Distance": data["rows"][0]["elements"][0]["distance"]["value"],
                        "Distance_Text": data["rows"][0]["elements"][0]["distance"]["text"],
                        "Time": data["rows"][0]["elements"][0]["duration_in_traffic"]["value"],
                        "Time_Text": data["rows"][0]["elements"][0]["duration_in_traffic"]["text"]}

            traffic_patterns.append(traffic)

        except Exception as e:
            print(e)
            print("Missing Data... Skipping")

    print("------------------------------------------")
    print("Requests complete. Elapsed Time: %s" % (timer() - start,))

    # Convert traffic_patterns to DataFrame
    traffic_pd = pd.DataFrame(traffic_patterns)

    # Merge the Traffic_Pattern data to the original data
    full_data = subset.merge(traffic_pd, on="Google_URL", how="outer") 

    # Export CSV
    print("Now saving...")
    full_data.to_excel("Traffic Data/File" + str(batch_no) + ".xlsx", index=False)

    # Add to the start index, end index, and batch number
    start_index = start_index + increment 
    end_index = end_index + increment
    print("start_index: " + str(start_index))
    print("end_index: " + str(end_index))
    print("total_rows: " + str(total_rows))
    batch_no = batch_no + 1

    # If we're approaching the last set of rows, modify the increment accordingly
    if (end_index + increment > total_rows):
        increment = total_rows - end_index