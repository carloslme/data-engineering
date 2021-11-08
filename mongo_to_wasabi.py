#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""
This script reads data from Mongo and send it to S3 bucket with endpoint.
"""

import time
import datetime

import boto3
import pandas as pd
import pymongo

# Create connection to Mongo iVoy
client_ivoy = pymongo.MongoClient(
    "mongodb+srv://user:pass@collection.nzjor.mongodb.net/database?retryWrites=true&w=majority"
)
database = client_ivoy["database"]
collection = database["collection"]

start_date = datetime.datetime(2021, 11, 3, 12, 00, 00)
end_date = datetime.datetime(2021, 11, 5, 12, 00, 00)

start = time.time()

# Add filters to the query and get data
response = collection.find(
    {"createdAt": {"$gte": start_date, "$lt": end_date}}, limit=1000000
)

# Instead of iterate the response (cursor), just convert it to list
mongo_docs = list(response)

# Normalize data dropping mongo metadata
df_normalized = pd.json_normalize(data=mongo_docs)

# Setup Wasabi info
BUCKET = "bucket_name"
ACCESS_ID = ""
ACCESS_KEY = ""

s3_wasabi = boto3.client(
    "s3",
    endpoint_url="https://s3.us-east-1.wasabisys.com",
    aws_access_key_id=ACCESS_ID,
    aws_secret_access_key=ACCESS_KEY,
)

# Send CSV file to bucket in Wasabi
s3_wasabi.put_object(
    Bucket=BUCKET,
    Key="bucket_destination/name_file.csv",
    Body=df_normalized.to_csv(None, index=False),
)

del mongo_docs
del df_normalized

end = time.time()
print("Took {} seconds".format(end - start))
