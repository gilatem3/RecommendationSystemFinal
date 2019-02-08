'''New Brunswick Group - Recommendation System Project - 02/08/2019

Task: Clean raw event data with this Glue Job.'''

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from datetime import datetime
## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "rec-system-bda", table_name = "json_folder", transformation_ctx = "datasource0")

def convert_dates(rec):
    '''Convert raw data's dates to UTC format and 
    removed extraneous characters from event description and name.'''
    rec["event_end_utc"] = datetime.strptime(rec["event_end_utc"], "%d %b %Y %I:%M:%S").isoformat()
    rec["event_start_utc"] = datetime.strptime(rec["event_start_utc"], "%d %b %Y %I:%M:%S").isoformat()
    rec["event_description"] = [i.decode('utf-8','ignore').encode("utf-8") for i in rec["event_description"]]
    rec["event_name"] = [i.decode('utf-8','ignore').encode("utf-8") for i in rec["event_name"]]
    rec["event_description"] = ''.join(rec["event_description"])
    rec["event_name"] = ''.join(rec["event_name"])
    
    return rec

custommapping1 = Map.apply(frame = datasource0, f = convert_dates, transformation_ctx = "custommapping1")

applymapping1 = ApplyMapping.apply(frame = custommapping1, mappings = [("event_description", "string", "event_description", "string"), ("event_end_utc", "timestamp", "event_end_utc", "timestamp"), ("event_id", "string", "event_id", "string"), ("event_name", "string", "event_name", "string"), ("event_start_utc", "timestamp", "event_start_utc", "timestamp"), ("facebook_event_id", "string", "facebook_event_id", "string"), ("hashtag", "string", "hashtag", "string"), ("organization_id", "string", "organization_id", "string"), ("organization_name", "string", "organization_name", "string"), ("tags", "array", "tags", "array"), ("venue_city", "string", "venue_city", "string"), ("venue_name", "string", "venue_name", "string"), ("venue_state", "string", "venue_state", "string"), ("venue_street", "string", "venue_street", "string"), ("venue_timezone", "string", "venue_timezone", "string"), ("venue_zip", "string", "venue_zip", "string")], transformation_ctx = "applymapping1")

resolvechoice2 = ResolveChoice.apply(frame = applymapping1, choice = "make_struct", transformation_ctx = "resolvechoice2")

dropnullfields3 = DropNullFields.apply(frame = resolvechoice2, transformation_ctx = "dropnullfields3")

datasink4 = glueContext.write_dynamic_frame.from_options(frame = dropnullfields3, connection_type = "s3", connection_options = {"path": "s3://parquet-folder2/parquet2"}, format = "parquet", transformation_ctx = "datasink4")

job.commit()
