''' New Brunswick Group - Recommendation System Project - 02/08/2019

Task: This Glue Job script will combine the DMA data with the event data along with the list of associated event
IDs created from Phase 4.

'''


import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
 

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
 
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# catalog: database and table names
db_name = "events-rec-system"
tbl_events = "events_parquet"
tbl_DMA = "dma_zip_csv_gz"
tbl_recs = "gmphase4_first1000_csv"

# output s3 and temp directories
output_dir = "s3://events-data-only/dma-parquet"

# Create dynamic frames from the source tables 
events = glueContext.create_dynamic_frame.from_catalog(database=db_name, table_name=tbl_events)
DMA = glueContext.create_dynamic_frame.from_catalog(database=db_name, table_name=tbl_DMA)
recs = glueContext.create_dynamic_frame.from_catalog(database=db_name, table_name=tbl_recs)

# Change column names for easier identification
newrecs = RenameField(recs, "col0", "event_id")
newrecs2 = RenameField(newrecs, "col1", "recommendations")

# Join the frames to create merged_data2
merged_data = Join.apply(events,DMA, 'venue_zip', 'zipcode')
merged_data2 = Join.apply(merged_data,newrecs2, 'event_id')

# Write out the dynamic frame into parquet in "merged table" directory
glueContext.write_dynamic_frame.from_options(frame = merged_data2, connection_type = "s3", connection_options = {"path": output_dir}, format = "parquet")

job.commit()
