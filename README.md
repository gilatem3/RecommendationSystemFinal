# **Big Data Algorithms Fall 2018 Final Project**

## Create a Recommendation System using AWS & algorithms demonstrated in class.
*By Harshinder Kohli, Phuong Le, Gilat Mandelbaum, Sakina Presswala*

**Phase 1**
* Uploaded “Events data (events.json.gz) and “DMA data (DMA-zip.csv.gz) to AWS S3 bucket 
* Created a AWS Glue crawler to read the events json file from S3 and converted it to csv format, which again went to S3
* Created a Glue job to transform csv data to parquet files which was stored in S3 
* Made changes to the proposed PySpark script generated by AWS Glue to change dates to ISO 8601 UTC datetime format (YYYY-mm-ddTHH:MM:SSZ) and remove extraneous characters (embedded newlines, non-renderable UTF-8 symbols, etc)
* Created a new table for the parquet data using glue crawler 
* Viewed the cleaned data using Athena 

**Phase 2**
* Part 1: 
    * Create lambda code to read Athena SQL queries and input data into DynamoDB. Lambda code invoked by uploading file to a specified S3 bucket. The code was provided by Mr. Young. We altered the script by taking care of missing values and replacing them with "unknown" so they can be entered into DynamoDB properly. To invoke this code we uploaded a file into s3.
    * A challenge we had was when we ran this code was our credits would quickly be used up. We only uploaded a few instances into DynamoDB so we wouldn't run out of credits.
* Part2: 
    * Create lambda function to read DynamoDB data based on API Gateway Invoked link event (GET event/{id}) where {id} is replaced with an event_id. This Lambda code was also from Mr. Young. The alteration we made was turning the list of recommended event id's from string to Python list type.
    * In API Gateway, create a new API, then create new Resource “event” and another resource “{id}”, select PROXY option.
Create a new method under “{id}” resource “GET”: select lambda_proxy option and choose lambda function created. 
    * Select OPTION under “{id}”, then Integration Request > Mapping Templates > Create a new template with JSON format: {"id": "$input.params('id')" }. 
    * Deploy API and use the event’s Invoked URL and add the event_id (i.e “/event/1” if 1 is event_id). Output will be the even_id’s information.
* Serverless host:
    * Javascript file provided by Mr. Young where it utilizes the API Gateway Invoke URL and along with the index.html and styles.css file, also provided by Mr. Young, is able to read the output into an HTML format.
    * Create S3 bucket to upload javascript,html, and css files.
    * Change bucket policy to allow access, and then set as serverless host.

**Phase 3**
* Merged DMA data to the Events data by modifying and adding code to the PySpark script / job from phase 1.
* Ran the glue crawler to get the new table containing the combined data 

**Phase 4**
* Merged data of “DMA-zip” with data of “Events.jason” based on the zip-code. Filling missing values with "Unknown". There are some duplicates in the event name so I drop duplicates across the event name and state except for the first occurrence
* Created a new column called “info” which contains event description; event name; organization name; state; dma code within each record. These are the basic details of each event.
* Applied TfidfVectorizer function to transforms text of ”info” to feature vectors that can be used as input to estimator. Number of features: 200894 for each of 73545 events
* Computed the SVDs and convert data to latent space. The sparse algorithm (sparse.linalg.svds) works fine for sparse matrices. K as the number of singular values is 100
* Built a function that computes a set of 5 most similar events to the requested event. The function essentially looks at the event’s matrix (73545, 100) in that latent feature space. The distance metric will be cosine distance in latent space.
For any requested event name, the function returns 5 recommended events in the same geographic area with basic details such as the event name, description, venue name and address. The input can be event name or event id.

* Created another chart with the event ids and a list with their associated event ids, then joined them to the data like in Phase 3.

**Phase 5**
We tried implementing date logic to filter out the events that had already passed but haven’t succeeded yet.

**Future work**:
* Improve our recommendation engine by trying out different ML algorithms such as clustering, KNN and Neural Networks 
* Use other approaches such as “Collaborative filtering” and “Hybrid filtering” based on availability of data . 

