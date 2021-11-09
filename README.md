# AWS_Lambda_reddit_historical_post_compare
This repository shows my code base for a data gathering system for reddit.  This system was designed to gather reddit submissions every hour and then gather the current state of the submissions every 24 hours after their original post date.  I envision this data can be used in the future as a leading indicator of social or market movements using anomaly detection concepts.  This system is running in AWS Lambda using Cloud Watch with data being stored in S3.  

Lambda functions are in run_me.py 
Scripts that do the heavy lifting of interacting with reddit and AWS S3

### Lambda function details
load_last_n_hours_and_save loads the last N hours of submissions from a subreddit and stores in an S3 bucket
load_time_lapsed_submissions loads the current state of submissions that are 24-25/48-49/72-73/etc hours beyond their original submission date time and stores in an S3 bucket