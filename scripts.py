
import configparser
import pickle
import praw
import boto3
from datetime import datetime, timedelta

def get_dynamic_attr_val(obj_in, 
                         attr_name_in:str,
                         is_debug=False):

    try:
        # loop through attributes and sub attributes
        obj_val = obj_in

        for attr in attr_name_in.split('.'):
            obj_val = getattr(obj_val, attr)
    except:
        if is_debug:
            raise
        obj_val = ''
            
    return obj_val
    
# reddit_obj_type is a string value that is used to lookup a location in a config file
# options:
#       submission
#       comment
#       redditor
def get_reddit_obj_attr_data_dict(reddit_obj_in,
                                  reddit_obj_type:str,
                                  is_debug=False):
    """
    Dynamically allocates attributes for a dictionary object using the response from 
    praw.  This allows for easy adding of attributes for a specific reddit object type (post, comment, etc).
    This also allows for sub attributes to exist and be parsed easily using a lookup 

    Args:
        reddit_obj_in (reddit object): reddit object that has details for the type of object specified in reddit_obj_type
        reddit_obj_type (str): reddit object type; options implemented: ['submission','comment','redditor']
        is_debug (bool, optional): indicator of debugging or not. Defaults to False.

    Returns:
        dict: dictionary that has content of the dynamic reddit object
    """

    config = configparser.ConfigParser()
    config.read('config.ini')

    reddit_api_object_db_mapping_filename = config[
            'reddit_api_object_mappings_location'][reddit_obj_type]
    
    with open(reddit_api_object_db_mapping_filename, 'rb') as handle:
        mappings_dict = pickle.load(handle)

    reddit_obj_attr_vals_dict = {}
        
    for key, value in mappings_dict.items():
        
        db_field_key = key
        reddit_obj_attr = value
        
        reddit_obj_attr_vals_dict[db_field_key] = get_dynamic_attr_val(reddit_obj_in, 
                                                                reddit_obj_attr,
                                                                is_debug=is_debug)
    
    return reddit_obj_attr_vals_dict

def get_s3_client():

    config = configparser.ConfigParser()
    config.read('config.ini')

    s3_access_key = config['aws']['access_key_id']
    s3_secret_key = config['aws']['secret_access_key']

    s3_client = boto3.client('s3', aws_access_key_id=s3_access_key,
                      aws_secret_access_key=s3_secret_key)

    return s3_client

def get_s3_resource():

    config = configparser.ConfigParser()
    config.read('config.ini')

    s3_access_key = config['aws']['access_key_id']
    s3_secret_key = config['aws']['secret_access_key']

    s3_resource = boto3.resource('s3', aws_access_key_id=s3_access_key,
                      aws_secret_access_key=s3_secret_key)

    return s3_resource

def get_s3_bucket_name(bucket_name='original_load_bucket'):

    config = configparser.ConfigParser()
    config.read('config.ini')

    s3_bucket = config['aws'][bucket_name]

    return s3_bucket

def write_to_s3_bucket(file_obj,
                    filename:str,
                    bucket_name='original_load_bucket'):
    
    write_pickle = pickle.dumps(file_obj)

    s3_client = get_s3_client()
    s3_bucket = get_s3_bucket_name(bucket_name=bucket_name)
    s3_client.put_object(Body=write_pickle,
                        Bucket=s3_bucket,
                        Key=filename)

    return 'completed'

# bucket_name is driven by the config file
#   options include
#       original_load_bucket
#       n_days_old_bucket
def read_from_s3_bucket(s3_filename:str,
                        type_in='pickle',
                        bucket_name='original_load_bucket'):

    config = configparser.ConfigParser()
    config.read('config.ini')

    s3_access_key = config['aws']['access_key_id']
    s3_secret_key = config['aws']['secret_access_key']
    s3_bucket = config['aws'][bucket_name]

    s3_client = boto3.client('s3', aws_access_key_id=s3_access_key,
                        aws_secret_access_key=s3_secret_key)

    s3_obj = s3_client.get_object(Bucket=s3_bucket, Key=s3_filename) 

    body_string = s3_obj['Body'].read()

    if type_in=='pickle':
        file_obj = pickle.loads(body_string)

    return file_obj

def get_reddit_api_object():

    config = configparser.ConfigParser()
    config.read('config.ini')
    
    client_id = config['reddit_api']['client_id']
    client_secret = config['reddit_api']['client_secret']
    user_agent = config['reddit_api']['user_agent']
    
    reddit = praw.Reddit(client_id=client_id,
                    client_secret=client_secret,
                    user_agent=user_agent)
    return reddit

# credit to dangayle @
# https://gist.github.com/dangayle/4e6864300b58fee09ce1#file-subreddit_latest-py
class SubredditLatest(object):
    """Get all available submissions within a subreddit newer than x."""

    def __init__(self, subreddit, dt):

        # master list of all available submissions
        self.total_list = []

        # subreddit must be a string of the subreddit name (e.g., "soccer")
        self.subreddit = subreddit

        # dt must be a utc datetime object
        self.dt = dt

        # create reddit praw object
        self.reddit = get_reddit_api_object()

    def __call__(self):
        self.get_submissions(self)
        return self.total_list

    def get_submissions(self, paginate=False):
        """Get limit of subreddit submissions."""
        limit = 100  # Reddit maximum limit

        if paginate is True:
            # get limit of items past the last item in the total list
            submissions = self.reddit.subreddit(self.subreddit).new(
                limit=limit,
                params={"after": self.total_list[-1].fullname})
        else:
            submissions = self.reddit.subreddit(self.subreddit).new(limit=limit)

        submissions_list = [
            # iterate through the submissions generator object
            x for x in submissions
            # add item if item.created_utc is newer than an N hours ago
            if datetime.utcfromtimestamp(x.created_utc) >= self.dt
        ]
        self.total_list += submissions_list

        # if you've hit the limit, recursively run this function again to get
        # all of the available items
        if len(submissions_list) == limit:
            self.get_submissions(paginate=True)
        else:
            return



# loads a single submission with a submission_id
def load_submission_dict(submission_id):

    reddit = get_reddit_api_object()
    submission = reddit.submission(submission_id)
    submission_dict = get_reddit_obj_attr_data_dict(submission,
                                                  'submission',
                                                  is_debug=False)

    return submission_dict


# this function requires the submission_reference_id
def load_submissions_dict_arr(submission_reference_ids,
                        verbose_in=False):
    if verbose_in:
        print(f'# of submissions to load: {len(submission_reference_ids)}')

    reddit = get_reddit_api_object()

    updated_submission_dicts = []

    for submission in reddit.info(submission_reference_ids):
        updated_submission_dict = get_reddit_obj_attr_data_dict(submission,
                                                              'submission',
                                                              is_debug=False)

        updated_submission_dicts.append(updated_submission_dict)

    return updated_submission_dicts

def load_submissions_back_n_hours(n_hours_back: int,
                                  subreddit_name: str = 'wallstreetbets',
                                  verbose_in=False,
                                  is_debug=False):

    datetime_utc_back_to = datetime.utcnow() - timedelta(hours=n_hours_back)

    if verbose_in:
        print(f'querying {subreddit_name} for the last {n_hours_back} hours')

    submissions = SubredditLatest(subreddit_name, datetime_utc_back_to)()

    submission_dicts = []
    i = 0
    for submission in submissions:

        i += 1
        submission_dict = get_reddit_obj_attr_data_dict(submission,
                                                    'submission',
                                                    is_debug=is_debug)
        submission_dicts.append(submission_dict)

        if verbose_in and i % 100 == 0:
            print(f'done with {i} submissions')

    return submission_dicts


def get_historical_submission_dataset(n_days_time_lapsed_submissions:int,
                                        verbose_in=True):
    
    unique_submission_dicts = get_unique_historical_submission_dicts(
        days_back=n_days_time_lapsed_submissions,
        verbose_in=verbose_in)

    # identify each of the past N days submissions that need to have their current states loaded
    n_days_before_submissions = []
    current_utc_datetime = datetime.utcnow()
    current_utc_datetime_str = current_utc_datetime.strftime("%m/%d/%Y, %H:%M:%S")
    print(f'loading submissions created in the N days before {current_utc_datetime_str}')

    for submission_dict in unique_submission_dicts:

        created_utc_datetime = datetime.fromtimestamp(submission_dict['created_time_utc'])
        seconds_since_created = (current_utc_datetime-created_utc_datetime).total_seconds()
        hours_since_created = seconds_since_created/(60*60)
        days_since_created = seconds_since_created/(60*60*24)

        # only gather current post info for posts that are N days old
        # when hours_since_created%24 < 1 we have a post that is 24-25 hours old 
        # this expands with the same code to 48-49 hours and so on
        if hours_since_created%24 < 1 and hours_since_created >=24:
            
            submission_dict['created_utc_datetime'] = created_utc_datetime
            submission_dict['hours_since_created'] = hours_since_created
            submission_dict['days_since_created'] = days_since_created
            submission_dict['n_days'] = int(days_since_created)
            n_days_before_submissions.append(submission_dict)
            
    print(f'loading {len(n_days_before_submissions)} posts to get updated scores')    


    # load current submission states from reddit
    load_current_submission_status_reference_ids = [n_days_before_submission['submission_reference_id'] 
                                                    for n_days_before_submission in n_days_before_submissions]
    n_days = [n_days_before_submission['n_days'] 
                for n_days_before_submission in n_days_before_submissions]

    submissions_dicts = load_submissions_dict_arr(
        submission_reference_ids=load_current_submission_status_reference_ids,
        verbose_in=True)


    # add days_old field to each of the submissions
    # allows for all of these to be stored in the same bucket
    for i in range(len(n_days)):
        n=n_days[i]
        submission_dict = submissions_dicts[i]
        submission_dict.update({'days_old': n})

    return submissions_dicts
    

def get_unique_historical_submission_dicts(days_back:int,
                                        verbose_in=True):

    s3_resource = get_s3_resource()
    s3_bucket = get_s3_bucket_name()

    my_bucket = s3_resource.Bucket(s3_bucket)
    bucket_files = []

    get_files_back_to_date = datetime.today() - timedelta(days=days_back)

    # loop through all objects in th bucket and determine which ones 
    # are relevant based on the last modified date
    for my_bucket_object in my_bucket.objects.all():
        # only gather files created in the last {days_back} days
        if my_bucket_object.last_modified.replace(tzinfo = None) > get_files_back_to_date:
            bucket_files.append(my_bucket_object.key)

    submission_dict_arrs = []

    for bucket_file in bucket_files:
        s3_file_obj = read_from_s3_bucket(bucket_file)
        submission_dict_arrs.append(s3_file_obj)

    # load all file dictionary arrays into single large array
    # this array of dictionarys might have duplicate submissions in it
    all_loaded_submissions = []

    for submission_dict_arr in submission_dict_arrs:
        all_loaded_submissions = all_loaded_submissions + submission_dict_arr
        

    submission_dicts = []

    for submission_dict in all_loaded_submissions:
        
        submission_dicts.append({
            'submission_id': submission_dict['submission_id'],
            'submission_reference_id': submission_dict['submission_reference_id'],
            'created_time_utc': submission_dict['created_time_utc']
        })
        
    unique_submission_dicts = [dict(t) for t in {tuple(d.items()) for d in submission_dicts}]
        
    if verbose_in:
        print(f'loaded {len(unique_submission_dicts)} unique submission posts from S3')

    return unique_submission_dicts

























