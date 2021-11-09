
from datetime import datetime
import scripts
import configparser
import json

def load_last_n_hours_and_save(event, context):
    """
    Loads the last N hours of submissions from a subreddit and stores in an S3 bucket

    Args:
        event (json): Lambda function event 
        context (Context): Lambda run context 

    Returns:
        json: json object with statusCode and body 
    """

    config = configparser.ConfigParser()
    config.read('config.ini')

    hour_interval=config['app_settings']['hour_interval']
    subreddit=config['app_settings']['subreddit']
    verbose=bool(config['app_settings']['verbose'])
    debug=bool(config['app_settings']['debug'])

    # add 1 to ensure overlap 
    # will fix in data cleanup and next steps
    n_hours_back = int(hour_interval)+1
    
    current_datetime = datetime.utcnow()

    submissions_dicts = scripts.load_submissions_back_n_hours(n_hours_back=n_hours_back,
                                                    subreddit_name=subreddit,
                                                    verbose_in=verbose,
                                                    is_debug=debug)


    datetime_str = current_datetime.strftime("%m_%d_%Y_%H_%M_%S")
    filename = f'{datetime_str}.pickle'

    scripts.write_to_s3_bucket(file_obj=submissions_dicts, 
                                filename=filename,
                                bucket_name='original_load_bucket')
    return {
        'statusCode': 200,
        'body': json.dumps(f'successfully ran load_last_n_hours_and_save')
    }

def load_time_lapsed_submissions(event, context):
    """
    Loads the current state of submissions that are 24-25/48-49/72-73/etc hours beyond their
    original submission date time and stores in an S3 bucket

    Args:
        event (json): Lambda function event 
        context (Context): Lambda run context 

    Returns:
        json: json object with statusCode and body 
    """

    config = configparser.ConfigParser()
    config.read('config.ini')
    
    verbose=bool(config['app_settings']['verbose'])
    debug=bool(config['app_settings']['debug'])
    n_days_time_lapsed_submissions=int(config['app_settings']['n_days_time_lapsed_submissions'])

    if verbose:
        print(f'loading unique submissions from the last {n_days_time_lapsed_submissions} days')

    current_datetime = datetime.utcnow()

    submissions_dicts = scripts.get_historical_submission_dataset(n_days_time_lapsed_submissions=n_days_time_lapsed_submissions,
                                                                verbose_in=verbose)

    print(f'write out to s3')
    # write out to s3
    datetime_str = current_datetime.strftime("%m_%d_%Y_%H_%M_%S")
    filename = f'{datetime_str}.pickle'

    scripts.write_to_s3_bucket(file_obj=submissions_dicts,
                            filename=filename,
                            bucket_name='n_days_old_bucket')

    return {
        'statusCode': 200,
        'body': json.dumps(f'successfully ran load_n_days_old_submissions')
    }










