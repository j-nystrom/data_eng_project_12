
import os
import time

from sub_overlap_pipeline import *


def run_pipeline():
    """Run all the steps in the pipeline in sequence"""
	times = []
	
    # TODO: Add time measurements for each part of the code, and save results
	start_time = time.time()
	
    # Step 1: Start spark session and load data
    spark_session = start_session()
    df_raw = load_data("sample_data.json", spark_session)
    
    end_time = time.time() - start_time
    times.append(end_time)
    
    # Step 2: Filter out columns that will not be used
    start_time = time.time()
    df_reddit = filter_columns(df_raw)
	end_time = time.time() - start_time
    times.append(end_time)
	
    # Step 3: Count the biggest subreddits and filter dataframe for those
    start_time = time.time()
    subs_to_incl = 100
    df_sub_filtered = filter_top_subreddits(df_reddit, subs_to_incl)
	
	end_time = time.time() - start_time
    times.append(end_time)
	
    # Step 4: Get list with users with significant amount of posts and filter dataframe
    start_time = time.time()
    comment_threshold = 1
    df_user_filtered = filter_top_users(df_sub_filtered, comment_threshold)
	
	end_time = time.time() - start_time
    times.append(end_time)
    
    # Step 5: Generate all subreddit combinations for each user
    start_time = time.time()
    df_user_subs = create_user_subreddit_tuples(df_user_filtered)
	
	end_time = time.time() - start_time
    times.append(end_time)
    
    # Step 6: Count how many times each subreddit combination appears
    start_time = time.time()
    df_tuple_counts = count_tuples(df_user_subs)
	
	end_time = time.time() - start_time
    times.append(end_time)
    
    # Step 7: Remove duplicates from data
    start_time = time.time()
    df_result = remove_duplicates(df_tuple_counts)
	
	end_time = time.time() - start_time
    times.append(end_time)
    
    # TODO: Dump result dataframe into csv and put in folder on VM
    os.makedirs('data', exist_ok=True)  
	df_result.to_csv('data/reddit_result.csv')
	
	print(times)
	
    return df_result, times


if __name__ == "__main__":
    run_pipeline()
