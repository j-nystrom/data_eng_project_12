
import os
import time

from sub_overlap_pipeline import *

def do_runtime_experiments():
    """Code for testing performance of the code for different number of cores used."""
    # Set up input parameters and variables
    subs_to_incl = 100
    comment_threshold = 1
    max_core_values = [1, 2, 4, 8, 16]
    # Run for loop with different number of cores
    for max_cores in max_core_values:
        run_pipeline(max_cores, subs_to_incl, comment_threshold)

def run_pipeline():
    """Run all the steps in the pipeline in sequence"""
    times = {}

    #TODO: Add time measurements for each part of the code, and save results
    start_time = time.perf_counter()

    #Step 1: Start spark session and load data
    spark_session = start_session()
    df_raw = load_data("sample_data.json", spark_session)
    
    end_time = time.perf_counter()
    times['start_session_and_load_data'] = end_time - start_time
    
    # Step 2: Filter out columns that will not be used
    start_time = time.perf_counter()
    df_reddit = filter_columns(df_raw)
    end_time =time.perf_counter()
    times['filter_columns'] = end_time - start_time

    # Step 3: Count the biggest subreddits and filter dataframe for those
    start_time = time.perf_counter()
    subs_to_incl = 100
    df_sub_filtered = filter_top_subreddits(df_reddit, subs_to_incl)

    end_time = time.perf_counter()
    times['filter_top_subreddits'] = end_time - start_time
    
    # Step 4: Get list with users with significant amount of posts and filter dataframe
    start_time = time.perf_counter()
    comment_threshold = 1
    df_user_filtered = filter_top_users(df_sub_filtered, comment_threshold)

    end_time = time.perf_counter()
    times['filter_top_users'] = end_time - start_time
    
    # Step 5: Generate all subreddit combinations for each user
    start_time = time.perf_counter()
    df_user_subs = create_user_subreddit_tuples(df_user_filtered)
    
    end_time = time.perf_counter()
    times['create_user_subreddit_tuples'] = end_time - start_time
    
    # Step 6: Count how many times each subreddit combination appears
    start_time = time.perf_counter()
    df_tuple_counts = count_tuples(df_user_subs)
    
    end_time = time.perf_counter()
    times['count_tuples'] = end_time - start_time
    
    # Step 7: Remove duplicates from data
    start_time = time.perf_counter()
    df_result = remove_duplicates(df_tuple_counts)
    
    end_time = time.perf_counter()
    times['remove_duplicates'] = end_time - start_time
    
    # TODO: Dump result dataframe into csv and put in folder on VM
    os.makedirs('data', exist_ok=True)  
    df_result.to_csv('data/reddit_result.csv')
    with open('data/times.csv', 'w') as f:
        for key in times.keys():
            f.write("%s,%s\n"%(key,times[key]))
    
    return df_result, times
    
    
    


if __name__ == "__main__":
    run_pipeline()
