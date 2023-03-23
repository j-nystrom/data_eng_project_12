
import time

from sub_overlap_pipeline import *


def run_pipeline(max_cores, comment_threshold, sample_fraction):
    """Run all the steps in the pipeline in sequence"""

    # Spark cluster and HDFS settings
    master_address = "spark://spark-master:7077"
    app_name = "group_12_app"
    hdfs_path = "hdfs://spark-master:9000/user/root/RC_2009-12.json"

    # Step 1: Start spark session and load data
    spark_session = start_session(master_address, app_name, max_cores)
    df_raw = load_data(hdfs_path, spark_session)

    # Step 2: Filter out columns that will not be used
    df_reddit = filter_columns_and_sample(df_raw, sample_fraction)

    # Step 3: Count the biggest subreddits and filter dataframe for those
    df_subred_count, df_sub_filtered = count_subreddits(df_reddit)

    # Step 4: Get list with users with significant amount of posts and filter dataframe
    df_user_filtered = filter_top_users(df_sub_filtered, comment_threshold)

    # Step 5: Generate all subreddit combinations for each user
    df_user_subs = create_user_subreddit_tuples(df_user_filtered)

    # Step 6: Count how many times each subreddit combination appears
    df_tuple_counts = count_tuples(df_user_subs)

    # Step 7: Remove duplicates from data
    df_no_dupes = remove_duplicates(df_tuple_counts)

    # Step 8: Join dataframes with counts together
    df_result = join_count_data(df_subred_count, df_no_dupes)

    return df_result, spark_session
