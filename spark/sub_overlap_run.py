
from sub_overlap_pipeline import *


def run_pipeline():
    """Run all the steps in the pipeline in sequence"""

    # TODO: Add time measurements for each part of the code, and save results

    # Step 1: Start spark session and load data
    spark_session = start_session()
    df_raw = load_data("sample_data.json", spark_session)

    # Step 2: Filter out columns that will not be used
    df_reddit = filter_columns(df_raw)

    # Step 3: Count the biggest subreddits and filter dataframe for those
    subs_to_incl = 100
    df_sub_filtered = filter_top_subreddits(df_reddit, subs_to_incl)

    # Step 4: Get list with users with significant amount of posts and filter dataframe
    comment_threshold = 1
    df_user_filtered = filter_top_users(df_sub_filtered, comment_threshold)

    # Step 5: Generate all subreddit combinations for each user
    df_user_subs = create_user_subreddit_tuples(df_user_filtered)

    # Step 6: Count how many times each subreddit combination appears
    df_tuple_counts = count_tuples(df_user_subs)

    # Step 7: Remove duplicates from data
    df_result = remove_duplicates(df_tuple_counts)

    # TODO: Dump result dataframe into csv and put in folder on VM
    return df_result


if __name__ == "__main__":
    run_pipeline()
