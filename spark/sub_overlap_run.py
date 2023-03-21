
import os
import time

from sub_overlap_pipeline import *


def do_runtime_experiments():
    """Code for testing performance of the code for different number of cores used."""

    # Create directory for saving output
    directory = f"data/{time.strftime('%Y-%m-%d %H:%M')}/"
    os.makedirs(directory, exist_ok=True)

    # Set up input parameters and variables
    sample_fraction = 1
    subs_to_incl = 100
    comment_threshold = 1
    max_core_values = [1, 2]  # [1, 2, 4, 8, 16]

    # Run for loop with different number of cores
    for max_cores in max_core_values:
        df_result, times, spark_session = run_pipeline(
            max_cores, subs_to_incl, comment_threshold, sample_fraction
        )

        print(f"Printing runtimes from run with [{max_cores}] cores (in seconds): \n")
        print(f"{times} \n")

        # Save computation time for this max core setting in csv file
        timing_file = f"timing_cores_{max_cores}.csv"
        with open(f"{directory}{timing_file}", "w") as f:
            for key in times.keys():
                f.write(f"{key}: {times[key]} \n")

    # Convert dataframe to pandas as write to csv did not work
    df_result_out = df_result.toPandas()

    # Print output example
    print("Displaying first rows of the output file \n")
    print(df_result_out.head())

    # Save output dataframe as csv
    output_file = f"output.csv"
    df_result_out.to_csv(f"{directory}{output_file}")

    # Close connection to spark cluster
    spark_session.stop()

    # Return the output dataframe
    return df_result_out


def run_pipeline(max_cores, subs_to_incl, comment_threshold, sample_fraction):
    """Run all the steps in the pipeline in sequence"""

    # Spark cluster and HDFS settings
    master_address = "spark://192.168.2.70:9870"
    app_name = "group_12_app"
    hdfs_path = "hdfs://192.168.2.70:9000/path"

    # Dictionary for saving runtime
    times = {}

    # Step 1: Start spark session and load data
    start_time = time.perf_counter()
    spark_session = start_session(master_address, app_name, max_cores)
    df_raw = load_data(hdfs_path, spark_session)
    end_time = time.perf_counter()
    times["start_session_and_load_data"] = round(end_time - start_time, 2)

    # Step 2: Filter out columns that will not be used
    start_time = time.perf_counter()
    df_reddit = filter_columns_and_sample(df_raw, sample_fraction)
    end_time = time.perf_counter()
    times["filter_columns"] = round(end_time - start_time, 2)

    # Step 3: Count the biggest subreddits and filter dataframe for those
    start_time = time.perf_counter()
    df_subred_count, df_sub_filtered = filter_top_subreddits(df_reddit, subs_to_incl)
    end_time = time.perf_counter()
    times["filter_top_subreddits"] = round(end_time - start_time, 2)

    # Step 4: Get list with users with significant amount of posts and filter dataframe
    start_time = time.perf_counter()
    df_user_filtered = filter_top_users(df_sub_filtered, comment_threshold)
    end_time = time.perf_counter()
    times["filter_top_users"] = round(end_time - start_time, 2)

    # Step 5: Generate all subreddit combinations for each user
    start_time = time.perf_counter()
    df_user_subs = create_user_subreddit_tuples(df_user_filtered)
    end_time = time.perf_counter()
    times["create_user_subreddit_tuples"] = round(end_time - start_time, 2)

    # Step 6: Count how many times each subreddit combination appears
    start_time = time.perf_counter()
    df_tuple_counts = count_tuples(df_user_subs)
    end_time = time.perf_counter()
    times["count_tuples"] = round(end_time - start_time, 2)

    # Step 7: Remove duplicates from data
    start_time = time.perf_counter()
    df_no_dupes = remove_duplicates(df_tuple_counts)
    end_time = time.perf_counter()
    times["remove_duplicates"] = round(end_time - start_time, 2)

    # Step 8: Join dataframes with counts together
    start_time = time.perf_counter()
    df_result = join_count_data(spark_session, df_subred_count, df_no_dupes)
    end_time = time.perf_counter()
    times["join_count_data"] = round(end_time - start_time, 2)

    return df_result, times, spark_session


if __name__ == "__main__":
    do_runtime_experiments()
