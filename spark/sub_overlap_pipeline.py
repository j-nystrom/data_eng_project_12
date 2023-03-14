
import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_set, udf, explode, count
from pyspark.sql.types import ArrayType, StringType, StructType, StructField


def start_session():
    """
    Connect to spark cluster using private IP of spark master node

    Args:
        - ip: private ip of the spark master node
        - max_cores: max number of cores to utilize on each worker node
        - app_name: name of the application

    Returns:
        - spark_session:
    """

    spark_session = SparkSession.builder.appName("test_3").getOrCreate()

    return spark_session


def load_data(path, spark_session):
    """
    Read json data and create dataframe. inferring headers from the data

    Args:
        - path: ip and file path of HDFS namenode
        - spark_session: the spark session object initiated in the previous step

    Returns:
        - df_raw:
    """

    df_raw = spark_session.read.options(multiline=False, header=True).json(path)

    return df_raw


def filter_columns(df_raw):
    """
    Filter to only keep columns that will be relevant for the analysis, in order
    to reduce the size of the dataframe
    """

    # Define which columns should be kept
    cols_to_keep = [
        "author",
        "body",
        "created_utc",
        "score",
        "subreddit",
        "subreddit_id",
    ]

    # Select those columns to create new dataframe
    df_reddit = df_raw.select([col for col in cols_to_keep])

    return df_reddit


def filter_top_subreddits(df_reddit, subs_to_incl):
    """
    Create a list of the biggest subreddits by number of posts, then filter dataframe
    to only include these subreddits.

    Args:
        - df_reddit: dataframe containing all data
        - subs_to_incl: number of subreddits that should be included in top list

    Returns:
        - df_sub_filtered:
    """

    # Groupby subreddit and count how many posts there are in each
    df_subred_count = df_reddit.groupBy("subreddit").count()

    # Transform to pandas dataframe for easy slicing by index (this is a small df)
    # Take the top X rows and convert them to list
    df_count_pd = df_subred_count.toPandas()

    df_top_subs = df_count_pd.sort_values(by="count", ascending=False).iloc[
        0:subs_to_incl
    ]
    top_subs = df_top_subs["subreddit"].tolist()

    # Filter the original dataframe
    df_sub_filtered = df_reddit.filter(col("subreddit").isin(top_subs))

    return df_sub_filtered


def filter_top_users(df_sub_filtered, comment_threshold):
    """
    Create a list of all users who have made a significant amount of reddit comments.

    Args:
        - df_sub_filtered:
        - comment_threshold:

    Returns:
        - df_user_filtered:
    """

    # Groupby author and count how many posts each user has made
    df_user_count = df_sub_filtered.groupBy("author").count()
    df_top_users = df_user_count.filter(col("count") > comment_threshold)

    # Create a list to be used when filtering below
    top_users = df_top_users.select("author").rdd.flatMap(lambda x: x).collect()

    # Filter the dataframe
    df_user_filtered = df_sub_filtered.filter(col("author").isin(top_users))

    return df_user_filtered


def create_user_subreddit_tuples(df_user_filtered):
    """
    Create a column with all combinations of overlapping subreddits a user has posted in.

    Args:
        - df_user_filtered:

    Returns:
        - df_user_subs:
    """

    # Create a column that contains a list of all subreddit a user has posted in
    df_user_subs = df_user_filtered.groupby("author").agg(
        collect_set("subreddit").alias("subreddit")
    )

    # Define custom schema for output column
    tuple_schema = ArrayType(
        StructType(
            [
                StructField("tuple_1", StringType(), False),
                StructField("tuple_2", StringType(), False),
            ]
        )
    )

    # Define UDF to create tuples with all combinations of subreddits from the
    # user's list
    def tuple_from_list(lst):
        return [(sub_1, sub_2) for sub_1 in lst for sub_2 in lst]

    # Generate the tuple column
    tuple_udf = udf(lambda x: tuple_from_list(x), tuple_schema)
    df_user_subs = df_user_subs.withColumn(
        "subreddit_tuples", tuple_udf(col("subreddit"))
    )

    return df_user_subs


def count_tuples(df_user_subs):
    """
    Count the occurrences of each tuple pair.

    Args:
        -

    Returns:
        -
    """

    # Explode the tuples into individual rows
    df_exploded = df_user_subs.select(explode("subreddit_tuples").alias("tuple_col"))

    # Group by the exploded tuples and count the occurrences of each tuple
    df_tuple_counts = df_exploded.groupBy("tuple_col").agg(count("*").alias("count"))

    return df_tuple_counts


def remove_duplicates(df_tuple_counts):
    """
    Clean the data by removing duplicates and tuples where both elements are the same:
        - (politics, CFB) and (CFB, politics) should not be double counted
        - (AskReddit, AskReddit) is not relevant for the overlap analysis

    Args:
        - df_tuple_counts:

    Returns:
        - df_result:
    """

    # Keep only rows where tuple elements are not identical
    df_counts_filtered = df_tuple_counts.filter(
        ~(col("tuple_col").getField("tuple_1") == col("tuple_col").getField("tuple_2"))
    )

    # Create list using list comprehension
    result_clean = df_counts_filtered.rdd.map(
        lambda row: [(row[0][0], row[0][1]), row[1]]
    ).collect()

    # Filter out the duplicates (each second occurrence)
    # TODO: Investigate if this can be made more efficient with PySpark
    result_no_dupes = []
    encountered_pairs = set()
    for lst in result_clean:
        tup = lst[0]
        count = lst[1]
        sorted_tup = tuple(sorted(tup))

        if sorted_tup in encountered_pairs:
            continue

        encountered_pairs.add(sorted_tup)
        result_no_dupes.append([sorted_tup, count])

    # Create dataframe from the cleaned list
    df_result = pd.DataFrame(
        result_no_dupes, columns=["subreddits", "count"]
    ).sort_values(by=["count"], ascending=False)

    return df_result
