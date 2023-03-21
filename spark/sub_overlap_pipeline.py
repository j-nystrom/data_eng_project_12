
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_set, udf, explode, count
from pyspark.sql.types import ArrayType, StringType, StructType, StructField


def start_session(master_address, app_name, max_cores):
    """
    Connect to spark cluster using private IP of spark master node

    Args:
        - ip: private IP of the spark master node
        - max_cores: max number of cores to utilize on each worker node
        - app_name: name of the application

    Returns:
        - spark_session: spark session object used to load data in next step
    """

    # Connection to project spark cluster
    spark_session = SparkSession.builder\
        .master(master_address)\
        .appName(app_name)\
        .config("spark.cores.max", max_cores)\
        .getOrCreate()

    return spark_session


def load_data(hdfs_path, spark_session):
    """
    Read json data and create dataframe. inferring headers from the data

    Args:
        - path: ip and file path of HDFS namenode
        - spark_session: the spark session object initiated in the previous step

    Returns:
        - df_raw: dataframe with one reddit comment per row; each column represents
            some attribute of the comment. see GitHub readme for details
    """

    df_raw = spark_session.read.options(multiline=False, header=True).json(hdfs_path)

    return df_raw


def filter_columns_and_sample(df_raw, sample_fraction):
    """
    Filter to only keep columns that will be relevant for the analysis, in order
    to reduce the size of the dataframe. Take random sample with specified fraction
    to facilitate scalability testing.

    Args:
        - df_raw: dataframe with one reddit comment per row
        - sample_fraction: fraction of data to sample

    Returns:
        - df_reddit: dataframe with the most relevant columns kept
    """

    # Define which columns should be kept
    cols_to_keep = [
        "author",
        "subreddit",
        "subreddit_id",
    ]

    # Select those columns to create new dataframe
    df_reddit_full = df_raw.select([col for col in cols_to_keep])

    # Sample the desired fraction
    df_reddit = df_reddit_full.sample(fraction=sample_fraction)

    return df_reddit


def filter_top_subreddits(df_reddit, subs_to_incl):
    """
    Create a list of the biggest subreddits by number of posts, then filter dataframe
    to only include these subreddits.

    Args:
        - df_reddit: dataframe containing relevant data for analysis
        - subs_to_incl: number of subreddits that should be included in top list

    Returns:
        - df_sub_filtered: dataframe filtered to contain only the largest subreddits
    """

    # Groupby subreddit and count how many posts there are in each
    df_subred_count = (
        df_reddit.groupBy("subreddit").count().sort("count", ascending=False)
    )

    # Take the top X rows and convert them to list
    df_top_subs = df_subred_count.limit(subs_to_incl)
    top_subs = df_top_subs.select("subreddit").rdd.flatMap(lambda x: x).collect()

    # Filter the original dataframe using the list
    df_sub_filtered = df_reddit.filter(col("subreddit").isin(top_subs))

    return df_subred_count, df_sub_filtered


def filter_top_users(df_sub_filtered, comment_threshold):
    """
    Create a list of all users who have made a significant amount of reddit comments.

    Args:
        - df_sub_filtered: dataframe filtered to contain only the largest subreddits
        - comment_threshold: min number of comments a user has posted in order to be
            included in the analysis

    Returns:
        - df_user_filtered: dataframe filtered for top subreddits and top users
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
    Create column with all combinations of overlapping subreddits a user has posted in.

    Args:
        - df_user_filtered: dataframe filtered for top subreddits and top users

    Returns:
        - df_user_subs: dataframe with two columns, one containing the user (author)
            and the other tuples of combinations of subreddits the user has posted in,
            e.g. (politics, CFB) and (politics, hockey)

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
    Count the occurrences of each tuple pair. Each user having posted in some
    combination of two subreddits, e.g. (politics, hockey), will add 1 to thie count.

    Args:
        - df_user_subs: dataframe with authors and their subreddit tuples

    Returns:
        - df_tuple_counts: dataframe that is grouped by each subreddit tuple,
            containing the number of times it occurs
    """

    # Explode the tuples into individual rows
    df_exploded = df_user_subs.select(
        explode("subreddit_tuples").alias("subred_tuples")
    )

    # Group by the exploded tuples and count the occurrences of each tuple
    df_tuple_counts = df_exploded.groupBy("subred_tuples").agg(
        count("*").alias("count")
    )

    return df_tuple_counts


def remove_duplicates(df_tuple_counts):
    """
    Clean the data by removing duplicates and tuples where both elements are the same:
        - (politics, CFB) and (CFB, politics) should not be double counted
        - (AskReddit, AskReddit) is not relevant for the overlap analysis

    Args:
        - df_tuple_counts: dataframe with occurences of each subreddit tuple
            (with duplicates)

    Returns:
        - df_result: clean dataframe without duplicates and tuples where both elements
            are identical
    """

    # Keep only rows where tuple elements are not identical
    df_counts_filtered = df_tuple_counts.filter(
        ~(
            col("subred_tuples").getField("tuple_1")
            == col("subred_tuples").getField("tuple_2")
        )
    )

    # Define custom schema for the sorted tuple column (to keep as is)
    tuple_schema = StructType(
        [
            StructField("tuple_1", StringType(), False),
            StructField("tuple_2", StringType(), False),
        ]
    )

    # Define UDF to sort the elements in each tuple
    def sorted_tuples(tup):
        return tuple(sorted(tup))

    # Apply the UDF to sort the tuples
    tuple_udf = udf(lambda x: sorted_tuples(x), tuple_schema)
    df_sorted_tuples = df_counts_filtered.withColumn(
        "subred_tuples", tuple_udf(col("subred_tuples"))
    )

    # Remove the duplicates to get a dataframe of half the lenght
    df_no_dupes = df_sorted_tuples.dropDuplicates()

    return df_no_dupes


def join_count_data(df_subred_count, df_no_dupes):
    """
    Join the comment count of each individual subreddit to the dataframe with tuple
    counts, to have all data in one place.

    Args:
        df_subred_count: dataframe with comment counts for each subreddit
        df_no_dupes: de-duplicated dataframe with counts for each pair of subreddits

    Returns:
        df_result_join: dataframe combining data from the two input dataframes
    """

    # Create columns for each tuple element, and rename count column
    # Create columns for each tuple element, and rename count column
    df_result = df_no_dupes.withColumn(
        "tup_1", col("subred_tuples").getField("tuple_1")
    )
    df_result = df_result.withColumn("tup_2", col("subred_tuples").getField("tuple_2"))
    df_result = df_result.withColumnRenamed("count", "tuple_count")

    # Join count data for the first tuple element
    df_result_join = df_result.join(
        df_subred_count, df_result.tup_1 == df_subred_count.subreddit
    ).select(df_result["*"], df_subred_count["count"])

    df_result_join = df_result_join.withColumnRenamed("count", "tup_1_count")

    # Join count data for the second tuple element
    df_result_join = df_result_join.join(
        df_subred_count, df_result_join.tup_2 == df_subred_count.subreddit
    ).select(df_result_join["*"], df_subred_count["count"])

    df_result_join = df_result_join.withColumnRenamed("count", "tup_2_count")

    return df_result_join
