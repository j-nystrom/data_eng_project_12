
import time
import os
import sys

from sub_overlap_run import run_pipeline


def do_runtime_experiments(option):
    """Code for testing performance of the code for different number of cores used."""

    assert option in ["strong_scaling", "weak_scaling"], \
        f"Command line option is not in ['weak_scaling', 'strong_scaling']"

    # Create directory for saving output
    directory = f"data/{time.strftime('%Y-%m-%d %H:%M') + '_' + option}/"
    os.makedirs(directory, exist_ok=True)

    # Set up input parameters and variables
    comment_threshold = 3
    max_core_values = [1, 2, 3, 4, 5, 6]

    # Data sample fraction depend on the command line argument provided
    if option == "strong_scaling":
        sample_fractions = [1.0, 1.0, 1.0, 1.0, 1.0, 1.0]
    else:
        sample_fractions = [(1/6), (2/6), (3/6), (4/6), (5/6), (6/6)]

    # Zip max cores and sample fraction to create input tuples
    input_tuples = list(zip(max_core_values, sample_fractions))

    # Run for loop with different number of cores
    for max_cores, sample_fraction in input_tuples:

        times = {}
        start_time = time.perf_counter()

        df_result, spark_session = run_pipeline(
            max_cores, comment_threshold, sample_fraction
        )

        # Count forces spark to calculate the previous RDD
        df_result.count()

        end_time = time.perf_counter()
        times["Total_runtime"] = round(end_time - start_time, 2)

        # The session needs to be stopped to update the amount of cores, the last run is needed for printing the results
        if max_cores != max_core_values[-1]:
            spark_session.stop()

        print(f"Printing runtimes from run with [{max_cores}] cores (in seconds): \n")
        print(f"{times} \n")

        # Save computation time for this max core setting in csv file
        timing_file = f"timing_cores_{max_cores}.csv"
        with open(f"{directory}{timing_file}", "w") as f:
            for key in times.keys():
                f.write(f"{key}: {times[key]} \n")

    # Print output example
    print("Displaying first rows of the output file \n")
    print("PySpark version \n")
    print(df_result.show(20))

    # Convert dataframe to pandas as write to csv did not work
    df_result_out = df_result.toPandas()
    print("Pandas version \n")
    print(df_result_out.head(20))

    # Save output dataframe as csv
    output_file = f"output.parquet"
    df_result_out.to_parquet(f"{directory}{output_file}")

    # Close connection to spark cluster
    spark_session.stop()

    # Return the output dataframe
    return df_result_out


if __name__ == "__main__":
    do_runtime_experiments(sys.argv[1])
