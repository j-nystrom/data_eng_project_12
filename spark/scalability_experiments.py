
import time
import os
import sys

from sub_overlap_run import run_pipeline


def do_runtime_experiments(option):
    """Code for testing performance of the code for different number of cores used."""

    assert option in ["strong_scaling", "weak_scaling"], \
        f"Command line option is not in ['weak_scaling', 'strong_scaling']"

    # Create directory for saving output
    directory = f"data/{time.strftime('%Y-%m-%d %H:%M')}/"
    os.makedirs(directory, exist_ok=True)

    # Set up input parameters and variables
    subs_to_incl = 100
    comment_threshold = 3
    max_core_values = [1, 2, 3, 4, 5, 6]

    # Data sample fraction depend on the command line argument provided
    if option == "strong_scaling":
        sample_fractions = [0.1, 0.1, 0.1, 0.1, 0.1, 0.1]
    else:
        sample_fractions = [0.1, 0.1 * 2, 0.1 * 3, 0.1 * 4, 0.1 * 5, 0.1 * 6]

    # Zip max cores and sample fraction to create input tuples
    input_tuples = list(zip(max_core_values, sample_fractions))

    # Run for loop with different number of cores
    for max_cores, sample_fraction in input_tuples:
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


if __name__ == "__main__":
    do_runtime_experiments(sys.argv[1])
