#!/usr/bin/python
"""
This code demonstrates how to use some extended dedupe variables
"""

import csv
import logging
import optparse
import os
import re

import addressvariable
import dedupe
import namevariable
import unidecode


def preProcess(column):
    """
    Do a little bit of data cleaning with the help of Unidecode and Regex.
    Things like casing, extra spaces, quotes and new lines can be ignored.
    """
    column = unidecode.unidecode(column)
    column = re.sub("  +", " ", column)
    column = re.sub("\n", " ", column)
    column = column.strip().strip('"').strip("'").lower().strip()
    return column


def readData(filename):
    """
    Read in our data from a CSV file and create a dictionary of records,
    where the key is a unique record ID and each value is dict
    """

    data_d = {}
    with open(filename) as f:
        reader = csv.DictReader(f)
        for row in reader:
            clean_row = {k: preProcess(v) for (k, v) in row.items()}
            clean_row["name"] = " ".join(
                [clean_row["FirstName"], clean_row["LastName"]]
            )
            if not clean_row["name"]:
                clean_row["name"] = None
            clean_row["address"] = " ".join(
                [clean_row["Address1"], clean_row["Address2"]]
            )
            if not clean_row["address"]:
                clean_row["address"] = None
            row_id = int(row["ID"])

            for k, v in clean_row.items():
                if not v:
                    clean_row[k] = None

            data_d[row_id] = clean_row

    return data_d


if __name__ == "__main__":

    # ## Logging

    # Dedupe uses Python logging to show or suppress verbose
    # output. Added for convenience.  To enable verbose logging, run
    # `python examples/csv_example/csv_example.py -v`
    optp = optparse.OptionParser()
    optp.add_option(
        "-v",
        "--verbose",
        dest="verbose",
        action="count",
        help="Increase verbosity (specify multiple times for more)",
    )
    (opts, args) = optp.parse_args()
    log_level = logging.WARNING
    if opts.verbose:
        if opts.verbose == 1:
            log_level = logging.INFO
        elif opts.verbose >= 2:
            log_level = logging.DEBUG
    logging.getLogger().setLevel(log_level)

    # ## Setup

    input_file = "officers.csv"
    output_file = "officers_output.csv"
    settings_file = "officers_settings"
    training_file = "officers_training.json"

    print("importing data ...")
    data_d = readData(input_file)

    # ## Training

    if os.path.exists(settings_file):
        print("reading from", settings_file)
        with open(settings_file, "rb") as f:
            deduper = dedupe.StaticDedupe(f)

    else:
        # Define the fields dedupe will pay attention to
        #
        # Notice how we are telling dedupe to use a custom field comparator
        # for the 'Zip' field.
        fields = [
            namevariable.WesternName("name", crf=True),
            addressvariable.USAddress("address", crf=True),
            dedupe.variables.ShortString("City"),
            dedupe.variables.ShortString("State"),
            dedupe.variables.ShortString("Zip"),
            dedupe.variables.ShortString("Phone"),
            dedupe.variables.Categorical(
                "RedactionRequested", categories=["true", "false"]
            ),
        ]

        # Create a new deduper object and pass our data model to it.
        deduper = dedupe.Dedupe(fields)

        # If we have training data saved from a previous run of dedupe,
        # look for it an load it in.
        # __Note:__ if you want to train from scratch, delete the training_file
        if os.path.exists(training_file):
            print("reading labeled examples from ", training_file)
            with open(training_file, "rb") as f:
                deduper.prepare_training(data_d, training_file=f)
        else:
            deduper.prepare_training(data_d, sample_size=300)

        # ## Active learning
        # Dedupe will find the next pair of records
        # it is least certain about and ask you to label them as duplicates
        # or not.
        # use 'y', 'n' and 'u' keys to flag duplicates
        # press 'f' when you are finished
        print("starting active labeling...")

        dedupe.console_label(deduper)

        deduper.train()

        # When finished, save our training away to disk
        with open(training_file, "w") as tf:
            deduper.write_training(tf)

        # Save our weights and predicates to disk.  If the settings file
        # exists, we will skip all the training and learning next time we run
        # this file.
        with open(settings_file, "wb") as sf:
            deduper.write_settings(sf)

    # ## Clustering

    # `match` will return sets of record IDs that dedupe
    # believes are all referring to the same entity.

    print("clustering...")
    clustered_dupes = deduper.partition(data_d, threshold=0.5)

    print("# duplicate sets", len(clustered_dupes))

    # ## Writing Results

    # Write our original data back out to a CSV with a new column called
    # 'Cluster ID' which indicates which records refer to each other.

    cluster_membership = {}
    for cluster_id, cluster in enumerate(clustered_dupes):
        id_set, scores = cluster
        for record_id, score in zip(id_set, scores):
            cluster_membership[record_id] = {
                "cluster id": cluster_id,
                "confidence": score,
            }

    with open(output_file, "w") as f_output:
        writer = csv.writer(f_output)

        with open(input_file) as f_input:

            reader = csv.DictReader(f_input)
            fieldnames = ["cluster id", "confidence"] + reader.fieldnames

            writer = csv.DictWriter(f_output, fieldnames=fieldnames)
            writer.writeheader()

            for row in reader:
                row_id = int(row["ID"])
                row.update(cluster_membership[row_id])
                writer.writerow(row)
