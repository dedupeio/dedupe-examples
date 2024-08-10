#!/usr/bin/python
"""
This code demonstrates the Gazetteer.

We will use one of the sample files from the RecordLink example as the
canonical set.

"""

import csv
import logging
import optparse
import os
import re

import dedupe
from unidecode import unidecode


def preProcess(column):
    """
    Do a little bit of data cleaning with the help of Unidecode and Regex.
    Things like casing, extra spaces, quotes and new lines can be ignored.
    """

    column = unidecode(column)
    column = re.sub("\n", " ", column)
    column = re.sub("-", "", column)
    column = re.sub("/", " ", column)
    column = re.sub("'", "", column)
    column = re.sub(",", "", column)
    column = re.sub(":", " ", column)
    column = re.sub(" +", " ", column)
    column = column.strip().strip('"').strip("'").lower().strip()
    if not column:
        column = None
    return column


def readData(filename):
    """
    Read in our data from a CSV file and create a dictionary of records,
    where the key is a unique record ID.
    """

    data_d = {}

    with open(filename) as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            clean_row = {k: preProcess(v) for (k, v) in row.items()}
            if clean_row["price"]:
                clean_row["price"] = float(clean_row["price"][1:])
            data_d[filename + str(i)] = dict(clean_row)

    return data_d


if __name__ == "__main__":

    # ## Logging

    # dedupe uses Python logging to show or suppress verbose output. Added
    # for convenience.  To enable verbose logging, run `python
    # examples/csv_example/csv_example.py -v`
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

    output_file = "gazetteer_output.csv"
    settings_file = "gazetteer_learned_settings"
    training_file = "gazetteer_training.json"

    canon_file = os.path.join("data", "AbtBuy_Buy.csv")
    messy_file = os.path.join("data", "AbtBuy_Abt.csv")

    print("importing data ...")
    messy = readData(messy_file)
    print(f"N data 1 records: {len(messy)}")

    canonical = readData(canon_file)
    print(f"N data 2 records: {len(canonical)}")

    def descriptions():
        for dataset in (messy, canonical):
            for record in dataset.values():
                yield record["description"]

    if os.path.exists(settings_file):
        print("reading from", settings_file)
        with open(settings_file, "rb") as sf:
            gazetteer = dedupe.StaticGazetteer(sf)

    else:
        # Define the fields the gazetteer will pay attention to
        fields = [
            dedupe.variables.String("title"),
            dedupe.variables.Text("title"),
            dedupe.variables.Text(
                "description", corpus=descriptions(), has_missing=True
            ),
            dedupe.variables.Price("price", has_missing=True),
        ]

        # Create a new gazetteer object and pass our data model to it.
        gazetteer = dedupe.Gazetteer(fields)

        # If we have training data saved from a previous run of gazetteer,
        # look for it an load it in.
        # __Note:__ if you want to train from scratch, delete the training_file
        if os.path.exists(training_file):
            print("reading labeled examples from ", training_file)
            with open(training_file) as tf:
                gazetteer.prepare_training(messy, canonical, training_file=tf)
        else:
            gazetteer.prepare_training(messy, canonical)

        # ## Active learning
        # Dedupe will find the next pair of records
        # it is least certain about and ask you to label them as matches
        # or not.
        # use 'y', 'n' and 'u' keys to flag duplicates
        # press 'f' when you are finished
        print("starting active labeling...")

        dedupe.console_label(gazetteer)

        gazetteer.train()

        # When finished, save our training away to disk
        with open(training_file, "w") as tf:
            gazetteer.write_training(tf)

        # Save our weights and predicates to disk.  If the settings file
        # exists, we will skip all the training and learning next time we run
        # this file.
        with open(settings_file, "wb") as sf:
            gazetteer.write_settings(sf)

        gazetteer.cleanup_training()

    gazetteer.index(canonical)

    results = gazetteer.search(messy, n_matches=2, generator=True)

    cluster_membership = {}
    cluster_id = 0

    for cluster_id, (messy_id, matches) in enumerate(results):
        for canon_id, score in matches:
            cluster_membership[messy_id] = {
                "Cluster ID": cluster_id,
                "Link Score": score,
            }
            cluster_membership[canon_id] = {
                "Cluster ID": cluster_id,
                "Link Score": score,
            }
            cluster_id += 1

    with open(output_file, "w") as f:

        header_unwritten = True

        for fileno, filename in enumerate((messy_file, canon_file)):
            with open(filename) as f_input:
                reader = csv.DictReader(f_input)

                if header_unwritten:

                    fieldnames = [
                        "Cluster ID",
                        "Link Score",
                        "source file",
                    ] + reader.fieldnames

                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()

                    header_unwritten = False

                for row_id, row in enumerate(reader):

                    record_id = filename + str(row_id)
                    cluster_details = cluster_membership.get(record_id, {})
                    row["source file"] = fileno
                    row.update(cluster_details)

                    writer.writerow(row)
