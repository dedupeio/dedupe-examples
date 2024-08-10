#!/usr/bin/python
"""
This code demonstrates how to use dedupe to disambiguate patent
authors and demonstrates the Set and LatLong data types.

"""

import csv
import logging
import optparse
import os

import dedupe


def readData(filename, set_delim="**"):
    """
    Remap columns for the following cases:
    - Lat and Long are mapped into a single LatLong tuple
    - Class and Coauthor are stored as delimited strings but mapped into
      tuples
    """

    data_d = {}
    with open(filename) as f:
        reader = csv.DictReader(f)
        for idx, row in enumerate(reader):
            row = {k: v.lower() for k, v in row.items()}
            if row["Lat"] == row["Lng"] == "0.0":
                row["LatLong"] = None
            else:
                row["LatLong"] = (float(row["Lat"]), float(row["Lng"]))
            row["Class"] = (
                tuple(sorted(row["Class"].split(set_delim))) if row["Class"] else None
            )
            row["Coauthor"] = tuple(
                sorted(
                    [
                        author
                        for author in row["Coauthor"].split(set_delim)
                        if author != "none"
                    ]
                )
            )
            if row["Name"] == "":
                row["Name"] = None

            data_d[idx] = row

    return data_d


# These generators will give us the corpora setting up the Set
# distance metrics
def classes(data):
    for record in data.values():
        yield record["Class"]


def coauthors(data):
    for record in data.values():
        yield record["Coauthor"]


def names(data):
    for record in data.values():
        yield record["Name"]


if __name__ == "__main__":

    # ## Logging

    # Dedupe uses Python logging to show or suppress verbose output. Added
    # for convenience.  To enable verbose logging, run `python
    # patent_example.py -v`

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
        elif opts.verbose > 1:
            log_level = logging.DEBUG
    logging.getLogger().setLevel(log_level)

    input_file = "patstat_input.csv"
    output_file = "patstat_output.csv"
    settings_file = "patstat_settings.json"
    training_file = "patstat_training.json"

    print("importing data ...")
    data_d = readData(input_file)

    # ## Training

    if os.path.exists(settings_file):
        print("reading from", settings_file)
        with open(settings_file, "rb") as sf:
            deduper = dedupe.StaticDedupe(sf, num_cores=2)

    else:
        # Define the fields dedupe will pay attention to
        fields = [
            dedupe.variables.String("Name", name="name_string", has_missing=True),
            dedupe.variables.LatLong("LatLong", has_missing=True),
            dedupe.variables.Set("Class", corpus=classes(data_d), has_missing=True),
            dedupe.variables.Set(
                "Coauthor", corpus=coauthors(data_d), has_missing=True
            ),
            dedupe.variables.Text(
                "Name", name="name_text", corpus=names(data_d), has_missing=True
            ),
            dedupe.variables.Interaction("name_string", "name_text"),
        ]

        # Create a new deduper object and pass our data model to it.
        deduper = dedupe.Dedupe(fields, num_cores=2)

        # If we have training data saved from a previous run of dedupe,
        # look for it an load it in.
        if os.path.exists(training_file):
            print("reading labeled examples from ", training_file)
            with open(training_file) as tf:
                deduper.prepare_training(data_d, training_file=tf)
        else:
            deduper.prepare_training(data_d)

        # ## Active learning

        # Starts the training loop. Dedupe will find the next pair of records
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

    clustered_dupes = deduper.partition(data_d, 0.5)

    print("# duplicate sets", len(clustered_dupes))

    # ## Writing Results

    # Write our original data back out to a CSV with a new column called
    # 'Cluster ID' which indicates which records refer to each other.

    cluster_membership = {}
    for cluster_id, (records, scores) in enumerate(clustered_dupes):
        for record_id, score in zip(records, scores):
            cluster_membership[record_id] = {
                "Cluster ID": cluster_id,
                "confidence_score": score,
            }

    with open(output_file, "w") as f_output, open(input_file) as f_input:

        reader = csv.DictReader(f_input)
        fieldnames = ["Cluster ID", "confidence_score"] + reader.fieldnames

        writer = csv.DictWriter(f_output, fieldnames=fieldnames)
        writer.writeheader()

        for row_id, row in enumerate(reader):
            row.update(cluster_membership[row_id])
            writer.writerow(row)
