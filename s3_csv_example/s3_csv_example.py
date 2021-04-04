#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
This code shows how to read input files from an S3 bucket, purge the bucket from the S3 folder,
then write the output to a different path in the S3 bucket
"""

import os
import csv
import re
import logging
import optparse
import sys

import dedupe
from unidecode import unidecode


def preProcess(column):
    """
    Do a little bit of data cleaning with the help of Unidecode and Regex.
    Things like casing, extra spaces, quotes and new lines can be ignored.
    """
    column = unidecode(column)
    column = re.sub('  +', ' ', column)
    column = re.sub('\n', ' ', column)
    column = column.strip().strip('"').strip("'").lower().strip()
    # If data is missing, indicate that by setting the value to `None`
    if not column:
        column = None
    return column


def readData(filename, idcol):
    """
    Read in our data from a CSV file and create a dictionary of records,
    where the key is a unique record ID and each value is dict
    """

    data_d = {}
    with open(filename) as f:
        reader = csv.DictReader(f)
        for row in reader:
            clean_row = [(k, preProcess(v)) for (k, v) in row.items()]
            row_id = str(int(row['FileNo'])) + '.' + str(int(row[idcol]))
            data_d[row_id] = dict(clean_row)

    return data_d

def writeToS3Bucket(local_file_to_send, output_bucket, s3output_file):
    s3 = boto3.resource('s3')
    s3.meta.client.upload_file(local_file_to_send, output_bucket, s3output_file)
    os.remove(local_file_to_send)


if __name__ == '__main__':

    # ## Logging

    # Dedupe uses Python logging to show or suppress verbose output. This
    # code block lets you change the level of loggin on the command
    # line. You don't need it if you don't want that. To enable verbose
    # logging, run `python examples/csv_example/csv_example.py -v`
    optp = optparse.OptionParser()
    optp.add_option('-v', '--verbose', dest='verbose', action='count',
                    help='Increase verbosity (specify multiple times for more)'
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
    import time
    timestr = time.strftime(".%Y.%m.%d-%H.%M.%S")

    FileSource = "comebackkc1"
    output_file = 's3_csv_example_output' + timestr + '.csv'
    s3output_file = 'output/s3_csv_example_output' + timestr + '.csv'
    if FileSource == "dedupesample":
        settings_file = 's3_csv_example_learned_settings'
        training_file = 's3_csv_example_training.json'
        idcol = 'Id'
    else: #FileSource = "comebackkc1"
        settings_file = 'reals3_csv_example_learned_settings'
        training_file = 'reals3_csv_example_training.json'
        idcol = 'Receipt Number'

    scriptpath = os.path.dirname(__file__)
    #output_file = os.path.join(scriptpath, output_file)
    settings_file = os.path.join(scriptpath, settings_file)
    training_file = os.path.join(scriptpath, training_file)

    import sys
    if len(sys.argv) > 1:
        bucket = sys.argv[1]
    if len(sys.argv) > 1:
        output_bucket = sys.argv[2]
    bucket='c4kc-cvax-deduplication'
    output_bucket=bucket
    s3files = []

    import boto3
    s3_client = boto3.client('s3')
    if FileSource == "dedupesample":
        result = s3_client.list_objects(Bucket = bucket, Prefix='')
        for o in result.get('Contents'):
            filename = o.get('Key');
            if filename[:7] != 'output/': #don't process files that are in the /output path of the s3 bucket
            
                data = s3_client.get_object(Bucket=bucket, Key=filename)
                if filename[:6] == 'input/' and len(filename) > 6:
                    print(filename)
                    local_file = filename[6:]  #remove input/ from the name of the file
                    s3_client.download_file(bucket,filename,local_file)
                    s3files.append(local_file)
     #               response = s3_client.delete_object(
     #                   Bucket=bucket,
     #                   Key=filename)
        csv_header = 'Id,Source,Site name,Address,Zip,Phone,Fax,Program Name,Length of Day,IDHS Provider ID,Agency,Neighborhood,Funded Enrollment,Program Option,Number per Site EHS,Number per Site HS,Director,Head Start Fund,Eearly Head Start Fund,CC fund,Progmod,Website,Executive Director,Center Director,ECE Available Programs,NAEYC Valid Until,NAEYC Program Id,Email Address,Ounce of Prevention Description,Purple binder service type,Column,Column2'
    else: #FileSource = "comebackkc1"
        s3files.append('C:/Users/robkr/Downloads/ResponseExport-KCRegionalCOVID19VaccinationSurvey-20210323 (1)/ResponseExport-KCRegionalCOVID19VaccinationSurvey-20210323.csv')
        csv_header = 'Receipt Number,Response Reference ID,Respondent Email,URL submitted from,Form version submitted in,Response Submission DateTime,Time Taken To Complete (seconds),External ID,External Status,Are you planning to receive the COVID-19 vaccine?,What makes you uncertain about receiving the COVID-19 vaccine?,When would you like to get your first dose of the COVID-19 vaccine?,What state do you live in?,Which Kansas county do you live in?,"Do you live within the city limits of Kansas City, MO?",Which Missouri county do you live in?,What is your zip code?,First Name,Last Name,Street Address,City,"Phone number (please enter numbers only, no dashes, spaces, or parentheses)",Email address,What is your preferred method of contact?,We need your permission to contact you about COVID-19 testing and vaccination.,Is it okay for us to leave you a voicemail?,Sex,Age,Race/ethnicity (check as many as apply),"Do you have any pre-existing medical conditions or do you have any conditions that put you at increased risk of severe illness? (i.e. immunocompromised, diabetes, chronic lung conditions, cardiovascular disease, morbid obesity, etc.)","Do you live in or visit often crowded living settings? (For example, a supportive care facility, assisted living facility, group home, homeless shelter, or correctional setting)","How many members, including yourself, live in your household?",Do you have a history of any of the following pre-existing medical conditions?,Are you immuno-compromised?,What is your work status?,"What is your work zip code, either where you normally work in the office or on location or the zip code of your employers primary office?",Are you employed by any of the following types of patient-facing organizations?,What is the name of the health care provider for which you work?,Are you responsible for or in a position to influence vaccination planning for your employer or another organization?,What is the name of your employer or other organization?,What is your job title or role at your organization?'
#new stuff here
    csv_out = 'C:/Users/robkr/Source/Repos/dedupe-examples/s3_csv_example/combinedfile.csv'



    fileno = 1
    count = -1
    csv_merge = open(csv_out, 'w')
    csv_headerNew = 'Receipt Number,Response Reference ID,Respondent Email,URL submitted from,Form version submitted in,Response Submission DateTime,Time Taken To Complete (seconds),External ID,External Status,Are you planning to receive the COVID-19 vaccine?,What makes you uncertain about receiving the COVID-19 vaccine?,When would you like to get your first dose of the COVID-19 vaccine?,What state do you live in?,Which Kansas county do you live in?,"Do you live within the city limits of Kansas City, MO?",Which Missouri county do you live in?,Zip,FirstName,LastName,Street Address,City,"Phone",Email,What is your preferred method of contact?,We need your permission to contact you about COVID-19 testing and vaccination.,Is it okay for us to leave you a voicemail?,Sex,Age,Race/ethnicity (check as many as apply),"Do you have any pre-existing medical conditions or do you have any conditions that put you at increased risk of severe illness? (i.e. immunocompromised, diabetes, chronic lung conditions, cardiovascular disease, morbid obesity, etc.)","Do you live in or visit often crowded living settings? (For example, a supportive care facility, assisted living facility, group home, homeless shelter, or correctional setting)","How many members, including yourself, live in your household?",Do you have a history of any of the following pre-existing medical conditions?,Are you immuno-compromised?,What is your work status?,"What is your work zip code, either where you normally work in the office or on location or the zip code of your employers primary office?",Are you employed by any of the following types of patient-facing organizations?,What is the name of the health care provider for which you work?,Are you responsible for or in a position to influence vaccination planning for your employer or another organization?,What is the name of your employer or other organization?,What is your job title or role at your organization?'
    csv_merge.write('FileNo,' + csv_headerNew)
    csv_merge.write('\n')
    for file in s3files:
        csv_in = open(file,"r",encoding='utf-8') #https://stackoverflow.com/questions/49562499/how-to-fix-unicodedecodeerror-charmap-codec-cant-decode-byte-0x9d-in-posit
        for line in csv_in:
            if count < 1: #line.startswith(csv_header):
                count = count + 1
                continue
            csv_merge.write(str(fileno) + ',' + line)
            print(count)
            count = count + 1
        csv_in.close()
        fileno = fileno + 1
    csv_merge.close()

    #'Email','Zip','FirstName','LastName','City','Phone'

        #fields = [
        #        {'field': 'Email address', 'type': 'Exact', 'has missing': True},
        #        {'field': 'What is your zip code?', 'type': 'String'},
        #        {'field': 'First Name', 'type': 'String'},
        #        {'field': 'Last Name', 'type': 'String'},
        #        {'field': 'City', 'type': 'String'},
        #        {'field': 'What is your zip code?', 'type': 'Exact', 'has missing': True},
        #        {'field': 'Phone number (please enter numbers only, no dashes, spaces, or parentheses)', 'type': 'String', 'has missing': True},
        #        ]
    #with open(output_file, 'w') as f_output, open(csv_out) as f_input:

    #    newcols = []
    #    reader = csv.DictReader(f_input)
    #    for colname in reader.fieldnames:
    #        colname = str.replace(colname,"First Name","FirstName")
    #        colname = str.replace(colname,"Last Name","LastName")
    #        colname = str.replace(colname,"Email address","Email")
    #        colname = str.replace(colname,"Phone number (please enter numbers only, no dashes, spaces, or parentheses)","Phone")
    #        colname = str.replace(colname,"What is your zip code?","Zip")
    #        newcols.append(colname)

        #writer = csv.DictWriter(f_output, fieldnames=newcols)
        #writer.writeheader()


    #results = pd.DataFrame([])
#https://www.techbeamers.com/merge-multiple-csv-files/
    #csv_header = 'Receipt Number,Response Reference ID,Respondent Email,URL submitted from,Form version submitted in,Response Submission DateTime,Time Taken To Complete (seconds),External ID,External Status,Are you planning to receive the COVID-19 vaccine?,What makes you uncertain about receiving the COVID-19 vaccine?,When would you like to get your first dose of the COVID-19 vaccine?,What state do you live in?,Which Kansas county do you live in?,"Do you live within the city limits of Kansas City, MO?",Which Missouri county do you live in?,What is your zip code?,First Name,Last Name,Street Address,City,"Phone number (please enter numbers only, no dashes, spaces, or parentheses)",Email address,What is your preferred method of contact?,We need your permission to contact you about COVID-19 testing and vaccination.,Is it okay for us to leave you a voicemail?,Sex,Age,Race/ethnicity (check as many as apply),"Do you have any pre-existing medical conditions or do you have any conditions that put you at increased risk of severe illness? (i.e. immunocompromised, diabetes, chronic lung conditions, cardiovascular disease, morbid obesity, etc.)","Do you live in or visit often crowded living settings? (For example, a supportive care facility, assisted living facility, group home, homeless shelter, or correctional setting)","How many members, including yourself, live in your household?",Do you have a history of any of the following pre-existing medical conditions?,Are you immuno-compromised?,What is your work status?,"What is your work zip code, either where you normally work in the office or on location or the zip code of your employers primary office?",Are you employed by any of the following types of patient-facing organizations?,What is the name of the health care provider for which you work?,Are you responsible for or in a position to influence vaccination planning for your employer or another organization?,What is the name of your employer or other organization?,What is your job title or role at your organization?'

#end new

    print('importing data ...')
    data_d = {}
    data_d.update(readData(csv_out, idcol))
#    for eachFile in s3files:
 #       data_d.update(readData(eachFile, idcol))

    if not data_d:
        print('no files found to process in s3 bucket named: ' + bucket)
        os._exit(1)

    # If a settings file already exists, we'll just load that and skip training
    if os.path.exists(settings_file):
        print('reading from', settings_file)
        with open(settings_file, 'rb') as f:
            deduper = dedupe.StaticDedupe(f)
    else:
        # ## Training

        # Define the fields dedupe will pay attention to
        if FileSource == "dedupesample":
            fields = [
                {'field': 'Site name', 'type': 'String'},
                {'field': 'Address', 'type': 'String'},
                {'field': 'Zip', 'type': 'Exact', 'has missing': True},
                {'field': 'Phone', 'type': 'String', 'has missing': True},
                ]
        else: #FileSource = "comebackkc1"
            fields = [
                {'field': 'Email', 'type': 'Exact', 'has missing': True},
                {'field': 'Zip', 'type': 'String'},
                {'field': 'FirstName', 'type': 'String'},
                {'field': 'LastName', 'type': 'String'},
                {'field': 'City', 'type': 'String'},
                {'field': 'Zip', 'type': 'Exact', 'has missing': True},
                {'field': 'Phone', 'type': 'String', 'has missing': True},
                ]
        # Create a new deduper object and pass our data model to it.
        deduper = dedupe.Dedupe(fields)

        # If we have training data saved from a previous run of dedupe,
        # look for it and load it in.
        # __Note:__ if you want to train from scratch, delete the training_file
        if os.path.exists(training_file):
            print('reading labeled examples from ', training_file)
            with open(training_file, 'rb') as f:
                deduper.prepare_training(data_d, f)
        else:
            deduper.prepare_training(data_d)

        # ## Active learning
        # Dedupe will find the next pair of records
        # it is least certain about and ask you to label them as duplicates
        # or not.
        # use 'y', 'n' and 'u' keys to flag duplicates
        # press 'f' when you are finished
        print('starting active labeling...')

        dedupe.console_label(deduper)

        # Using the examples we just labeled, train the deduper and learn
        # blocking predicates
        deduper.train()

        # When finished, save our training to disk
        with open(training_file, 'w') as tf:
            deduper.write_training(tf)

        # Save our weights and predicates to disk.  If the settings file
        # exists, we will skip all the training and learning next time we run
        # this file.
        with open(settings_file, 'wb') as sf:
            deduper.write_settings(sf)

    # ## Clustering

    # `partition` will return sets of records that dedupe
    # believes are all referring to the same entity.

    print('clustering...')
    clustered_dupes = deduper.partition(data_d, 0.5)

    print('# duplicate sets', len(clustered_dupes))

    # ## Writing Results

    # Write our original data back out to a CSV with a new column called
    # 'Cluster ID' which indicates which records refer to each other.

    cluster_membership = {}
    for cluster_id, (records, scores) in enumerate(clustered_dupes):
        for record_id, score in zip(records, scores):
            cluster_membership[record_id] = {
                "Cluster ID": cluster_id,
                "confidence_score": score
            }

    with open(output_file, 'w') as f_output, open(csv_out) as f_input:

        reader = csv.DictReader(f_input)
        fieldnames = ['Cluster ID', 'confidence_score'] + reader.fieldnames

        writer = csv.DictWriter(f_output, fieldnames=fieldnames)
        writer.writeheader()

        for row in reader:
            row_id = str(int(row['FileNo'])) + '.' + str(int(row[idcol]))
            row.update(cluster_membership[row_id])
            writer.writerow(row)
    writeToS3Bucket(output_file, output_bucket, s3output_file)

    #C:\Users\Administrator\AppData\Local\Programs\Python\Python39\python s3_csv_example.py c4kc-cvax-deduplication c4kc-cvax-deduplication

    #data sort, by confidence id smallest to largest then cluster id
    