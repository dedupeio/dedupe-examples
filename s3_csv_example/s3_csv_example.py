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

import pandas as pd

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
def preProcessCase(column):
    """
    Do a little bit of data cleaning with the help of Unidecode and Regex.
    Things like casing, extra spaces, quotes and new lines can be ignored.
    """
    column = unidecode(column)
    column = re.sub('  +', ' ', column)
    column = re.sub('\n', ' ', column)
    column = column.strip().strip('"').strip("'").strip()
    # If data is missing, indicate that by setting the value to `None`
    if not column:
        column = None
    return column
def readMappings(filename):


    data_d = {}
    with open(mappings_file) as f:
        reader = csv.DictReader(f)
        for row in reader:
            fldSource = str(row['Source'])
            clean_row = [(k, preProcessCase(v)) for (k, v) in row.items()]
            data_d[fldSource] = dict(clean_row)

    return data_d

def readData(filename):
    """
    Read in our data from a CSV file and create a dictionary of records,
    where the key is a unique record ID and each value is dict
    """

    data_d = {}
    with open(filename) as f:
        reader = csv.DictReader(f)
        for row in reader:
            clean_row = [(k, preProcess(v)) for (k, v) in row.items()]
            row_id = str(int(row[fieldNameFileNo])) + '.' + str(int(row[fieldNameIdCol]))
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
    
    fieldNameFileNo = 'FileNo' #not dynamic - we totally control this
    fieldNameFileName = 'FileName' #not dynamic - we totally control this
    fieldNameIdInSource = 'Key' #not dynamic - we totally control this
    fieldNameClusterId = 'ClusterId' #not dynamic - we totally control this 
    fieldNameConfidence = 'ConfidenceScore' #not dynamic - we totally control this 
    FileSource = "comebackkc1"
    output_file = 's3_csv_example_output' + timestr + '.csv'
    s3output_file = 'output/s3_csv_example_output' + timestr + '.csv'
    if FileSource == "dedupesample":
        settings_file = 's3_csv_example_learned_settings'
        training_file = 's3_csv_example_training.json'
        fieldNameIdCol = 'Id'
    else: #FileSource = "comebackkc1"
        settings_file = 'reals3_csv_example_learned_settings'
        training_file = 'reals3_csv_example_training.json'
        fieldNameIdCol = fieldNameIdInSource

    mappings_file = 'Mappings.csv'
    scriptpath = os.path.dirname(__file__)
    #output_file = os.path.join(scriptpath, output_file)
    settings_file = os.path.join(scriptpath, settings_file)
    training_file = os.path.join(scriptpath, training_file)
    mappings_file = os.path.join(scriptpath, mappings_file)



    header_map = {}
    header_map.update(readMappings(mappings_file))

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
        #csv_header = 'Id,Source,Site name,Address,Zip,Phone,Fax,Program Name,Length of Day,IDHS Provider ID,Agency,Neighborhood,Funded Enrollment,Program Option,Number per Site EHS,Number per Site HS,Director,Head Start Fund,Eearly Head Start Fund,CC fund,Progmod,Website,Executive Director,Center Director,ECE Available Programs,NAEYC Valid Until,NAEYC Program Id,Email Address,Ounce of Prevention Description,Purple binder service type,Column,Column2'
    else: #FileSource = "comebackkc1"
        s3files.append('C:/Users/robkr/Downloads/ResponseExport-KCRegionalCOVID19VaccinationSurvey-20210323 (1)/ResponseExport-KCRegionalCOVID19VaccinationSurvey-20210323.csv')
        #csv_header = 'Receipt Number,Response Reference ID,Respondent Email,URL submitted from,Form version submitted in,Response Submission DateTime,Time Taken To Complete (seconds),External ID,External Status,Are you planning to receive the COVID-19 vaccine?,What makes you uncertain about receiving the COVID-19 vaccine?,When would you like to get your first dose of the COVID-19 vaccine?,What state do you live in?,Which Kansas county do you live in?,"Do you live within the city limits of Kansas City, MO?",Which Missouri county do you live in?,What is your zip code?,First Name,Last Name,Street Address,City,"Phone number (please enter numbers only, no dashes, spaces, or parentheses)",Email address,What is your preferred method of contact?,We need your permission to contact you about COVID-19 testing and vaccination.,Is it okay for us to leave you a voicemail?,Sex,Age,Race/ethnicity (check as many as apply),"Do you have any pre-existing medical conditions or do you have any conditions that put you at increased risk of severe illness? (i.e. immunocompromised, diabetes, chronic lung conditions, cardiovascular disease, morbid obesity, etc.)","Do you live in or visit often crowded living settings? (For example, a supportive care facility, assisted living facility, group home, homeless shelter, or correctional setting)","How many members, including yourself, live in your household?",Do you have a history of any of the following pre-existing medical conditions?,Are you immuno-compromised?,What is your work status?,"What is your work zip code, either where you normally work in the office or on location or the zip code of your employers primary office?",Are you employed by any of the following types of patient-facing organizations?,What is the name of the health care provider for which you work?,Are you responsible for or in a position to influence vaccination planning for your employer or another organization?,What is the name of your employer or other organization?,What is your job title or role at your organization?'

    csv_out = 'C:/Users/robkr/Source/Repos/dedupe-examples/s3_csv_example/combinedfile.csv'


    fileInfos = { "f":[]}
    fileInfo = {0: []}
    fileInfo["source"]=""
    fileInfo["num"]=0
    fileInfos["f"].append(fileInfo)

    fileprefix = 'a_'
#4Start############# This ugly section exists to remove the extra header that is in some csv files #################
    fileno = 1 #Used to make the key independent per record when we combine multiple input files
    count = -1
    #replacing Receipt Number with IDinSource in this file!!!
    #TODO - Read in Headers and do a replace of header names based on the mappings.
    for file in s3files:
        firstpos=file.rfind("/")
        lastpos=len(file)
        filenameonly = file[firstpos+1:lastpos]
        if filenameonly.startswith("Response"):
            FileSource="comebackkc1"
            fileInfo = {fileno: []}
            fileInfo["source"]=FileSource
            fileInfo["num"]=fileno
            fileInfo["unq"]=header_map[FileSource]["Unique ID"]
            fileInfos["f"].append(fileInfo)
        csv_stripextraheader = open(fileprefix + filenameonly, 'w')
        csv_in = open(file,"r",encoding='utf-8') #https://stackoverflow.com/questions/49562499/how-to-fix-unicodedecodeerror-charmap-codec-cant-decode-byte-0x9d-in-posit
        for line in csv_in:
            if (FileSource=="comebackkc1") and (count < 0): #line.startswith(csv_header):
                count = count + 1 #skip this line - remove it from the rewritten file
                continue
            csv_stripextraheader.write(line)
            print(count)
            count = count + 1
        csv_in.close()
        fileno = fileno + 1
    csv_stripextraheader.close()
#4End############# This ugly section exists to remove the extra header that is in some csv files #################

#5Start############# Combine multiple input files into one single file with consistent column headers #################
    fileno = 1 #Used to make the key independent per record when we combine multiple input files
    count = 0
    csv_merge = open(csv_out, 'w', newline='')
    #replacing Receipt Number with IDinSource in this file!!!
    #TODO - Read in Headers and do a replace of header names based on the mappings.
    #csv_headerNew = fieldNameIdInSource + ',Response Reference ID,Respondent Email,URL submitted from,Form version submitted in,Response Submission DateTime,Time Taken To Complete (seconds),External ID,External Status,Are you planning to receive the COVID-19 vaccine?,What makes you uncertain about receiving the COVID-19 vaccine?,When would you like to get your first dose of the COVID-19 vaccine?,What state do you live in?,Which Kansas county do you live in?,"Do you live within the city limits of Kansas City, MO?",Which Missouri county do you live in?,Zip,FirstName,LastName,Street Address,City,"Phone",Email,What is your preferred method of contact?,We need your permission to contact you about COVID-19 testing and vaccination.,Is it okay for us to leave you a voicemail?,Sex,Age,Race/ethnicity (check as many as apply),"Do you have any pre-existing medical conditions or do you have any conditions that put you at increased risk of severe illness? (i.e. immunocompromised, diabetes, chronic lung conditions, cardiovascular disease, morbid obesity, etc.)","Do you live in or visit often crowded living settings? (For example, a supportive care facility, assisted living facility, group home, homeless shelter, or correctional setting)","How many members, including yourself, live in your household?",Do you have a history of any of the following pre-existing medical conditions?,Are you immuno-compromised?,What is your work status?,"What is your work zip code, either where you normally work in the office or on location or the zip code of your employers primary office?",Are you employed by any of the following types of patient-facing organizations?,What is the name of the health care provider for which you work?,Are you responsible for or in a position to influence vaccination planning for your employer or another organization?,What is the name of your employer or other organization?,What is your job title or role at your organization?'
    csv_headerNew = 'FirstName,LastName,Email,City,Phone,Zip,Unique ID,Key'
    csv_merge.write(fieldNameFileName + ',' + fieldNameFileNo + ',' + csv_headerNew)
    csv_merge.write('\n')
    for file in s3files:
        firstpos=file.rfind("/")
        lastpos=len(file)
        filenameonly = file[firstpos+1:lastpos]
        if filenameonly.startswith("Response"):
            FileSource="comebackkc1"
        #https://stackoverflow.com/questions/49562499/how-to-fix-unicodedecodeerror-charmap-codec-cant-decode-byte-0x9d-in-posit
        #I had sever 0x92 - https://stackoverflow.com/questions/37083687/unicodedecodeerror-ascii-codec-cant-decode-byte-0x92
        with open(fileprefix + filenameonly,"r",encoding='windows-1252') as f_input:
            reader = csv.DictReader(f_input, dialect='excel')
            for row in reader:
                print(count)
                count = count + 1
                outrow = (filenameonly + ',' + str(fileno) + ',"' + row[header_map[FileSource]["FirstName"]]
                + '","' + row[header_map[FileSource]["LastName"]] + '",' + row[header_map[FileSource]["Email"]]
                + ',"' + row[header_map[FileSource]["City"]] + '",' + row[header_map[FileSource]["Phone"]]
                + ',' + row[header_map[FileSource]["Zip"]] + ',' + row[header_map[FileSource]["Unique ID"]]
                + ',' + row[header_map[FileSource]["Key"]] + '\n')
                print(outrow)
                csv_merge.write(outrow)
        fileno = fileno + 1
    csv_merge.close()
#5End############# Combine multiple input files into one single file with consistent column headers #################


    #results = pd.DataFrame([])
#https://www.techbeamers.com/merge-multiple-csv-files/
    #csv_header = 'Receipt Number,Response Reference ID,Respondent Email,URL submitted from,Form version submitted in,Response Submission DateTime,Time Taken To Complete (seconds),External ID,External Status,Are you planning to receive the COVID-19 vaccine?,What makes you uncertain about receiving the COVID-19 vaccine?,When would you like to get your first dose of the COVID-19 vaccine?,What state do you live in?,Which Kansas county do you live in?,"Do you live within the city limits of Kansas City, MO?",Which Missouri county do you live in?,What is your zip code?,First Name,Last Name,Street Address,City,"Phone number (please enter numbers only, no dashes, spaces, or parentheses)",Email address,What is your preferred method of contact?,We need your permission to contact you about COVID-19 testing and vaccination.,Is it okay for us to leave you a voicemail?,Sex,Age,Race/ethnicity (check as many as apply),"Do you have any pre-existing medical conditions or do you have any conditions that put you at increased risk of severe illness? (i.e. immunocompromised, diabetes, chronic lung conditions, cardiovascular disease, morbid obesity, etc.)","Do you live in or visit often crowded living settings? (For example, a supportive care facility, assisted living facility, group home, homeless shelter, or correctional setting)","How many members, including yourself, live in your household?",Do you have a history of any of the following pre-existing medical conditions?,Are you immuno-compromised?,What is your work status?,"What is your work zip code, either where you normally work in the office or on location or the zip code of your employers primary office?",Are you employed by any of the following types of patient-facing organizations?,What is the name of the health care provider for which you work?,Are you responsible for or in a position to influence vaccination planning for your employer or another organization?,What is the name of your employer or other organization?,What is your job title or role at your organization?'


    print('importing data ...')
    data_d = {}
    data_d.update(readData(csv_out))
#    for eachFile in s3files:
 #       data_d.update(readData(eachFile, idcol))

    if not data_d:
        print('no files found to process in s3 bucket named: ' + bucket)
        os._exit(1)

    fieldNameSourceFileUniqueId=''
    if FileSource == "dedupesample":
        outputfieldnames = reader.fieldnames
    else: #FileSource = "comebackkc1"
        fieldNameSourceFileUniqueId = header_map[FileSource]["Unique ID"] #this is probably different for each file - a GUID in some cases
        outputfieldnames = [fieldNameFileName,fieldNameFileNo,fieldNameIdInSource,fieldNameSourceFileUniqueId,'FirstName', 'LastName','Email','City','Phone','Zip']
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
                fieldNameClusterId: cluster_id,
                fieldNameConfidence: score
            }

    with open(output_file, 'w') as f_output, open(csv_out) as f_input:

        reader = csv.DictReader(f_input)
        fieldnames = [fieldNameClusterId, fieldNameConfidence] + outputfieldnames 
        writer = csv.DictWriter(f_output, fieldnames=fieldnames)
        writer.writeheader()

        for row in reader:
            row_id = str(int(row[fieldNameFileNo])) + '.' + str(int(row[fieldNameIdCol]))
            row.update(cluster_membership[row_id])
            
                        #   newrow = {fieldNameFileName: filenameonly, fieldNameFileNo: str(fileno),
                        # 'Unique ID': row[header_map[agency]["Unique ID"]],
                        #'Key':row[header_map[agency]["Key"]],
                        #'FirstName': row[header_map[agency]["FirstName"]],
                        #'LastName': row[header_map[agency]["LastName"]],
                        # 'City': row[header_map[agency]["City"]],
                        #'Zip': row[header_map[agency]["Zip"]],
                        # 'Phone': row[header_map[agency]["Phone"]],
                        #'Email': row[header_map[agency]["Email"]]}


            newrow = {fieldNameFileName: row[fieldNameFileName], fieldNameFileNo: row[fieldNameFileNo],
                     fieldNameIdInSource: row[fieldNameIdInSource], fieldNameSourceFileUniqueId:row["Unique ID"],
                    'FirstName': row['FirstName'], 'LastName': row['LastName']
                     , 'City': row['City'], 'Zip': row['Zip']
                     , 'Phone': row['Phone'], 'Email': row['Email']
                     , fieldNameClusterId: row[fieldNameClusterId], fieldNameConfidence: row[fieldNameConfidence]}
            writer.writerow(newrow)

#https://www.usepandas.com/csv/sort-csv-data-by-column
    df = pd.read_csv(output_file)
    sorted_df = df.sort_values(by=["ConfidenceScore","ClusterId"], ascending=[True,True])
    sorted_df.to_csv('sorted' + output_file, index=False)
    writeToS3Bucket(output_file, output_bucket, s3output_file)

    #C:\Users\Administrator\AppData\Local\Programs\Python\Python39\python s3_csv_example.py c4kc-cvax-deduplication c4kc-cvax-deduplication

    #data sort, by confidence id smallest to largest then cluster id
    