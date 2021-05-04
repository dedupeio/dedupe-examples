#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
This script identifies duplicate signups.
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
    if column == None:
        return column
    try:
        column = unidecode(column)
        column = re.sub('  +', ' ', column)
        column = re.sub('\n', ' ', column)
        column = column.strip().strip('"').strip("'").lower().strip()
    except:
        return column
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
            fldSource = str(row[fieldNameSource])
            clean_row = [(k, preProcessCase(v)) for (k, v) in row.items()]
            data_d[fldSource] = dict(clean_row)

    return data_d
def writeToLog(message, error):
    csv_log = open(log_file, 'a+', newline='')
#    csv_logHeader = 'DateTime,Message,Error'
    #csv_log.write(csv_logHeader)
    csv_log.write(time.strftime("%Y.%m.%d %H:%M.%S") + ',' + message)
    csv_log.write('\n')
    print(message + ' at ' + time.strftime("%Y.%m.%d %H:%M.%S"))
    if error != '':
        csv_err = open(errors_file, 'a+', newline='')
        csv_err.write(time.strftime("%Y.%m.%d %H:%M.%S") + ',' + message + ',' + error)
        csv_err.write('\n')
        print(message + ': ' + error + ' at ' + time.strftime("%Y.%m.%d %H:%M.%S"))


def readData(filename):
    """
    Read in our data from a CSV file and create a dictionary of records,
    where the key is a unique record ID and each value is dict
    """
    data_d = {}
    with open(filename) as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                clean_row = [(k, preProcess(v)) for (k, v) in row.items()]
                if row['Key'] != None:
                    row_id = str(int(row[fieldNameFileNo])) + '.' + str(int(row[fieldNameIdCol]))
                    data_d[row_id] = dict(clean_row)
            except:
                print(row)
    return data_d

def writeToS3Bucket( output_bucket):
    s3 = boto3.resource('s3')
    for filen in fileInfos["f"]:
        ss = filen['source']
        if ss == '':
            continue
        agencyFilePrefix = ss + '.'
        agency_sorted_file = os.path.join(scriptpath, agencyFilePrefix + 'sorted.' + output_file_base)
        s3.meta.client.upload_file(agency_sorted_file, output_bucket, 'output/' + agencyFilePrefix + 'sorted.' + output_file_base)
        os.remove(agency_sorted_file)


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

    # 1. Setup
    import time
    timestr = time.strftime(".%Y.%m.%d-%H.%M.%S")
    
    fieldNameFileNo = 'FileNo' #not dynamic - we totally control this
    fieldNameFileName = 'FileName' #not dynamic - we totally control this
    fieldNameIdInSource = 'Key' #not dynamic - we totally control this
    fieldNameIdCol = fieldNameIdInSource
    fieldNameClusterId = 'ClusterId' #not dynamic - we totally control this 
    fieldNameConfidence = 'ConfidenceScore' #not dynamic - we totally control this 
    fieldNameSource = 'Source' #not dynamic - we totally control this 
    AppFileSource = "s3" #s3 or local
    log_file = 'logfile.csv'
    errors_file = 'errors.csv'
    mappings_file = 'Mappings.csv'
    settings_file = 'Duplicate_Vaccination_Signups_learned_settings'
    training_file = 'Duplicate_Vaccination_Signups_training.json'
    combined_file = 'combinedfile.csv'
    output_file_base = 'Duplicate_Vaccination_Signups' + timestr + '.csv'
    outputsm_file = 'sm.' + output_file_base
    outputsorted_file = 'sorted.' + output_file_base
    s3output_file = 'output/' + output_file_base
    bucket='c4kc-cvax-deduplication' #bucket name could be overridden by value passed in below

    scriptpath = os.path.dirname(__file__)
    log_file = os.path.join(scriptpath, log_file)
    errors_file = os.path.join(scriptpath, errors_file)
    mappings_file = os.path.join(scriptpath, mappings_file)
    settings_file = os.path.join(scriptpath, settings_file)
    training_file = os.path.join(scriptpath, training_file)
    combined_file = os.path.join(scriptpath, combined_file)
    output_file = os.path.join(scriptpath, output_file_base)
    outputsm_file = os.path.join(scriptpath, outputsm_file)
    outputsorted_file = os.path.join(scriptpath, outputsorted_file)

    writeToLog('Pgm','Start')
    #print('1. Combining Files at' + time.strftime(".%Y.%m.%d-%H.%M.%S") + '  ...') 
    writeToLog('1.0 Combining Files','')



    #2. Load all file mappings metadata
    #print('2. Loading headers at' + time.strftime(".%Y.%m.%d-%H.%M.%S") + '  ...')  
    writeToLog('2.0 Loading headers','')
    header_map = {}
    header_map.update(readMappings(mappings_file))

    #3. Import files to working area (aka folders local to python script on hard drive)
    #print('3. Importing files to python script directories at' + time.strftime(".%Y.%m.%d-%H.%M.%S") + '  ...')  
    writeToLog('3.0 Importing files to python script directories','')
    import sys
    if len(sys.argv) > 1:
        bucket = sys.argv[1]
    if len(sys.argv) > 2:
        output_bucket = sys.argv[2]
    if len(sys.argv) > 3:
        AppFileSource = sys.argv[3]
    output_bucket=bucket
    s3files = []

    import boto3
    s3_client = boto3.client('s3')
    if AppFileSource == "s3":
        result = s3_client.list_objects(Bucket = bucket, Prefix='')
        for o in result.get('Contents'):
            filename = o.get('Key');
            if filename[:7] != 'output/': #don't process files that are in the /output path of the s3 bucket
                data = s3_client.get_object(Bucket=bucket, Key=filename)
                if filename[:6] == 'input/' and len(filename) > 6:
                    print(filename)
                    if len(filename) > filename.rfind("/")+1:
                        local_file = filename[6:].replace("/",".")  #remove input/ from the name of the file
                        local_file = os.path.join(scriptpath, local_file)
                        writeToLog('3.5 Downloading ' + filename + ' from s3 to ' + local_file,'')
                        s3_client.download_file(bucket,filename,local_file)
                        s3files.append(local_file)
                        response = s3_client.delete_object(Bucket=bucket,Key=filename)
    else: #AppFileSource = "local"
        #s3files.append('C:/Users/robkr/Downloads/ResponseExport-KCRegionalCOVID19VaccinationSurvey-20210323 (1)/ResponseExportComeBackKC1.csv')
        s3files.append('C:/Users/robkr/Downloads/ResponseExport-KCRegionalCOVID19VaccinationSurvey-20210323 (1)/SAFE01X.csv')

#4. Pre-Process Files- This ugly section exists to remove the extra header that is in some csv files #################
    #print('4. Pre-processing files at' + time.strftime(".%Y.%m.%d-%H.%M.%S") + '  ...')  
    writeToLog('4.0 Pre-processing files','')

    fileInfos = { "f":[]}
    fileInfo = {0: []}
    fileInfo["source"]=""
    fileInfo["num"]=0
    fileInfos["f"].append(fileInfo)

    fileprefix = 'pre_'
    fileno = 1 #Used to make the key independent per record when we combine multiple input files
    #replacing Receipt Number with IDinSource in this file!!!
    #TODO - Read in Headers and do a replace of header names based on the mappings.
    for file in s3files:
        count = -1
        firstpos=file.rfind("\\") #not sure why this works differently on deployed site
        lastpos=len(file)
        filenameonly = file[firstpos+1:lastpos]
        writeToLog('4.5 Processing file ' + filenameonly,'')
        if filenameonly.startswith("Response"):
            FileSource="comebackkc1"
            fileInfo = {fileno: []}
            fileInfo["source"]=FileSource
            fileInfo["num"]=fileno
            fileInfo["unq"]=header_map[FileSource]["Unique ID"]
            fileInfos["f"].append(fileInfo)
        if filenameonly.startswith("CBKC01"):
            FileSource="CBKC01"
            fileInfo = {fileno: []}
            fileInfo["source"]=FileSource
            fileInfo["num"]=fileno
            fileInfo["unq"]=header_map[FileSource]["Unique ID"]
            fileInfos["f"].append(fileInfo)
        if filenameonly.startswith("SAFE01"):
            FileSource="SAFE01"
            fileInfo = {fileno: []}
            fileInfo["source"]=FileSource
            fileInfo["num"]=fileno
            fileInfo["unq"]=header_map[FileSource]["Unique ID"]
            fileInfos["f"].append(fileInfo)  
        step4_file = os.path.join(scriptpath, fileprefix + filenameonly)
        csv_stripextraheader = open(step4_file, 'w')
        csv_in = open(file,"r",encoding='utf-8') #https://stackoverflow.com/questions/49562499/how-to-fix-unicodedecodeerror-charmap-codec-cant-decode-byte-0x9d-in-posit
        for line in csv_in:
            if (FileSource=="comebackkc1") and (count < 0): #line.startswith(csv_header):
                count = count + 1 #skip this line - remove it from the rewritten file
                continue
            if (FileSource=="CBKC01") and (count < 0): #line.startswith(csv_header):
                count = count + 1 #skip this line - remove it from the rewritten file
                continue
            try:
                csv_stripextraheader.write(line)
            except UnicodeEncodeError as e:
                #print(line) - argh - print doesn't work on unattended script
                writeToLog('UnicodeEncodeError', 'record ' + str(count) +  ': ' + str(e))
            #print(count)
            count = count + 1
        csv_in.close()
        #os.remove(file)
        fileno = fileno + 1
    csv_stripextraheader.close()
#4End############# This ugly section exists to remove the extra header that is in some csv files #################

#5. Combine the input files into one .csv files with consistent column headers #################
    #print('5. Combining files at' + time.strftime(".%Y.%m.%d-%H.%M.%S") + '  ...')  
    writeToLog('5.0 Combining files','')

    fileno = 1 #Used to make the key independent per record when we combine multiple input files
    count = 0
    csv_merge = open(combined_file, 'w', newline='')
    #TODO Unique ID and Key
    csv_headerNew = 'FirstName,LastName,Email,City,Phone,Zip,Unique ID,Key,' + fieldNameSource
    csv_merge.write(fieldNameFileName + ',' + fieldNameFileNo + ',' + csv_headerNew)
    csv_merge.write('\n')
    for file in s3files:
        firstpos=file.rfind("\\") #argh
        lastpos=len(file)
        filenameonly = file[firstpos+1:lastpos]
        if filenameonly.startswith("Response"): #TODO - Can we map this?
            FileSource="comebackkc1"
        if filenameonly.startswith("CBKC01"):
            FileSource="CBKC01"
        if filenameonly.startswith("SAFE01"):
            FileSource="SAFE01"
        #https://stackoverflow.com/questions/49562499/how-to-fix-unicodedecodeerror-charmap-codec-cant-decode-byte-0x9d-in-posit
        #I had sever 0x92 - https://stackoverflow.com/questions/37083687/unicodedecodeerror-ascii-codec-cant-decode-byte-0x92
        step4_file = os.path.join(scriptpath, fileprefix + filenameonly)
        with open(step4_file,"r",encoding='windows-1252') as f_input:
            reader = csv.DictReader(f_input, dialect='excel')
            for row in reader:
                #print(count)
                count = count + 1
                try:
                    outrow = (filenameonly + ',' + str(fileno) + ',"' + row[header_map[FileSource]["FirstName"]]
                    + '","' + row[header_map[FileSource]["LastName"]] + '",' + row[header_map[FileSource]["Email"]]
                    + ',"' + row[header_map[FileSource]["City"]] + '",' + row[header_map[FileSource]["Phone"]]
                    + ',' + row[header_map[FileSource]["Zip"]] + ',' + row[header_map[FileSource]["Unique ID"]]
                    + ',' + row[header_map[FileSource]["Key"]] + ',' + FileSource + '\n')
                    #print(outrow)
                    csv_merge.write(outrow)
                except UnicodeEncodeError as e:
                    print(row)
                    writeToLog('UnicodeEncodeError', 'record ' + str(count) +  ': ' + str(e))
        fileno = fileno + 1
        os.remove(step4_file)
    csv_merge.close()
#5End############# Combine multiple input files into one single file with consistent column headers #################


    #results = pd.DataFrame([])
#https://www.techbeamers.com/merge-multiple-csv-files/
    #csv_header = 'Receipt Number,Response Reference ID,Respondent Email,URL submitted from,Form version submitted in,Response Submission DateTime,Time Taken To Complete (seconds),External ID,External Status,Are you planning to receive the COVID-19 vaccine?,What makes you uncertain about receiving the COVID-19 vaccine?,When would you like to get your first dose of the COVID-19 vaccine?,What state do you live in?,Which Kansas county do you live in?,"Do you live within the city limits of Kansas City, MO?",Which Missouri county do you live in?,What is your zip code?,First Name,Last Name,Street Address,City,"Phone number (please enter numbers only, no dashes, spaces, or parentheses)",Email address,What is your preferred method of contact?,We need your permission to contact you about COVID-19 testing and vaccination.,Is it okay for us to leave you a voicemail?,Sex,Age,Race/ethnicity (check as many as apply),"Do you have any pre-existing medical conditions or do you have any conditions that put you at increased risk of severe illness? (i.e. immunocompromised, diabetes, chronic lung conditions, cardiovascular disease, morbid obesity, etc.)","Do you live in or visit often crowded living settings? (For example, a supportive care facility, assisted living facility, group home, homeless shelter, or correctional setting)","How many members, including yourself, live in your household?",Do you have a history of any of the following pre-existing medical conditions?,Are you immuno-compromised?,What is your work status?,"What is your work zip code, either where you normally work in the office or on location or the zip code of your employers primary office?",Are you employed by any of the following types of patient-facing organizations?,What is the name of the health care provider for which you work?,Are you responsible for or in a position to influence vaccination planning for your employer or another organization?,What is the name of your employer or other organization?,What is your job title or role at your organization?'

#6. Read the data from the combined file into our DataFrame for deduping
    #print('6. Importing data for Deduping at' + time.strftime(".%Y.%m.%d-%H.%M.%S") + '  ...') 
    writeToLog('6.0 Importing data for Deduping','')

    data_d = {}
    data_d.update(readData(combined_file))
#    for eachFile in s3files:
 #       data_d.update(readData(eachFile, idcol))

    if not data_d:
        print('no files found to process in s3 bucket named: ' + bucket)
        os._exit(1)

    fieldNameSourceFileUniqueId=''
    fieldNameSourceFileUniqueId = header_map[FileSource]["Unique ID"] #this is probably different for each file - a GUID in some cases
    outputfieldnames = [fieldNameFileName,fieldNameFileNo,fieldNameIdInSource,fieldNameSourceFileUniqueId,'FirstName', 'LastName','Email','City','Phone','Zip','Source']
    # If a settings file already exists, we'll just load that and skip training
    if os.path.exists(settings_file):
        print('reading from', settings_file)
        with open(settings_file, 'rb') as f:
            deduper = dedupe.StaticDedupe(f)
    else:
        # ## Training

        # Define the fields dedupe will pay attention to
        fields = [
            {'field': 'Email', 'type': 'Exact', 'has missing': True},
            {'field': 'Zip', 'type': 'String'},
            {'field': 'FirstName', 'type': 'String'},
            {'field': 'LastName', 'type': 'String'},
            {'field': 'City', 'type': 'String'},
            {'field': 'Zip', 'type': 'Exact', 'has missing': True},
            {'field': 'Phone', 'type': 'String', 'has missing': True},]

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
        #print('starting active labeling...')
        writeToLog('6.3 starting active labeling...','')

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

    #print('clustering...' + time.strftime(".%Y.%m.%d-%H.%M.%S") + '  ...')
    writeToLog('6.5 clustering...','')
    clustered_dupes = deduper.partition(data_d, 0.5)

    #print('# duplicate sets', len(clustered_dupes))
    writeToLog('6.8 # duplicate sets...','')
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

#7. Create new output files with the clustered information.  One file of just dups, another file with all data
    #print('7. Create output files with clustered info at' + time.strftime(".%Y.%m.%d-%H.%M.%S") + '  ...') 
    writeToLog('7.0 Create output files with clustered info','')

    with open(output_file, 'w') as f_output, open(combined_file) as f_input,open(outputsm_file, 'w') as f_outsm:

        reader = csv.DictReader(f_input)
        fieldnames = [fieldNameClusterId, fieldNameConfidence] + outputfieldnames 
        writer = csv.DictWriter(f_output, fieldnames=fieldnames)
        writer.writeheader()
        writersm = csv.DictWriter(f_outsm, fieldnames=fieldnames)
        writersm.writeheader()
        count = 0
        for row in reader:
            count = count +1
            try:
                row_id = str(int(row[fieldNameFileNo])) + '.' + str(int(row[fieldNameIdCol]))
                row.update(cluster_membership[row_id])
                newrow = {fieldNameFileName: row[fieldNameFileName], fieldNameFileNo: row[fieldNameFileNo],
                         fieldNameIdInSource: row[fieldNameIdInSource], fieldNameSourceFileUniqueId:row["Unique ID"],
                        'FirstName': row['FirstName'], 'LastName': row['LastName']
                         , 'City': row['City'], 'Zip': row['Zip']
                         , 'Phone': row['Phone'], 'Email': row['Email'], fieldNameSource: row[fieldNameSource]
                         , fieldNameClusterId: row[fieldNameClusterId], fieldNameConfidence: row[fieldNameConfidence]}
                writer.writerow(newrow)
                if row[fieldNameConfidence] < 1 and row[fieldNameConfidence] > .75:
                    writersm.writerow(newrow)
            except Exception as e:
                print (row)
                writeToLog('Error', 'record ' + str(count) +  ': ' + str(e))
    os.remove(combined_file)
#8. Sort the single output file that has cluster info
    #print('8. Sorting files at' + time.strftime(".%Y.%m.%d-%H.%M.%S") + '  ...')  
    writeToLog('8.0 Sorting files','')

#https://www.usepandas.com/csv/sort-csv-data-by-column
    df = pd.read_csv(outputsm_file)
    sorted_df = df.sort_values(by=[fieldNameClusterId,"ConfidenceScore"], ascending=[True,True])
    sorted_df.to_csv(outputsorted_file, index=False)
    os.remove(output_file)
    os.remove(outputsm_file)

#9. Split the output file into output files per agency and sort it
    #print('9. Split files per agency at' + time.strftime(".%Y.%m.%d-%H.%M.%S") + '  ...') 
    writeToLog('9.0 Split files per agency','')

    df = pd.read_csv(outputsorted_file)
    for filen in fileInfos["f"]:
        ss = filen['source']
        if ss == '':
            continue
        agencyFilePrefix = ss + '.'
        gg = df.groupby(fieldNameSource).get_group(ss).ClusterId.values
        linesInFile=0
        agency_file = os.path.join(scriptpath, agencyFilePrefix + output_file_base)
        agency_sorted_file = os.path.join(scriptpath, agencyFilePrefix + 'sorted.' + output_file_base)
        with open(agency_file, 'w') as a_out, open(outputsorted_file) as f_input:
            reader = csv.DictReader(f_input)
            fieldnames = [fieldNameClusterId, fieldNameConfidence] + outputfieldnames 
            writer = csv.DictWriter(a_out, fieldnames=fieldnames)
            writer.writeheader()
            for row in reader:
                if int(row[fieldNameClusterId]) in gg:
                    writer.writerow(row)
                    linesInFile = linesInFile+1
            a_out.close()
            df2 = pd.read_csv(agency_file)
            sorted_df = df2.sort_values(by=[fieldNameClusterId,"ConfidenceScore"], ascending=[True,True])
            sorted_df.to_csv(agency_sorted_file, index=False)
            os.remove(agency_file)
            writeToLog('9.8 ' + str(linesInFile) + ' records written to ' + agency_sorted_file ,'')
    os.remove(outputsorted_file)
#10 
    #print('10. Send files to s3 at' + time.strftime(".%Y.%m.%d-%H.%M.%S") + '  ...')  
    writeToLog('9.9 Send files to s3','')
    writeToS3Bucket( output_bucket)

    #C:\Users\Administrator\AppData\Local\Programs\Python\Python39\python s3_csv_example.py c4kc-cvax-deduplication c4kc-cvax-deduplication

    #data sort, by confidence id smallest to largest then cluster id
    