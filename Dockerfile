FROM python:3

ADD KCDigitalDrive/KCDigitalDrive_Vacc.py /

RUN pip install unidecode
RUN pip install pandas
RUN pip install dedupe
RUN pip install boto3

COPY KCDigitalDrive/Mappings.csv ./
COPY SAFE01TestData.csv ./
COPY KCDigitalDrive/Duplicate_Vaccination_Signups_learned_settings ./
COPY KCDigitalDrive/Duplicate_Vaccination_Signups_training.json ./


CMD ["python", "./KCDigitalDrive_Vacc.py", "wildrydes-rob-kraft", "pp4ncaliftwo", "local"]