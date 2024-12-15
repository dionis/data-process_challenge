import os
from datetime import datetime, timedelta
import requests
import pandas as pd
import boto3
import botocore

EXTERNAL_URL_CSV = 'https://www.stats.govt.nz/assets/Uploads/Balance-of-payments/Balance-of-payments-and-international-investment-position-June-2024-quarter/Download-data/balance-of-payments-and-international-investment-position-june-2024-quarter.csv'

BASE_FILENAME = 'csv_processing'

AWS_BUCKET_NAME = 'geekscastle-challenge'

URL_NOT_DEFINED = 'Url not defined'

ADDRESS_TO_SAVE_NOT_DEFINED = 'Address to save in local directories not defined'

ERROR_UPLOAD_FILE_ADDRESS = 'Error upload file address not defined'

FAILED_DOWNLOAD_CSV = 'Failed to download CSV file. Status code.'

FAILED_NOT_EXIST_ADDRESS_TO_SAVE = 'Not exist address to save in local directories'

ARN_SNS_URL_IN_AWS = "arn:aws:sns:us-east-2:694619293848:data_engineer_email_topic"

def download_csv_file(url, save_address, file_name):
    try:
        if  not url or url == '':
            raise Exception(URL_NOT_DEFINED)
        elif not save_address or save_address == '':
            raise Exception(ADDRESS_TO_SAVE_NOT_DEFINED)

        response = requests.get(url)

        if response.status_code == 200:
            # Save the content of the response to a local CSV file
            suffix = datetime.now().strftime("%y%m%d_%H%M%S")
            filename = "_".join([file_name, suffix])

            filename = filename.replace('.csv', '')
            file_address = save_address + os.sep + filename + '.csv'

            if os.path.exists(file_address):
                os.remove(file_address)

            with open(file_address, "wb") as f:
                f.write(response.content)

                print("CSV file downloaded successfully")

                # Read the CSV file into a Pandas DataFrame
                df = pd.read_csv(file_address)

                return file_address
        else:
            print(FAILED_DOWNLOAD_CSV, response.status_code)



    except:
        raise Exception("Cannot read CSV file")


def upload_to_aws_s3(buket_name, file_address):

    if not os.path.exists(file_address):
        raise Exception(ERROR_UPLOAD_FILE_ADDRESS)
    else:
        # Upload a new file
        with open(file_address, 'rb') as data:

            file_name = os.path.basename(file_address)
            s3.Bucket(buket_name).put_object(Key = file_name, Body = data)

def download_file_from_s3(buket_name, key, file_address_to_save):
    if not os.path.exists(file_address_to_save):
        raise Exception(FAILED_NOT_EXIST_ADDRESS_TO_SAVE)
    else:
        try:
            s3.download_file(buket_name, key, file_address_to_save + os.sep + key)
        except:
            print(FAILED_DOWNLOAD_CSV)
            #if e.response['Error']['Code'] == "404":
                #print(FAILED_DOWNLOAD_CSV)
            #else:
             #   raise


def  aws_sns_notification(message, title, sns_address):
    mysns = boto3.client("sns")

    print(sns_address)

    mysns.publish(
        TopicArn = sns_address,
        Message = message,
        Subject = title,
    )
if __name__ == "__main__":
  # Let's use Amazon S3
  s3 = boto3.resource('s3')

  # Print out bucket names
  for bucket in s3.buckets.all():
      print(bucket.name)

  #Download from url
  #downloaded_filename = download_csv_file(EXTERNAL_URL_CSV, './datasets', BASE_FILENAME)

  #Upload to S3
  #upload_to_aws_s3(AWS_BUCKET_NAME, 'datasets/csv_processing_241214_222149.csv')

  #Download from S3
  download_file_from_s3(AWS_BUCKET_NAME, 'csv_processing_241214_222149.csv  ', './datasets')

  aws_sns_notification("Saved result in Redshift's table ",'Redshift saved result', ARN_SNS_URL_IN_AWS)


