import os
from datetime import datetime, timedelta
import requests
import pandas as pd
import boto3
import botocore.exceptions
import logging
import time
from os import environ as env
from dotenv import load_dotenv,find_dotenv

load_dotenv('config/.env')

#EXTERNAL_URL_CSV = 'https://www.stats.govt.nz/assets/Uploads/Balance-of-payments/Balance-of-payments-and-international-investment-position-June-2024-quarter/Download-data/balance-of-payments-and-international-investment-position-june-2024-quarter.csv'

BASE_FILENAME = 'csv_processing'

#AWS_BUCKET_NAME = 'geekscastle-challenge'

URL_NOT_DEFINED = 'Url not defined'

ADDRESS_TO_SAVE_NOT_DEFINED = 'Address to save in local directories not defined'

ERROR_UPLOAD_FILE_ADDRESS = 'Error upload file address not defined'

FAILED_DOWNLOAD_CSV = 'Failed to download CSV file. Status code.'

FAILED_NOT_EXIST_ADDRESS_TO_SAVE = 'Not exist address to save in local directories'

#ARN_SNS_URL_IN_AWS = "arn:aws:sns:us-east-2:694619293848:data_engineer_email_topic"

TEMP_FILE_PREFIX   = 'redshift_data_upload' #Temporary file prefix
REDSHIFT_WORKGROUP = ''
REDSHIFT_DATABASE  = 'dev' #default "dev"
REDSHIFT_DBUSER = 'awsuser'
REDSHIFT_CLUSTER_IDENTIFIER = 'redshift-geeks-castle-challenge'
MAX_WAIT_CYCLES = 5

DATASETS = 'datasets'


def aws_sns_notification(message, title, sns_address):
    mysns = boto3.client("sns")

    print(sns_address)

    mysns.publish(
        TopicArn=sns_address,
        Message=message,
        Subject=title,
    )

class S3Aws():

    def __init__(self):
        try:
            self.s3 = boto3.resource('s3')
        except botocore.exceptions.ClientError as e:
            logging.error(e)

    def create_random_filename(self, basename):
        if basename is not None:
            suffix = datetime.now().strftime("%y%m%d_%H%M%S")
            filename = "_".join([basename, suffix])
            filename = filename.replace('.csv', '')

            return filename
        else: raise Exception('Not basename defined')

    def download_csv_file(self, url, save_address, file_name):
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


    def upload_to_aws_s3(self, buket_name, file_address):

        if not os.path.exists(file_address):
            raise Exception(ERROR_UPLOAD_FILE_ADDRESS)
        else:
            # Upload a new file
            with open(file_address, 'rb') as data:

                file_name = os.path.basename(file_address)
                self.s3.Bucket(buket_name).put_object(Key = file_name, Body = data)

    def download_file_from_s3(self, buket_name, key, file_address_to_save):
        if self.s3 is None:
            raise Exception('Not connect to S3 instances')
        else:
            try:
                self.s3.Bucket(buket_name).download_file(key, file_address_to_save)

                return file_address_to_save
            except botocore.exceptions.ClientError as error:
                raise error
            except botocore.exceptions.ParamValidationError as error:
                raise ValueError('The parameters you provided are incorrect: {}'.format(error))


    def process_and_filter_file(self, file_address):

        if not os.path.exists(file_address):
            raise Exception(ADDRESS_TO_SAVE_NOT_DEFINED)
        else:
            df = pd.read_csv(file_address)

            df_filter = df[df['Data_value'] >= 200]

            print(f"Dataset size {df.size} and Filter Dataset {df_filter.size}")

            new_filter_name = f"filter_{self.create_random_filename(BASE_FILENAME)}.csv"

            new_filter_file_path = DATASETS + os.sep + new_filter_name

            df_filter.to_csv(new_filter_file_path, encoding='utf-8', index=False)

            return new_filter_file_path

    def delete_files_in_directory(self, directory_path):
        try:
            files = os.listdir(directory_path)
            for file in files:
                file_path = os.path.join(directory_path, file)

                if os.path.isfile(file_path):
                    os.remove(file_path)

        except OSError:
            print("Error occurred while deleting files.")


class RedShiftAws ():
    def __init__(self):
        try:
            self.client = boto3.client('redshift-data', region_name=env['REDSHIFT_REGION_CLUSTER'])

        except client.exceptions.ActiveSessionsExceededException as e:
            aws_sns_notification("Exception ", env['ARN_SNS_URL_IN_AWS'])
        except Exception as err:
            aws_sns_notification(f"Exception Unexpected {err=}, {type(err)=}", env['ARN_SNS_URL_IN_AWS'])


    def run_redshift_statement(self, sql_statement, client):
         """
         Generic function to handle redshift statements (DDL, SQL..),
         it retries for the maximum MAX_WAIT_CYCLES.
         Returns the result set if the statement return results.
         """
         res = self.client.execute_statement(
          Database = env['REDSHIFT_DATABASE'],
          DbUser =  env['REDSHIFT_DBUSER'],
          Sql = sql_statement,
          ClusterIdentifier = env['REDSHIFT_CLUSTER_IDENTIFIER'],
         )

         # DDL statements such as CREATE TABLE doesn't have result set.
         has_result_set = False
         done = False
         attempts = 0

         while not done and attempts < MAX_WAIT_CYCLES:

          attempts += 1
          time.sleep(1)

          desc = self.client.describe_statement(Id=res['Id'])
          query_status = desc['Status']

          if query_status == "FAILED":
           raise Exception('SQL query failed: ' + desc["Error"])

          elif query_status == "FINISHED":
           done = True
           has_result_set = desc['HasResultSet']
          else:
           logging.info("Current working... query status is: {} ".format(query_status))

         if not done and attempts >= MAX_WAIT_CYCLES:
          raise Exception('Maximum of ' + str(attempts) + ' attempts reached.')

         if has_result_set:
             try:
              data = self.client.get_statement_result(Id=res['Id'])
              return data
             except self.client.exceptions.QueryTimeoutException as e:
                 raise e

    ######
    #  Create the Redshift Table
    #
    #####

    def create_redshift_table(self):
         create_table_ddl = """
           CREATE TABLE public.income (
                series_reference character varying(256) ENCODE lzo,
                period real ENCODE raw,
                data_value character varying(256) ENCODE lzo,
                suppressed boolean ENCODE raw,
                status character varying(256) ENCODE lzo,
                units character varying(256) ENCODE lzo,
                magntude integer ENCODE az64,
                subject character varying(256) ENCODE lzo,
                group character varying(256) ENCODE lzo,
                series_title_1 character varying(256) ENCODE lzo
            )
            DISTSTYLE AUTO;
         """

         self.run_redshift_statement(create_table_ddl)
         logging.info('Table created successfully.')

    ######
    #   Import file content to S3 Table
    #
    #####

    def import_s3_file(self, file_name):
         """
         Loads the content of the S3 temporary file into the Redshift table.
         """

         ### 's3://geekscastle-challenge/csv_processing_241214_222149.csv'

         load_data_ddl = f"""
          COPY {env['TABLE_NAME']} 
          FROM 's3://{env['AWS_BUCKET_NAME']}/{file_name}'
          FORMAT AS CSV  
          DELIMITER ','
          IGNOREHEADER as 1
          REGION '{env['REDSHIFT_REGION_CLUSTER']}'
          IAM_ROLE default;
         """

         self.run_redshift_statement(load_data_ddl, client)
         logging.info('Imported S3 file to Redshift.')

    ######
    #    Query data from the Redshift table
    #
    #####

    def query_redshift_table(self):
         # You can use your own SQL to fetch data.
         select_sql = 'SELECT * FROM punlic.income;'
         data = self.run_redshift_statement(select_sql)
         print(data['Records'])

    def query_statement(self, select_sql):
        data = self.run_redshift_statement(select_sql, client)
        print(data['Records'])


if __name__ == "__main__":
  # Let's use Amazon S3
  logging.basicConfig(level=logging.INFO)
  logging.info('Process started')

  s3 = S3Aws()

  #Download from url
  downloaded_filename = s3.download_csv_file(env['EXTERNAL_URL_CSV'], DATASETS, BASE_FILENAME)

  #Upload to S3
  s3.upload_to_aws_s3(env['AWS_BUCKET_NAME'], downloaded_filename)

  file_to_filter_address = 'datasets' + os.sep + s3.create_random_filename(BASE_FILENAME) + '.csv'

  file_name = os.path.basename(downloaded_filename)

  #Download from S3
  s3.download_file_from_s3(
      env['AWS_BUCKET_NAME'],
      file_name,
      file_to_filter_address
  )

  file_to_filter_address = s3.process_and_filter_file(file_to_filter_address)

  print(f"Filter file {file_to_filter_address}")

  s3.upload_to_aws_s3(env['AWS_BUCKET_NAME'], file_to_filter_address)

  #### RedShift actions
  client = None
  try:
    client = RedShiftAws()
  except Exception as err:
      aws_sns_notification(f"Exception Unexpected {err=}, {type(err)=}", env['ARN_SNS_URL_IN_AWS'])

  file_name = os.path.basename(file_to_filter_address)

  client.import_s3_file(file_name)

  logging.info('Process finished')

  aws_sns_notification(
      "Saved result in Redshift's table ",
      'Redshift saved result',
      env['ARN_SNS_URL_IN_AWS']
  )

  s3.delete_files_in_directory(DATASETS)




