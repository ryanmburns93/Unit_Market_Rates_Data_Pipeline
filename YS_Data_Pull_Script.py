# -*- coding: utf-8 -*-
"""
Created on Thu Jul 14 18:18:12 2022

@author: rburns
"""

import os
import subprocess
from datetime import datetime
import pandas as pd
import urllib
from sqlalchemy import create_engine
from sqlalchemy.types import NUMERIC, VARCHAR, DATE, NVARCHAR, DECIMAL
import smtplib
import logging
from logging.handlers import RotatingFileHandler
from dotenv import load_dotenv
from proprietary_data_loader_utils import get_property_name_id_dict


load_dotenv('./.env')


def set_program_operations_directory(logger_name):
    logger = logging.getLogger(f'{logger_name}.set_program_operations_directory')
    if os.path.exists(os.path.join(os.getcwd(), 'temp_dir/')):
        logger.debug(f'File directory {os.path.join(os.getcwd(), "temp_dir/")} already exists, deleting out old files.')
        old_files_list = os.listdir('./temp_dir/')
        for file in old_files_list:
            logger.debug(f'Deleting file: {file} from temp_dir/ directory.')
            os.remove(os.path.join(f'./temp_dir/{file}'))
    else:
        logger.debug(f'File directory {os.path.join(os.getcwd(), "temp_dir/")} does not exist. Creating now.')
        os.mkdir(os.path.join(os.getcwd(), 'temp_dir/'))
    return


def download_file_from_remote_to_local(logger_name, winSCP_program_directory=r"C:\Users\RBurns\AppData\Local\Programs\WinSCP\\"):   
    logger = logging.getLogger(f'{logger_name}.download_file_from_remote_to_local')
    error_message = None
    args = fr'"{winSCP_program_directory}' + fr'WinSCP.com" /ini=nul /command "open {os.getenv("SESSION_URL")} -hostkey=""{os.getenv("HOSTKEY")}""" "pwd" "cd outgoing" "get {os.getenv("TARGET_FILENAME")} .\temp_dir\" "exit"'
    logger.debug(f'WinSCP script command and arguments set to: {args}.')
    logger.debug('WinSCP command output:\n')
    winSCP_output_lines = []
    process = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    for line in iter(process.stdout.readline, b''):
        winSCP_output_lines.append(line.decode().rstrip())

    for index, output_line in enumerate(winSCP_output_lines):
        logger.debug(f'WinSCP Output Line {index}: {output_line}')

    for index, output_line in enumerate(winSCP_output_lines):
        if 'Error message' in output_line:
            logger.debug('Found string "Error message" in a WinSCP output line, triggering non-null error_message to be returned.')
            error_message = f"The Market Rate Data Pull for {datetime.today().strftime('%Y-%m-%d')} has failed.\n\nThe WinSCP command script was unable to connect to the FTP or download the {os.getenv('TARGET_FILENAME')} file located in the /outgoing/ subdirectory. WinSCP responded: {output_line}.\n\nThe data for {datetime.today().strftime('%Y-%m-%d')} will not be present in the {os.getenv('SQL_TABLE')} table.\n\nPlease reach out to Ryan Burns or a member of the RPA team for assistance as needed."
        elif 'System Error.' in output_line:
            logger.debug('Found string "System Error." in a WinSCP output line, triggering non-null error_message to be returned.')
            error_message = f"The Market Rate Data Pull for {datetime.today().strftime('%Y-%m-%d')} has failed.\n\nThe WinSCP command script was unable to connect to the FTP or download the {os.getenv('TARGET_FILENAME')} file located in the /outgoing/ subdirectory. WinSCP responded: {winSCP_output_lines[index+1]}.\n\nThe data for {datetime.today().strftime('%Y-%m-%d')} will not be present in the {os.getenv('SQL_TABLE')} table.\n\nPlease reach out to Ryan Burns or a member of the RPA team for assistance as needed."
        elif 'Access denied' in output_line:
            logger.debug('Found string "Access denied" in a WinSCP output line, triggering non-null error_message to be returned.')
            error_message = f"The Market Rate Data Pull for {datetime.today().strftime('%Y-%m-%d')} has failed.\n\nThe WinSCP command script was unable to connect to the FTP or download the {os.getenv('TARGET_FILENAME')} file located in the /outgoing/ subdirectory. WinSCP responded: {output_line} which likely indicates an authentication issue.\n\nThe data for {datetime.today().strftime('%Y-%m-%d')} will not be present in the {os.getenv('SQL_TABLE')} table.\n\nPlease reach out to Ryan Burns or a member of the RPA team for assistance as needed."
        elif 'does not exist' in output_line:
            logger.debug('Found string "does not exist" in a WinSCP output line, triggering non-null error_message to be returned.')
            error_message = f"The Market Rate Data Pull for {datetime.today().strftime('%Y-%m-%d')} has failed.\n\nThe WinSCP command script was unable to connect to the FTP or download the {os.getenv('TARGET_FILENAME')} file located in the /outgoing/ subdirectory. WinSCP responded: {output_line}.\n\nThe data for {datetime.today().strftime('%Y-%m-%d')} will not be present in the {os.getenv('SQL_TABLE')} table.\n\nPlease reach out to Ryan Burns or a member of the RPA team for assistance as needed."
    return error_message


def read_and_clean_csv_file(logger_name):
    logger = logging.getLogger(f'{logger_name}.send_data_to_sql_table')
    error_message = None
    test_df = pd.read_csv(f'./temp_dir/{os.getenv("TARGET_FILENAME")}', header=None)
    logger.debug(f"Completed reading csv file located at {os.path.abspath(os.path.join('./temp_dir/', {os.getenv('TARGET_FILENAME')}))}.")
    test_df.columns = ['PropertyID', 'Date', 'Floorplan', 'Building', 'Unit', 'BaseRent', 'MarketRent']
    logger.debug('Renamed columns excluding Property column not in dataset yet.')    
    prop_name_id_dict = get_property_name_id_dict()
    
    logger.debug(f'Loaded prop_name_id_dict for Property lookup by PropertyID. Dictionary contains {len(prop_name_id_dict)} properties.')
    prop_name_list = []
    for index, row in test_df.iterrows():
        prop_name_list.append(prop_name_id_dict.get(row['PropertyID']))
    
    test_df['Property'] = prop_name_list
    if test_df['Property'].nunique() != 111:
        logger.debug(f'Data in dataframe had {test_df["Property"].nunique()} instead of expected 111.')
        logger.debug('This may cause properties to be missing names in the table, and is likely a result of an acquisition which has not been incorporated into the program code.')
        logger.debug('Setting error_message and returning empty test_df_today dataframe object.')
        error_message = f"The Market Rate Data Pull for {datetime.today().strftime('%Y-%m-%d')} has failed.\n\nThe Report had {test_df['PropertyID'].nunique()} PropertyID's while only 111 existed at program write. This may cause properties to be missing names in the table, and is likely a result of an acquisition which has not been incorporated into the program code. As a result, the data for {datetime.today().strftime('%Y-%m-%d')} will not be present in the {os.getenv('SQL_TABLE')} table.\n\nPlease reach out to Ryan Burns or a member of the RPA team for correction."
        test_df_today = pd.DataFrame()
        return test_df_today, error_message
        
    test_df = test_df[['Property', 'PropertyID', 'Date', 'Floorplan', 'Building', 'Unit', 'BaseRent', 'MarketRent']]
    
    test_df['Date'] = [datetime.strptime(str(date), '%Y%m%d').strftime('%Y-%m-%d') for date in test_df['Date']]
    test_df_today = test_df[test_df['Date']==datetime.today().strftime('%Y-%m-%d')]
    if len(test_df_today)==0:
        error_message = f"The Market Rate Data Pull for {datetime.today().strftime('%Y-%m-%d')} has failed.\n\nThe document did not have contain data for today, {datetime.today().strftime('%Y-%m-%d')}. The Windows Scheduler Task instance may need to be scheduled for later in the day, or the data provider may have failed to provide the daily update for today. The data for {datetime.today().strftime('%Y-%m-%d')} will not be present in the {os.getenv('SQL_TABLE')} table."
    return test_df_today, error_message


def send_data_to_sql_table(data_df, logger_name):
    logger = logging.getLogger(f'{logger_name}.send_data_to_sql_table')
    error_message = None
    quoted = urllib.parse.quote_plus(os.getenv('SQL_CONNECTION_STRING'))
    engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted))
    logger.debug(f"Successfully created engine with connection string: {'mssql+pyodbc:///?odbc_connect={}'.format(quoted)}")
    try:
        logger.debug(f'Started upload to SQL database in table {os.getenv("SQL_TABLE")}.')
        start = datetime.now()
        data_df.to_sql(name=os.getenv("SQL_TABLE"),
                       schema="ads",
                       con=engine,
                       if_exists='append',
                       index=False,
                       chunksize=260,
                       method='multi',
                       dtype={'Property': VARCHAR(250, collation="SQL_Latin1_General_CP1_CI_AS"),
                              'PropertyID': NUMERIC,
                              'Date': DATE,
                              'Floorplan': NVARCHAR(250, collation="SQL_Latin1_General_CP1_CI_AS"),
                              'Building': NVARCHAR(250, collation="SQL_Latin1_General_CP1_CI_AS"),
                              'Unit': NVARCHAR(250, collation="SQL_Latin1_General_CP1_CI_AS"),
                              'BaseRent': DECIMAL,
                              'MarketRent': DECIMAL})
        
        end = datetime.now()
        runtime = end-start
        logger.debug(f'Successfully completed transfer of {len(data_df)} rows to {os.getenv("SQL_TABLE")} SQL table in {runtime}.')
        logger.debug(f'Program run complete for {datetime.today().strftime("%Y-%m-%d")}.')
    except Exception:
        logger.debug(f'An unexpected exception occurred while transfering third-party data to {os.getenv("SQL_TABLE")} SQL table.')
        error_message = f"The Market Rate Data Pull for {datetime.today().strftime('%Y-%m-%d')} has failed.\n\nWhile no errors were found in collecting and processing the report data, the program was unable to append today's data into the SQL table {os.getenv('SQL_TABLE')}. This may be due to database downtime or other factors leading to the table being inaccessible. As a result, the data for {datetime.today().strftime('%Y-%m-%d')} will not be present in the {os.getenv('SQL_TABLE')} table.\n\nPlease reach out to Ryan Burns or a member of the RPA team for correction."
    return error_message


def check_for_errors(error_message, logger_name):
    logger = logging.getLogger(f'{logger_name}.check_for_errors')
    if error_message is not None:
        logger.debug('Error message is not None. Calling distribute_error_message function.')
        recipients_list = os.getenv('ERROR_RECIPIENTS_LIST').split("'")
        recipients_list = [email for email in recipients_list if '@' in email]
        distribute_error_message(error_message, logger_name, os.getenv('ERROR_SENDER'), os.getenv('ERROR_SENDER_PASSWORD'), recipients_list)
        error_flag = True
        logger.debug(f'Finished calling distribute_error_message function. Set error_flag to {error_flag}.')
    else:
        error_flag = False
        logger.debug(f'Error message is None. Set error_flag to {error_flag}.')
    return error_flag


def distribute_error_message(error_message, logger_name, sender, sender_password, recipients_list):
    # Still need to fix missing subject line for error notification emails
    logger = logging.getLogger(f'{logger_name}.distribute_error_message')
    mailserver = smtplib.SMTP('smtp.office365.com', 587)
    logger.debug('Mailserver object created.')
    mailserver.ehlo()
    mailserver.starttls()
    logger.debug('TLS started for mailserver object.')
    mailserver.login(sender, sender_password)
    logger.debug('Mailserver object login complete.')
    # Adding a newline before the body text fixes the missing message body
    for recipient in recipients_list:
        mailserver.sendmail(sender, recipient, f'\n\n{error_message}')
        logger.debug(f'Mailserver finished sending mail with error message to {recipient} from {sender}.')
    mailserver.quit()
    logger.debug(f'Mailserver quit after sending error message: {error_message}')
    return


def setup_logging(save_dir, logger_name, output_filename='logging_output.log', verbosity=10):
    """
    Establish logging configurations such as verbosity and output file location for the 
    remainder of the program.

    Parameters
    ----------
    save_dir : str
        String path to directory in which logging should be recorded. 
    logger_name : str
        String name of logger to be used while logging output into output_filename.
    output_filename : str, optional
        String filename specifying the name of the logging output file in which a logger specified
        by logger_name records output. The default is 'logging_output.log'.
    verbosity : int, optional
        Integer representation of the threshold at which a record will be logged. Options
        range from 0 (NOTSET threshold) to 50 (CRITICAL threshold). The default is 10 (DEBUG).

    Returns
    -------
    None.

    """
    logger = logging.getLogger(logger_name)
    logger.setLevel(verbosity)
    logger.handlers = []
    logger.propogate = False
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(lineno)d - %(message)s')
    handler = RotatingFileHandler(filename=os.path.join(save_dir, output_filename),
                                  mode='a+',
                                  maxBytes=2000000,
                                  backupCount=10)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.info('-----------------------------')
    logger.info('--BEGINNING NEW PROGRAM RUN--')
    logger.info('Start Time: %s' % datetime.now())
    logger.info('-----------------------------')
    logger.info(f'Logging Level set to: {logger.getEffectiveLevel()}')
    return


def main(ops_dir='C:/Users/RBurns/Documents/Unit_Rate_Data_Pull/', logger_name='Unit_Rate_Data_Pull'):
    os.chdir(ops_dir)
    setup_logging(save_dir=ops_dir,
                  logger_name=logger_name,
                  output_filename=f'logging_output_{datetime.today().strftime("%Y-%m-%d")}.log')
    set_program_operations_directory(logger_name)
    error_message = download_file_from_remote_to_local(logger_name)
    if check_for_errors(error_message, logger_name):
        return
    data_df, error_message = read_and_clean_csv_file(logger_name)
    if check_for_errors(error_message, logger_name):
        return
    error_message = send_data_to_sql_table(data_df, logger_name)
    if check_for_errors(error_message, logger_name):
        return
    return


if __name__=='__main__':
    main()
