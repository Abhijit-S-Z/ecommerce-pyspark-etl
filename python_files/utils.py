import logging
import os
from datetime import datetime
import sys
import configparser

def logging_config(python_filename):

    # Setting up logging config
    curr_ts = datetime.now().strftime("_%d-%m-%Y_%H-%M-%S")
    # log_filename = f"{python_filename}_{curr_ts}.log"
    log_filename = f"{python_filename}.log"  # CHANGE: Temporary file as no need of multiple logs during dev
    log_path = os.path.join("logs", log_filename)

    logging.basicConfig(
    filename = log_path,
    filemode = 'w',     # CHANGE: change to 'a' once log_filename is updated
    level=logging.INFO,
    format = '%(asctime)s | %(levelname)s | %(message)s'
    )

def read_config_file(config_file_path, section):
    config = configparser.ConfigParser()
    config.read(config_file_path)
    return config


def read_source_files(spark, path, file_schema):
    try:
        filename = os.path.basename(path)
        extension = filename.split(".")[-1]
        if extension.lower() == 'csv':
            # Read csv files and return it as spark df
            df = spark.read.csv(path, schema = file_schema, header = True)
            logging.info("Successfully loaded %s file", filename)
            return df
        else:
            logging.warning("Not a valid extension, valid extensions - [csv]")
        
        # Add / modify the function as per the new file/s requirement
    except Exception as e:
        logging.warning("Error while reading the source file: %s", filename)
        raise

    