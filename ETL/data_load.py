"""
etl_crm_rest_poc.py

ETL CRM REST API to Oracle sandbox
"""
import logging
import sys
import mysql.connector
from mysql.connector import Error
import requests

import constant


def main():
    """
    Main ETL script definition.
    :return: None
    """
    logging.basicConfig(level=logging.INFO)

    try:
        # Connect to MySQL database
        mysqldb = mysql.connector.connect(
            host=constant.HOST,
            port=constant.PORT,
            user=constant.USER,
            password=constant.PASSWORD,
            database=constant.DATABASE
        )
        if mysqldb.is_connected():
            db_Info = mysqldb.get_server_info()
            logging.debug("Connected to MySQL Server version: {} ".format(str(db_Info)))
            cursor = mysqldb.cursor()
            cursor.execute("select database();")
            record = cursor.fetchone()
            logging.info("You're connected to database: {}".format(str(record)))

        try:
            logging.info('Extracting data from API')
            data = extract_data(constant.API_URL)

            obj: dict = data["results"]

            for col in obj:
                user_data: dict = col["name"]

            logging.info("Extracted data: {}".format(user_data))

        except Exception as ex:
            logging.error("REST endpoint could not be reached. Exiting with error: {}".format(str(ex)))
            sys.exit(1)

        transformed_data = transform_data(user_data)

        logging.info('Loading data into the database...')

        load_data(mysqldb, transformed_data)

    except Error as e:
        logging.error("Error while connecting to MySQL", e)
    finally:
        if mysqldb.is_connected():
            cursor.close()
            mysqldb.close()
            logging.debug("MySQL connection is closed")

    mysqldb.connect()


    return None

def extract_data(url):
    """
    Extracts JSON data from REST endpoint.
    :param log: Logger
    :param rest_url: REST endpoint
    :return: JSON object
    """
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

def transform_data(data: dict):
    """
    Transform the original data
    :param log: Logger
    :param data: extracted data
    :return: Transformed data
    """
    # Transformation to upper case
    logging.info("Data before transformation: {}".format(data))
    data_transformed = {k: v.upper() for k, v in data.items()}
    logging.info("Data after transformation: {}".format(data_transformed))
    return data_transformed

def load_data(db, data):
    """
    Trigger load to Oracle Sandbox
    :param log: Logger
    :param db: db object
    :param data: dict
    """
    try:
        row = [data["title"] + " " + data["first"] + " " + data["last"]]


        insert_stmt = (
            "INSERT INTO training.user (name) "
            "VALUES (%s)"
        )

        db.cursor().execute(insert_stmt, row)



        # print('Data for id:{} has been loaded'.format(data["d"]["CountryId"]))
    except Exception as ex:
        logging.error('Failed to load data to DB: {}'.format(str(ex)))
        sys.exit(2)

    db.commit()
    logging.info('Data has been loaded to the database successfully')


# entry point
if __name__ == '__main__':
    main()
