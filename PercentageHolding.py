import mysql.connector
from PythonLogging import setup_logging
import logging
import json


def get_percentage_holding(db_config, registration_no):
    setup_logging()
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
        connection.autocommit = True
        total_paid_up_capital = 0
        paid_up_capital_query = f"select * from paid_up_capital_values where registration_no = '{registration_no}'"
        logging.info(paid_up_capital_query)
        cursor.execute(paid_up_capital_query)
        paid_up_capitals = cursor.fetchall()
        for paid_up_capital in paid_up_capitals:
            try:
                paid_up_capital = float(paid_up_capital[2])
            except:
                pass
            total_paid_up_capital += paid_up_capital
        logging.info(total_paid_up_capital)
        shareholders_query = f"Select * from current_shareholdings where registration_no = '{registration_no}'"
        cursor.execute(shareholders_query)
        shareholders = cursor.fetchall()
        for shareholder in shareholders:
            database_id = shareholder[0]
            no_of_shares = shareholder[4]
            try:
                no_of_shares = str(no_of_shares).replace(',','')
                no_of_shares = float(no_of_shares)
            except:
                pass
            percentage_holding = (no_of_shares/total_paid_up_capital)*100
            try:
                percentage_holding = round(percentage_holding, 2)
            except:
                pass
            update_query = f"update current_shareholdings set percentage_holding = '{percentage_holding}' where registration_no = '{registration_no}' and id = '{database_id}'"
            logging.info(update_query)
            cursor.execute(update_query)
        cursor.close()
        connection.close()
    except Exception as e:
        logging.error(f"Exception occurred while getting percentage holding {e}")
