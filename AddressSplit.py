import json
import mysql.connector
import logging
from PythonLogging import setup_logging
from OpenAI import split_openai


def remove_text_before_marker(text, marker):
    index = text.find(marker)
    if index != -1:
        return text[index + len(marker):]
    return text


def remove_string(text, string_to_remove):
    if string_to_remove in text:
        text = text.replace(string_to_remove, "")
    return text


def split_address(registration_no,config_dict,db_config):
    setup_logging()
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()
    connection.autocommit = True
    address_query = f"select address,id from authorized_signatories where registration_no = '{registration_no}'"
    logging.info(address_query)
    cursor.execute(address_query)
    address_list = cursor.fetchall()
    cursor.close()
    connection.close()
    prompt = config_dict['Prompt']
    for address in address_list:
        try:
            address_to_split = address[0]
            database_id = address[1]
            address_to_split = address_to_split.replace("'", "").replace('"', "")
            logging.info(address_to_split)
            if str(address_to_split).lower() != 'null' and address_to_split is not None:
                connection = mysql.connector.connect(**db_config)
                cursor = connection.cursor()
                connection.autocommit = True
                splitted_address = split_openai(address_to_split,prompt)
                splitted_address = remove_text_before_marker(splitted_address, "```json")
                splitted_address = remove_string(splitted_address, "```")
                try:
                    splitted_address = json.loads(splitted_address)
                except Exception as e:
                    splitted_address = eval(splitted_address)
                splitted_address['address_line2'] = splitted_address['address_line1']
                splitted_address['address_line1'] = address_to_split
                try:
                    splitted_address = str(splitted_address).replace("'",'"')
                except:
                    pass
                update_query = f"update authorized_signatories set splitted_address = '{splitted_address}' where registration_no = '{registration_no}' and id = {database_id}"
                logging.info(update_query)
                cursor.execute(update_query)
                cursor.close()
                connection.close()
        except Exception as e:
            logging.error(f"Error in splitting address for  {e}")

    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()
    connection.autocommit = True
    registered_address_query = f"select registered_full_address,id from Company where registration_no = '{registration_no}'"
    logging.info(registered_address_query)
    cursor.execute(registered_address_query)
    registered_address_list = cursor.fetchall()
    cursor.close()
    connection.close()
    for registered_address in registered_address_list:
        try:
            registered_address_to_split = registered_address[0]
            database_id = registered_address[1]
            registered_address_to_split = registered_address_to_split.replace("'", "").replace('"', "")
            if str(registered_address_to_split).lower() != 'null' and registered_address_to_split is not None:
                connection = mysql.connector.connect(**db_config)
                cursor = connection.cursor()
                connection.autocommit = True
                registered_splitted_address = split_openai(registered_address_to_split, prompt)
                registered_splitted_address = remove_text_before_marker(registered_splitted_address, "```json")
                registered_splitted_address = remove_string(registered_splitted_address, "```")
                try:
                    registered_splitted_address = json.loads(registered_splitted_address)
                except Exception as e:
                    registered_splitted_address = eval(registered_splitted_address)
                registered_splitted_address['address_line2'] = registered_splitted_address['address_line1']
                registered_splitted_address['address_line1'] = registered_address_to_split
                address_line1 = registered_splitted_address['address_line1']
                address_line2 = registered_splitted_address['address_line2']
                city = registered_splitted_address['city']
                state = registered_splitted_address['state']
                pincode = registered_splitted_address['pincode']
                try:
                    registered_splitted_address = str(registered_splitted_address).replace("'", '"')
                except:
                    pass
                update_query = f"update Company set registered_splitted_address	 = '{registered_splitted_address}',registered_city = '{city}',registered_state = '{state}',registered_pincode = '{pincode}',registered_address_line1 = '{address_line1}',registered_address_line2 = '{address_line2}' where registration_no = '{registration_no}' and id = {database_id}"
                logging.info(update_query)
                cursor.execute(update_query)
                cursor.close()
                connection.close()
        except Exception as e:
            logging.error(f"Error in splitting registered address {e}")