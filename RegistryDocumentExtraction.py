import pandas as pd
import json
from PythonLogging import setup_logging
import os
import logging
from AmazonOCRAllPages import extract_text_from_pdf
from OpenAI import split_openai
from ReadExcelConfig import create_main_config_dictionary
from DatabaseQueries import get_db_credentials
from DatabaseQueries import update_database_single_value_with_one_column_check
from DatabaseQueries import update_database_single_value
from DatabaseQueries import insert_datatable_with_table_director
import traceback
from datetime import datetime


def remove_text_before_marker(text, marker):
    index = text.find(marker)
    if index != -1:
        return text[index + len(marker):]
    return text


def remove_string(text, string_to_remove):
    if string_to_remove in text:
        text = text.replace(string_to_remove, "")
    return text


def registry_document_main(db_config, config_dict, pdf_path, output_file_path, registration_no):
    setup_logging()
    error_count = 0
    errors = []
    try:
        extraction_config = config_dict['extraction_config_path']
        map_file_sheet_name = config_dict['config_sheet']
        if not os.path.exists(extraction_config):
            raise Exception("Main Mapping File not found")
        try:
            df_map = pd.read_excel(extraction_config, engine='openpyxl', sheet_name=map_file_sheet_name)
        except Exception as e:
            raise Exception(f"Below exception occurred while reading mapping file {e}")
        df_map['Value'] = None
        output_dataframes_list = []
        single_df = df_map[df_map[df_map.columns[1]] == config_dict['single_keyword']]
        group_df = df_map[df_map[df_map.columns[1]] == config_dict['group_keyword']]
        single_nodes = single_df['Node'].unique()
        open_ai_dict = {field_name: '' for field_name in single_nodes}
        for index, row in group_df.iterrows():
            node_values = str(row['Node']).split(',')
            sub_dict = {field_name: '' for field_name in node_values}
            main_node = row['main_dict_node']
            sub_list = {main_node: [sub_dict]}
            open_ai_dict.update(sub_list)
        pdf_text = extract_text_from_pdf(pdf_path)
        form10_prompt = config_dict['common_prompt'] + '\n' + str(open_ai_dict)
        output = split_openai(pdf_text, form10_prompt)
        output = remove_text_before_marker(output, "```json")
        output = remove_string(output, "```")
        logging.info(output)
        try:
            output = eval(output)
        except:
            output = json.loads(output)
        for index, row in df_map.iterrows():
            dict_node = str(row.iloc[2]).strip()
            type = str(row.iloc[1]).strip()
            main_group_node = str(row.iloc[6]).strip()
            if type.lower() == 'single':
                value = output.get(dict_node)
                value = str(value).replace("'", "")
            elif type.lower() == 'group':
                value = output.get(main_group_node)
            else:
                value = None
            df_map.at[index, 'Value'] = value
        single_df = df_map[df_map[df_map.columns[1]] == config_dict['single_keyword']]
        group_df = df_map[df_map[df_map.columns[1]] == config_dict['group_keyword']]
        output_dataframes_list.append(single_df)
        output_dataframes_list.append(group_df)
        registration_no_column_name = config_dict['registration_no_Column_name']
        sql_tables_list = single_df[single_df.columns[3]].unique()
        for table_name in sql_tables_list:
            table_df = single_df[single_df[single_df.columns[3]] == table_name]
            columns_list = table_df[table_df.columns[4]].unique()
            for column_name in columns_list:
                logging.info(column_name)
                # filter table df with only column value
                column_df = table_df[table_df[table_df.columns[4]] == column_name]
                logging.info(column_df)
                # create json dict with keys of field name and values for the same column name entries
                json_dict = column_df.set_index(table_df.columns[0])['Value'].to_dict()
                # Convert the dictionary to a JSON string
                json_string = json.dumps(json_dict)
                logging.info(json_string)
                try:
                    update_database_single_value(db_config, table_name,registration_no_column_name,
                                                                           registration_no,
                                                                           column_name, json_string)
                except Exception as e:
                    logging.error(f"Exception {e} occurred while updating data in dataframe for {table_name} "
                                  f"with data {json_string}")
                    error_count += 1
                    tb = traceback.extract_tb(e.__traceback__)
                    for frame in tb:
                        if frame.filename == __file__:
                            errors.append(f"Line {frame.lineno}: {frame.line} - {str(e)}")
        for index, row in group_df.iterrows():
            try:
                field_name = str(row.iloc[0]).strip()
                nodes = str(row.iloc[2]).strip()
                sql_table_name = str(row.iloc[3]).strip()
                column_names = str(row.iloc[4]).strip()
                main_group_node = str(row.iloc[6]).strip()
                value_list = row['Value']
                if value_list is not None:
                    if len(value_list) == 0:
                        logging.info(f"No value for {field_name} so going to next field")
                        continue
                else:
                    logging.info(f"No value for {field_name} so going to next field")
                    continue
                table_df = pd.DataFrame(value_list)
                logging.info(table_df)
                column_names_list = column_names.split(',')
                column_names_list = [x.strip() for x in column_names_list]
                table_df = table_df.fillna('')
                table_df[registration_no_column_name] = registration_no
                column_names_list.append(registration_no_column_name)
                column_names_list = [x.strip() for x in column_names_list]
                table_df.columns = column_names_list
                for _, df_row in table_df.iterrows():
                    try:
                        insert_datatable_with_table_director(config_dict, db_config, sql_table_name, column_names_list,
                                                             df_row)
                    except Exception as e:
                        logging.info(
                            f'Exception {e} occurred while inserting below table row in table {sql_table_name}- \n',
                            df_row)
                        error_count += 1
                        tb = traceback.extract_tb(e.__traceback__)
                        for frame in tb:
                            if frame.filename == __file__:
                                errors.append(f"Line {frame.lineno}: {frame.line} - {str(e)}")
            except Exception as e:
                logging.error(f"Exception occurred while inserting for group values {e}")
                error_count += 1
                tb = traceback.extract_tb(e.__traceback__)
                for frame in tb:
                    if frame.filename == __file__:
                        errors.append(f"Line {frame.lineno}: {frame.line} - {str(e)}")
        with pd.ExcelWriter(output_file_path, engine='xlsxwriter') as writer:
            row_index = 0
            for dataframe in output_dataframes_list:
                # logging.info(dataframe)
                dataframe.to_excel(writer, sheet_name='Sheet1', index=False, startrow=row_index)
                row_index += len(dataframe.index) + 2
        output_dataframes_list.clear()
    except Exception as e:
        logging.error(f"Error in extracting data from Form 40 {e}")
        tb = traceback.extract_tb(e.__traceback__)
        for frame in tb:
            if frame.filename == __file__:
                errors.append(f"Line {frame.lineno}: {frame.line} - {str(e)}")
        raise Exception(errors)
    else:
        if error_count == 0:
            logging.info(f"Successfully extracted for Form 40")
            return True
        else:
            raise Exception(f"Multiple exceptions occurred:\n\n" + "\n".join(errors))

