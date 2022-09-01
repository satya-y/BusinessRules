try:
    # comment below two for local testing
    from ace_logger import Logging
    logging = Logging()  
    from db_utils import DB 
except Exception as e:
    # uncomment these below lines for local testing
    import logging 
    logger=logging.getLogger() 
    logger.setLevel(logging.DEBUG)

from db_utils import DB 
import time,datetime
import pandas as pd
import ntpath
import numpy as np
import re
import traceback
import requests
from pandas import json_normalize
import pandas as pd
from pyxlsb import open_workbook as open_xlsb

import json
import os
import re


from BusinessRules import BusinessRules

# one configuration
db_config = {
    'host': os.environ['HOST_IP'],
    'user': os.environ['LOCAL_DB_USER'],
    'password': os.environ['LOCAL_DB_PASSWORD'],
    'port': os.environ['LOCAL_DB_PORT']
}


# give any path...platform independent...get the base name
def path_leaf(path):
    """give any path...platform independent...get the base name"""
    head, tail = ntpath.split(path)
    return tail or ntpath.basename(head)


def to_DT_data(parameters):
    """Amith's processing for parameters"""
    output = []
    try:
        for param_dict in parameters:
            print(param_dict)    
            if param_dict['column'] == 'Add_on_Table':
                output.append({'table': param_dict['table'],'column': param_dict['column'],'value': param_dict['value']})
                # Need to add a function to show this or tell Kamal check if its addon table and parse accordingly
            else:                
                output.append({'table': param_dict['table'],'column': param_dict['column'],'value': param_dict['value']})
    except:
        print("Error in to_DT_data()")
        traceback.print_exc()
        return []
    try:
        output = [dict(t) for t in {tuple(d.items()) for d in output}]
    except:
        print("Error in removing duplicate dictionaries in list")
        traceback.print_exc()
        pass
    return output

def get_data_sources(tenant_id, case_id, column_name, master=False):
    """Helper to get all the required table data for the businesss rules to apply
    """
    get_datasources_query = "SELECT * from [data_sources]"
    business_rules_db = DB('business_rules', tenant_id=tenant_id, **db_config)
    data_sources = business_rules_db.execute(get_datasources_query)

    # sources
    sources = json.loads(list(data_sources[column_name])[0])
    
    data = {}
    for database, tables in sources.items():
        db = DB(database, tenant_id=tenant_id, **db_config)
        for table in tables:
            if master:
                query = f"SELECT * from [{table}]"
                df = db.execute(query)
            else:
                query = f"SELECT * from [{table}] WHERE [case_id] = %s"
                params = [case_id]
                df = db.execute(query, params=params)
            if not df.empty:
                data[table] = df.to_dict(orient='records')[0]
            else:
                data[table] = {}
    
    
    case_id_based_sources = json.loads(list(data_sources['case_id_based'])[0])
    
    return data
                
def get_rules(tenant_id, group):
    """Get the rules based on the stage, tenant_id"""
    business_rules_db = DB('business_rules', tenant_id=tenant_id, **db_config)
    get_rules_query = "SELECT * from [sequence_rule_data] where [group] = %s"
    params = [group]
    rules = business_rules_db.execute(get_rules_query, params=params)
    return rules

def update_tables(case_id, tenant_id, updates):
    """Update the values in the database"""
    try:
        extraction_db = DB('extraction', tenant_id=tenant_id, **db_config) # only in ocr or process_queue we are updating
        queue_db = DB('queues', tenant_id=tenant_id, **db_config) # only in ocr or process_queue we are updating
        
        for table, colum_values in updates.items():
            if table == 'ocr':
                extraction_db.update(table, update=colum_values, where={'case_id':case_id})
            if table == 'process_queue':
                queue_db.update(table, update=colum_values, where={'case_id':case_id})
    except Exception as e:
        logging.error("Error in updating the tables")
        logging.error("check whether the table exists")
        logging.error(e)
        
    return "UPDATED IN THE DATABASE SUCCESSFULLY"

def capture_matrix(df,file_name, tenant_id):
    total_count = len(df.index)
    rejected_count = len(df[df['Filter'] == 'Y'])
    query = f"INSERT INTO [capture_matrix](file_name, total_count, rejected_count) VALUES ('{file_name}',{total_count},{rejected_count})"
    try:
        db = DB('business_rules', tenant_id=tenant_id, **db_config)
        db.execute(query,'karvy_business_rules')
    except Exception as e:
        logging.error("Error in updating the table of capture_matrix")
        logging.error("check whether the table exists")
        logging.error(e)

    return "UPDATED CAPTURE MATRIX IN THE DATABASE SUCCESSFULLY"

def progress_bar(unique_id ,file_name, tenant_id, map_stage, case_id, stage, description):
    # keep updates reports to database for progress_bar...
    FileName = file_name
    Stage = stage
    FolderName = map_stage
    Description = description
    db_config = {
            'host': os.environ['HOST_IP'],
            'port': os.environ['LOCAL_DB_PORT'],
            'user': os.environ['LOCAL_DB_USER'],
            'password': os.environ['LOCAL_DB_PASSWORD']
    }
    
    query = f"INSERT INTO [progress_bar](file_name, stage, folder_name, description, unique_id) VALUES ('{FileName}','{Stage}','{FolderName}','{Description}', '{unique_id}')"
    try:
        db = DB('extraction', tenant_id=tenant_id, **db_config)
        db.execute(query,'karvy_extraction')
    except Exception as e:
        logging.error(f"Error in updating the table of progress_bar at : {description}")
        logging.error("check whether the table exists")
        logging.error(e)

    return "UPDATED PROGRESS_BAR IN THE DATABASE SUCCESSFULLY"

def get_account_no(file_path, df):
    # getting account_number from file_path...
    account_number = ""
    try:
        logging.info(f"base file name is {os.path.basename(file_path)}")
        accnos = ([(re.findall('[0-9]{9,18}', str(e))) for e in os.path.basename(file_path).split("_")])
        accnos = [x for x in accnos if x]
        if accnos:
            if accnos[0]:
                account_number =  (accnos[0][0])
        if not account_number:
            try:
                logging.info(f"base file name is {os.path.basename(file_path)}")
                accnos = ([(re.findall('[0-9]{8,18}', str(e))) for e in os.path.basename(file_path).split("_")])
                accnos = [x for x in accnos if x]
                if accnos:
                    if accnos[0]:
                        account_number =  (accnos[0][0])
            except:
                account_number = ""
    except Exception as e:
        logging.error(str(e))
    logging.info(f"account number got from the bank file path is {account_number}")
    df['Account Number'] = str(account_number)
    return df, account_number


def get_new_df(biz_master_df, df,stage, file_path):
    # removing junk present on the top of the dataframe...
    try:
        logging.info(f"headers got are : {biz_master_df['headers'][0]}")
        headers = json.loads(biz_master_df['headers'][0])
        key = stage
        values = headers
        strip_values = [str(ele).strip() for ele in values]
        if headers:
            if stage == 'CPVIRTUAL_MB99' or stage == 'IDBI':
                df = df.dropna(axis = 1,how='all')
            for index, row in df.iterrows():
                strips_rows = [str(ele).strip() for ele in list(row)]
                if (strips_rows[0] == strip_values[0] and strips_rows[1] == strip_values[1]):

                    new_header = df.iloc[index] #grab the first row for the header
                    df = df[index+1:] #take the data less the header row
                    df.columns = new_header #set the header row as the df header
                    break
            # add the remaining columns if there are any
            df.reindex(columns = headers)
            
        df = df.dropna(axis = 0,how='all')
        df = df.reset_index(drop=True)
        logging.info(f"Dataframe headers are : {df.columns}")
        try:
            headers_got = [str(ele).strip() for ele in list(df.columns)]
        except:
            headers_got = list(df.columns)

        configured_headers = strip_values
        missing_headers = [header for header in headers_got if header not in configured_headers]
        if missing_headers:
            stage = str(stage)
            reject_files = [file_path]
            reject_file_json = {"topic": 'rejection_email_api', "tenant_id": 'karvy',"rejection_reason":f"Headers {missing_headers} are missing from the input file",
                                        "rejected_file_path": reject_files, "stage": stage}
            logging.debug(reject_file_json)
            host_name = os.environ['HOST_IP']
            headers = {'Content-type': 'application/json; charset=utf-8', 'Accept': 'text/json'}
            rejection_email_api = f'http://{host_name}:5002/rejection_email_api'
            requests.post(rejection_email_api, json=reject_file_json, headers=headers).json()
            logging.info(f"sent post request for rejection_email_api")
            logging.error("Cannot read the input file")

    except Exception as e:
        logging.info("something went wrong in junk removal")
        logging.info(str(e))
    return df


def generate_feed_sub_feed_bank(biz_master_df, df, account_number, stage, tenant_id):

    key_subfeed_map = json.loads(biz_master_df['Trans_descp_map'][0])
    def get_sub_feed(row):
        try:
            for key,sub_feed in key_subfeed_map.items():
                if str(key).lower() in str(row['Transaction Description']).lower():
                    row['Sub_Feed'] = sub_feed
                    return row
            row['Sub_Feed'] = 'Unidentified'
        except Exception as e:
            logging.error("Unable to fetch sub feed from transaction description")
            logging.error(e)
            row['Sub_Feed'] = 'Unidentified'
        return row

    business_rules_db = DB('business_rules', tenant_id=tenant_id, **db_config)
    try:
        feed_sub_df = business_rules_db.execute_default_index("SELECT * FROM [bank_feed_details] WHERE [account] = %s", params=[account_number])
    except:
        feed_sub_df = business_rules_db.execute_default_index("SELECT * FROM [bank_feed_details] WHERE [account] = %s", params=[str(account_number)])
    logging.info(f"Got feed sub_feed for the given account number : {feed_sub_df}")
    try:
        feed = feed_sub_df['feed'][0]
        sub_feed = feed_sub_df['sub_feed'][0]
        scheme = feed_sub_df['Scheme'][0]
    except Exception as e:
        logging.error("cannot get feed and sub_feed from table")
        logging.error(e)
        feed = ""
        sub_feed = ""
        scheme = ""
        
    df['Feed'] = feed 
    df['Scheme'] = scheme
    if sub_feed:
        print ("SUB FEED GOT FROM BANK", sub_feed)
        df['Sub_Feed'] = sub_feed
    else:
        df['Sub_Feed'] = ''
        df = df.apply(get_sub_feed, axis=1)
    
    return df

def generate_feed_id(biz_master_df, df, stage, tenant_id, type_="Feed"):
    business_rules_db = DB('business_rules', tenant_id=tenant_id, **db_config)
    if str(type_).lower().strip() == "feed":
        df['Feed_ID'] = ''
    else:
        df['Bank_ID'] = ''
    row_count = biz_master_df['row_count'][0]
    if str(type_).lower().strip() == 'feed':
        df['Feed_ID'] = df['Feed']+"_"+df['Sub_Feed']+"_"+(df.index + int(row_count)).astype(str)
        business_rules_db.execute_default_index("UPDATE [preprocessing_master] SET [row_count] = %s WHERE [sub_feed] = %s",params=[len(df)+list(biz_master_df['row_count'])[0], stage])
    else:
        df['Bank_ID'] = stage +"_"+(df.index + int(row_count)).astype(str)
        business_rules_db.execute_default_index("UPDATE [preprocessing_master] SET [row_count] = %s WHERE [Name] = %s",params=[len(df)+list(biz_master_df['row_count'])[0], stage])

    return df

def writeToCsv(biz_master_df, df, junk_removed_df, old_df, file_path_junk_removed, file_path_raw_, file_path_standard, file_path_rejected, tenant_id, stage, required_raw_mapping=None, required_standard_mapping=None, type_='Feed', unique_id = None, file_name=None):
    """Write the dataframe to csv"""
    print("required standard mapping got is")
    print(required_standard_mapping)
    logging.debug(f"LENGTH OF DF GOT TO WRITE TO CSV IS : {len(df)}")

    # creating file to store junk removed original file...
    try:
        junk_removed_df.to_csv(file_path_junk_removed, index=False, sep = "|")
    except Exception as e :
        logging.error("Filed in creating junk removed file")
        logging.error(e)

    # creating a life for rejected data...
    #rejected_df = old_df[old_df['Filter']=='N']
    rejected_df = df[df['Filter']=='N']
    #logging.info(f"Got rejected data is : {rejected_df}")
    try:
        rejected_df.to_csv(file_path_rejected, index=False, sep = "|")
    except Exception as e :
        logging.error("Failed in creating rejected file")
        logging.error(e)
    
    if not required_standard_mapping:
        required_standard_mapping = {ele:ele for ele in list(df.columns)}

    df = generate_feed_id(biz_master_df, df, stage, tenant_id, type_)
    logging.info("done generating feed")
    
    # write the processed_raw_files
    headers = True
    mode = 'w'
    logging.debug(f"got type is : {type_}")
    logging.debug(f"Required raw mapping is : {required_raw_mapping}")
    if str(type_).lower().strip() == 'feed':
        df['Raw_ID'] = df['Feed_ID']
        df['case_id'] = df['Feed_ID']
        # df = df.rename(columns=required_raw_mapping)
        required_columns_raw = list(required_raw_mapping.keys())
        #required_columns_raw = ['Feed_ID', 'Raw_Id', 'case_id'] + required_columns_raw + ['queue']
    else:
        df['Raw_ID'] = df['Bank_ID']
        df['case_id'] = df['Bank_ID']
        # df = df.rename(columns=required_raw_mapping)
        required_columns_raw = list(required_raw_mapping.keys())
        #required_columns_raw = ['Bank_ID', 'Raw_Id', 'case_id'] + required_columns_raw + ['queue']

    logging.debug(f"requrired columns raw are : {required_columns_raw}")
    logging.debug(f"LENGTH OF DF IN RAW IS : {len(df)}")
    logging.debug(f"column of input dataframe are : {df.columns}")
    try:
        map_stage = stage
        file_name  = file_name
        description = 'Generated raw processed file successfully'
        progress_stage = 'Processed File generation'
        case_id = None
        logging.debug(f"Got df to raw is : {df.head(5)}")
        # writing dataframe to csv raw ...
        df.to_csv(file_path_raw_, columns=list(required_columns_raw), mode=mode, header=list(required_raw_mapping.values()), index=False, sep = '|')
        # Uploading the progess stage of business rules ...
        progress_bar(unique_id,file_name,tenant_id,map_stage,case_id,progress_stage,description)
    except Exception as e:
        logging.debug(e)
        map_stage = stage
        file_name  = file_name
        description = 'Generation of raw file failed'
        progress_stage = 'Processed File generation'
        case_id = None
        # Uploading the progess stage of business rules ...
        progress_bar(unique_id,file_name,tenant_id,map_stage,case_id,progress_stage,description)
    
    if str(type_).lower().strip() == 'feed':
        try:
            standard_column_query = "SELECT [feed_standard_columns] FROM [master_data]"
            business_rules_db = DB('business_rules', tenant_id=tenant_id, **db_config)
            required_columns_standard = json.loads(business_rules_db.execute_default_index(standard_column_query)['feed_standard_columns'][0])
        except:
            logging.error("error in fetching required standard feed")
            required_columns_standard = ['Feed_ID', 'Feed', 'Sub_Feed', 'Date', 'Code', 'Filter', 'Amount', 'Matched' , 'ID', 'queue','Credit Received Date', 'Credit Received Time']
    else:
        # Unmatched amount should be matched amount
        swap_required_standard_mapping = {v:k for k,v in required_standard_mapping.items()}
        df['Unmatched_Amount'] = df[swap_required_standard_mapping['Amount']]
        try:
            standard_column_query = "SELECT [bank_standard_columns] FROM [master_data]"
            business_rules_db = DB('business_rules', tenant_id=tenant_id, **db_config)
            required_columns_standard = json.loads(business_rules_db.execute_default_index(standard_column_query)['bank_standard_columns'][0])
        except Exception as e:
            logging.debug(f"Cannot get standard columns from data base : {e}")
            logging.error("error in fetching required standard bank")
            required_columns_standard = ["Bank_ID", "Bank_Name", "Feed", "Sub_Feed", "Date", "Code", "Filter", "Amount", "Unmatched_Amount", "matched_amount", "ID", "queue", "Reference_number", "time_stamp", "Credit Received Date", "Credit Received Time"]
    """try:
        # Converting data types of columns according to standard table...
        df = type_conversion(biz_master_df, df, 'standard_column_type_mapping')
    except Exception as e :
        logging.error(f"Failed in conversion of data types for standard table")
        logging.error(e)
        df = df"""
    
    # rename the columns in the dataframe to standardized columns            
    if str(type_).lower().strip() == 'feed':
        required_columns_in_orig_df = list(required_standard_mapping.keys())
    else:
        required_columns_in_orig_df =  list(required_standard_mapping.keys())

    logging.debug(f"LENGTH OF DF IN STANDARD IS : {len(df)}")
    logging.debug(f"Required standard mapping is : {required_standard_mapping}")
    logging.debug(f"column of input dataframe are : {df.columns}")
    logging.debug(f"required columns in original df is : {required_columns_in_orig_df}")
    logging.debug(f"required columns standard is : {required_columns_standard}")

    try:
        map_stage = stage
        file_name  = file_name
        description = 'Generated standard file successfully'
        progress_stage = 'Data Standardization'
        case_id = None
        logging.info("writting to csv standard")
        # writing dataframe to csv standard ...
        df.to_csv(file_path_standard, columns=list(required_columns_in_orig_df), mode=mode, header=list(required_columns_standard), index=False, sep = '|')
        # Uploading the progess stage of business rules ...
        progress_bar(unique_id,file_name,tenant_id,map_stage,case_id,progress_stage,description)
    except Exception as e:
        logging.debug(e)
        map_stage = stage
        file_name  = file_name
        description = 'Generation of file failed'
        progress_stage = 'Data Standardization'
        case_id = None
        # Uploading the progess stage of business rules ...
        progress_bar(unique_id,file_name,tenant_id,map_stage,case_id,progress_stage,description)
                
    return "Written to csv successfully"


def assign_default_value(biz_master_df, df):
    # assigning required default values...
    default_values = json.loads(biz_master_df['default_values'][0])
    logging.info(f" Got default valuse to assign are : {default_values}")
    for column, value in default_values.items():
        df[column] = value
    logging.info("successfully assigned default values")
    return df

def generate_feed_sub_feed(biz_master_df, df, stage):
    df['Sub_Feed'] =  stage
    df['Feed'] = biz_master_df['Name'][0]
    return df

def conversion_code_apply(row, column, type_):
    try:
        if row[column] and row[column] != 'NA':
            if type_ == 'string' or type_ == 'varchar' or type_ == 'object':
                row[column] = str(row[column])
            elif type_ == 'float32' or type_ == 'float':
                row[column] = str(row[column]).replace(" ","")
                row[column] = float(str(row[column]).replace(",",""))
            elif type_ == 'float64':
                row[column] = str(row[column]).replace(" ","")
                row[column] = np.float64(str(row[column]).replace(",",""))
            elif type_ == 'int32' or type_ == 'int':
                row[column] = str(row[column]).replace(",","")
                row[column] = str(row[column]).replace(" ","")
                if "." in str(row[column]):
                    row[column] = float(row[column])
                row[column] = int(row[column])
            elif type_ == 'int64':
                row[column] = str(row[column]).replace(",","")
                row[column] = str(row[column]).replace(" ","")
                if "." in str(row[column]):
                    row[column] = float(row[column])
                row[column] = np.int64(row[column])

    except Exception as e:
        row['Filter'] = 'N'
        row['rejection_reason'] = str(str(row['rejection_reason'])+f"; Type conversion failed for {column}").lstrip(';')
        logging.error(f"error in type conversion")
        logging.error(e)
    return row

def type_conversion(biz_master_df, df, column_name = None):
    # converting specified columns to required formats...
    if column_name:
        try:
            df = df.fillna("")
            column_types = json.loads(biz_master_df[column_name][0])
            logging.info(f"Got type conversion data is : {column_types}")
        except Exception as e:
            logging.error("Cannot get type conversion columns data")
            logging.error(e)
        
        try:
            for column, type_ in column_types.items():
                try:
                    df = df.apply(conversion_code_apply, column=column, type_=type_, axis=1)
                except Exception as e:
                    logging.error(f"Unable to convert type for {column}")
                    logging.error(e)
            logging.info(f"dtypes after converted are : {df.dtypes}")
        except Exception as e:
            logging.error(f"Type conversion failed")
            logging.error(e)
        logging.info("Type conversion completed ")

    return df

def select_req_columns_df(biz_master_df, df):
   # selecting a dataframe with required columns... If required columns empty then returning same dataframe...
    req_cols_list = json.loads(biz_master_df['required_columns'][0])
    logging.info(f"in req col is{req_cols_list}")
    logging.info(f"input dataframe columns are ===> {list(df.columns)}")
    logging.info(f"required dataframe columns are ===> {req_cols_list}")
    if req_cols_list :
        try:
            req_df = df[req_cols_list]
            return req_df
        except Exception as e:
            logging.error(f"filtering of required columns failed")
            logging.error(e)
            return df
    else:
        return df

def dateConvertion(biz_master_df, df):
    #converting specified date columns to corresponding formats...
    try:
        date_format = json.loads(biz_master_df['date_column_format'][0])
    except Exception as e:
        date_format = pd.DataFrame()
        logging.error("Cannot get date conversion columns data")
        logging.error(e)

    date_first_formats = ["dd-mm-yy","dd-mm-yy hh:mm:ss","dd-mm-yyyy hh:mm:ss","dd/mm/yy hh:mm:ss","dd-yy-mm",
            "dd-mmm-yy","dd-yy-mmm","dd-mmm-yyyy","dd-yyyy-mm","dd-yyyy-mmm","dd/mm/yy","dd/yy/mm","dd/mm/yyyy","dd/mmm/yy"
            ,"dd/yy/mmm","dd/mmm/yyyy","dd/yyyy/mm","dd/yyyy/mmm","dd-mm-yyyy",
            "dd-mm-yyyy hh:mm","dd-Mmm-yyyy","dd-Mmm-yy","dd Mmm yyyy","dd/mm/yyyy hh:mm:ss AM/PM"
            ,"dd/mm/yyyy hh:mm:ss","dd.mm.yyyy","ddmmyyyy","dmmyyyy","dd.mm.yy"]

    date_last_formats = ["mm-yy-dd","yy-dd-mm","yy-mm-dd","mm-dd-yy","mm-yyyy-dd","mm-dd-yyyy","yyyy-dd-mm",
            "yyyy-mm-dd","mmm-yy-dd","mmm-dd-yy","yy-dd-mmm","yy-mmm-d","mmm-yyyy-dd","mmm-dd-yyyy","yyyy-dd-mmm",
            "yyyy-mmm-dd","mm/yy/dd","yy/dd/mm","yy/mm/dd","mm/dd/yy","mm/yyyy/dd","mm/dd/yyyy","yyyy/dd/mm",
            "yyyy/mm/dd","mmm/yy/dd","mmm/dd/yy","yy/dd/mmm","yy/mmm/dd","mmm/yyyy/dd","mmm/dd/yyyy","yyyy/dd/mmm",
            "yyyy/mmm/dd","yyyy-mm-dd hh:mm:ss+5:30","yyyy:mm:dd","yyyymmdd","mm/dd/yyyy hh:mm:ss AM/PM","mm/dd/yyyy hh:mm:ss"]
    
    time_mapping = {"hh:mm":"%H:%M", "hh:mm:ss":"%H:%M:%S", "hh:mm AM/PM":"%H:%M %p", "hh:mm:ss AM/PM":"%H:%M:%S %p",
                    "hh":"%H", "hh AM/PM":"%H %p"}

    for column, format_ in date_format.items():
        try:
            df[column] = df[column].astype(str)
            if format_ in  date_first_formats :
                try:
                    df[column] =  pd.to_datetime(df[column],dayfirst=True,errors='coerce').dt.strftime("%Y-%m-%d")
                except:
                    df[column] =  pd.to_datetime(df[column],dayfirst=True,errors='coerce',utc=True).dt.strftime("%Y-%m-%d")
                logging.info(f"dates of {column} after converted are : {df[column].head(5)}")

            elif format_ in date_last_formats :
                try:
                    df[column] =  pd.to_datetime(df[column],errors='coerce').dt.strftime("%Y-%m-%d")
                except:
                    df[column] =  pd.to_datetime(df[column],errors='coerce',utc=True).dt.strftime("%Y-%m-%d")
                logging.info(f"dates of {column} after converted are : {df[column].head(5)}")

            elif format_ in time_mapping.keys():
                df[column] =  pd.to_datetime(df[column],errors='coerce').dt.strftime(time_mapping[format_])
                logging.info(f"dates of {column} after converted are : {df[column].head(5)}")

            else:
                df['Filter'] = 'N'
                df['rejection_reason'] = (df['rejection_reason'] +"; "+f"Format {format_} is not in handled date formats for column {column}").astype(str).str.lstrip(';')
        except Exception as e:
            df['Filter'] = 'N'
            df['rejection_reason'] = (df['rejection_reason'] +"; "+f"Unable to parse the format {format_} for column {column}").astype(str).str.lstrip(';')
            logging.error(f"Date conversion of {column} is failed")
            logging.error(e)
    return df

def dict_column_split_value(biz_master_df, df):
    #converting specified date columns to corresponding formats...
    try:
        dict_columns = json.loads(biz_master_df['dict_column_split_value'][0])
        logging.info(f"dict_column_split_value data is {dict_columns}")
    except Exception as e:
        logging.error("Cannot get dict_column_split_value data")
        logging.error(e)
    if dict_columns != {} :
        try:
            sample_df = pd.DataFrame(df)
            sample_df[dict_columns["json_column"]] = sample_df[dict_columns["json_column"]].apply(lambda x: json.loads(x))
            col2_expanded = sample_df[dict_columns["json_column"]].apply(lambda x:pd.Series(x))
            logging.info(f"===>{col2_expanded}")
            sample_df[list(dict_columns["output_column_key"].keys())] = col2_expanded[list(dict_columns["output_column_key"].values())]
            sample_df[dict_columns["json_column"]] = ""
        except Exception as e:
            logging.error("ERROR IN dict_column_split_value data")
            logging.error(e)
            sample_df = df
        return sample_df
    else:
        return df

def assign_basedon_compare(biz_master_df, df):
    # assigning a value to a column based on specifc conditions...
    try:
        dict_columns = json.loads(biz_master_df['assign_basedon_compare'][0])
        logging.info(f"assign_basedon_compare data is {dict_columns}")
    except Exception as e:
        logging.error("Cannot get assign_basedon_compare data")
        logging.error(e)
    if dict_columns != {} :
        try:
            sample_df = pd.DataFrame(df)
            logging.info(f"=======>{dict_columns['compare_column_key'].items()}")
            for key, value in dict_columns['compare_column_key'].items():
                logging.info(f"<====={sample_df},{dict_columns['assign_column']}")
                sample_df[dict_columns['assign_column']] = sample_df[dict_columns['assign_column']].where(~(sample_df[dict_columns['from_column']] == key), value)
            logging.info(f'====>{sample_df}')
        except Exception as e:
            logging.error("error in assign_basedon_compare data")
            logging.error(e)
            sample_df = df
        return sample_df
    else:
        return df

def ffill_values(biz_master_df, df):
    # splitting merged columns for specifc columns...
    try:
        dict_columns = json.loads(biz_master_df['ffill_columns'][0])
        logging.info(f"ffill_values data is {dict_columns}")
    except Exception as e:
        logging.error("Cannot get ffill_values data")
        logging.error(e)
    if dict_columns:
        try:
            for column_map in dict_columns:
                column = column_map['column']
                method = column_map['method']
                logging.info(f"---->>>>{df[column]}")
                df[column] = df[column].fillna(method=method)
                logging.info(f"---->>>>{df[column]}")
            return df
        except Exception as e:
            logging.error("error in ffill_values data")
            logging.error(e)
            return df
    else:
        return df

def read_file(BR, file_path, biz_master_df, map_stage, look_ups={}, unique_id=None, file_name=None, tenant_id=None):
    logging.info(f"file path is {file_path}")
    logging.info(f"split is {file_path.split('.')}")
    file_extension = file_path.split(".")[-1]
    logging.info(f"inside function")
    logging.info(f"file extension is {file_extension}")
    try:
        if  file_extension == 'csv':
            logging.info(f"inside csv")
            try:
                BR.data_source['master'] = pd.read_csv(file_path)
            except Exception as e:
                logging.info("Unable to read csv trying with open")
                with open(file_path) as f:
                    content = f.readlines()
                    logging.debug(f"content is : {content}")

                headers_list = json.loads(biz_master_df['headers'][0]) 
                logging.info(f"got list is : {headers_list}")
                header_str = ""
                row_id = 0
                for header in headers_list:
                    header_str = header_str+str(header)+","
                header_str = header_str.rstrip(",")
                logging.info(f"Converted headers list is : {header_str}")
                for c in content:
                    if (c).rstrip("\n") == header_str:
                        row_id = content.index(c)
                        break
                if row_id:
                    BR.data_source['master'] = pd.read_csv(file_path, skiprows = row_id)
                else:
                    logging.error("Unable to read file with open")
                        
        elif file_extension == 'txt':
            BR.data_source['master'] = pd.read_csv(file_path, sep='|')
        elif file_extension == 'xlsx' or file_extension == 'xls' or file_extension == 'xlsm':
            is_sheet_name = False
            try:
                #getting sheet name from database...
                sheet_name =  biz_master_df['sheetname'][0]
            except:
                sheet_name = ""
            if sheet_name:
                is_sheet_name = True
                logging.info(f"got the sheet name {sheet_name}")
            
            #if sheet_name exists then read that sheet or else read first sheet defaultly
            if is_sheet_name:
                try:
                    BR.data_source['master'] = pd.read_excel(file_path, sheet_name)
                except:
                    BR.data_source['master'] = pd.read_csv(file_path, sheet_name, sep='\t')
            else:
                try:
                    BR.data_source['master'] = pd.read_excel(file_path)
                except:
                    BR.data_source['master'] = pd.read_csv(file_path, sep='\t')
            
            #holiday files for cms...
            for table, holiday_file_path in look_ups.items():
                try:
                    BR.data_source[table] = pd.read_excel(holiday_file_path)
                except:
                    BR.data_source[table] = pd.read_csv(holiday_file_path, sep='\t')

        elif file_extension == 'xlsb':
            df = []
            is_sheet_name = False
            try:
                sheet_name =  biz_master_df['sheetname'][0]
            except:
                sheet_name = ""
            if sheet_name:
                is_sheet_name = True
                #sheet_name = biz_master_df['sheetname'][0]
                logging.info(f"got the sheet name {sheet_name}")
            
            with open_xlsb(file_path) as wb:
                with wb.get_sheet(sheet_name) as sheet:
                    for row in sheet.rows():
                        df.append([item.v for item in row])

            df = pd.DataFrame(df[1:], columns=df[0])
            BR.data_source['master'] = df
        logging.info(f"master data frame is : {BR.data_source['master']}")

        map_stage = map_stage
        file_name  = file_name
        description = 'File validated Successfully'
        stage = 'File Validation'
        case_id = None
        # Uploading the progess stage of business rules ...
        progress_bar(unique_id,file_name,tenant_id,map_stage,case_id,stage,description)
    except Exception as e :

        stage = str(map_stage)
        reject_files = [file_path]
        reject_file_json = {"topic": 'rejection_email_api', "tenant_id": 'karvy',"rejection_reason":"Unable to parse the file",
                                    "rejected_file_path": reject_files, "stage": stage}
        logging.debug(reject_file_json)
        host_name = os.environ['HOST_IP']
        headers = {'Content-type': 'application/json; charset=utf-8', 'Accept': 'text/json'}
        rejection_email_api = f'http://{host_name}:5002/rejection_email_api'
        requests.post(rejection_email_api, json=reject_file_json, headers=headers).json()
        logging.info(f"sent post request for rejection_email_api")
        logging.error("Cannot read the input file")
        logging.error(e)

        map_stage = map_stage
        file_name  = file_name
        description = 'Corrupt/Wrong FORMAT file got.'
        stage = 'File Validation'
        case_id = None
        progress_bar(unique_id,file_name,tenant_id,map_stage,case_id,stage,description)
        # Uploading the progess stage of business rules...
        file_receipt_query = "INSERT INTO [file_receipt_report] ([Folder_Name], [File_Name], [rejected_reason]) VALUES (%s, %s, %s)"
        params = [map_stage, file_name, "CORRUPT FILE or NOT SUPPORTED FORMAT"]
        # updating report as if failed in reading file...
        reports_db = DB('reports', tenant_id=tenant_id, **db_config)
        reports_db.execute(file_receipt_query, params=params)

def strip_date_time(df, column, new_column_date, new_column_time):
    df[column] = pd.to_datetime(df[column], dayfirst=True, errors='coerce')
    df[new_column_date] = df[column].dt.date
    df[new_column_time] = df[column].dt.time
    return df

def consider_amount_negative(df, amount_column, credit_column, credit_val):
    df[amount_column] = df[amount_column].astype(float)
    df[amount_column] = df[amount_column].where(df[credit_column] != credit_val, df[amount_column]*-1)
    return df

def append_rejection_reason(df, t_value, reason):
    try:
        df['rejection_reason'] = df['rejection_reason'].where(t_value,df['rejection_reason']+'; '+reason).astype(str).str.lstrip(';')
    except Exception as e:
        logging.error("Failed in assigning error message")
        logging.error(e)
    return df

def filter_rows(df, account_no, account_no_filter_map, filter_field):
    filter_values = []
    if account_no in account_no_filter_map:
        filter_values = account_no_filter_map[account_no]
    t_value = pd.Series([False]*len(df))
    for val in filter_values:
        t_value = t_value | df[filter_field].astype(str).str.lower().str.contains(val.lower())
    if filter_values:
        print(f"filter values are{filter_values}")
        df['Filter'] = df['Filter'].where(t_value, 'N')
        df = append_rejection_reason(df, t_value, 'Rejected due to missing values in '+filter_field)
    return df

def sub_feed_generation(df, account_no, sub_feed_map, filter_field):
    filter_values = []
    if account_no in sub_feed_map:
        filter_values = sub_feed_map[account_no].keys()
    for val in filter_values:
        df.loc[df[filter_field].astype(str).str.lower().str.contains(val.lower()), 'Sub_Feed'] = sub_feed_map[account_no][val]
    return df

def axis_code_apply(row):
    if row['Filter'] == 'N':
        return row
    if str(row['Debit']) == 'nan' or (not str(row['Debit']).strip()):
        row['Created Amount'] = row['Credit']
    else:
        row['Created Amount'] = row['Debit']
    try:
        if str(row['Account Number']) == '004010202190088' or str(row['Account Number']) == '4010202190088' :
            row['Code'] = str(row['Particulars']).split("/")[1]
            return row
        elif str(row['Account Number']) == '917020082397356':
            row['Code'] = 'AO99'
            if str(row['Particulars'])[-1] == '#':
                row['Sub_Feed'] = 'AX99'
                row['Code'] = str(row['Particulars']).split("/")[1]
            if str(row['Particulars'])[-1] != '#':
                row['Filter'] = 'N'
                row['rejection_reason'] = str(str(row['rejection_reason'])+"; Particulars does not end with #").lstrip(';')
            return row
        else:
            row['Code'] = row['Particulars']
    except:
        return row
    
    return row
    
def axis_bank(axis_df, tenant_id, accno):
    axis_df['Code'] = ''
    axis_df['Created Amount'] = ''
    axis_df = strip_date_time(axis_df, 'Tran Time', 'Created Date', 'Time')
    t_value = ~axis_df['Credit'].isnull()
    axis_df['Filter'] = axis_df['Filter'].where(~axis_df['Credit'].isnull(), 'N')
    axis_df = append_rejection_reason(axis_df, t_value, "Transaction is not a credit transaction")
    axis_df = axis_df.apply(axis_code_apply, axis=1)
    #t_value = ~(axis_df['Credit'].isnull()| axis_df['Debit'].isnull())
    #axis_df['Filter'] = axis_df['Filter'].where(t_value, 'N')
    #axis_df = append_rejection_reason(axis_df, t_value, "Transaction is a debit transaction")
    t_value = ~(axis_df['Created Amount'].isnull())
    axis_df['Filter'] = axis_df['Filter'].where(t_value, 'N')
    axis_df = append_rejection_reason(axis_df, t_value, "Not a transaction")
    account_no_filter_map = {
            "917020082397356":["ARN-64610"]
        }
    sub_feed_map = {
        "917020082397356":{"ARN-64610":"CPDIRECT_AO99"}
    }
    axis_df = filter_rows(axis_df, accno, account_no_filter_map, 'Particulars')
    axis_df = sub_feed_generation(axis_df, accno, sub_feed_map, 'Particulars')
    return axis_df

def citi_code_apply(row):
    if row['Filter'] == 'N':
        return row
    try:
        
        if str(row['Account Number']) == '036498307' or str(row['Account Number']) == '36498307' :
            row['Code'] = ''
            return row
        elif str(row['Account Number']) == '0036498676' or str(row['Account Number']) == '36498676':
            row['Code'] = 'CB99'
            return row
        else:
            row['Code'] = row['Narrative']
    except:
        return row
    return row

def citi_bank(citi_df, tenant_id, accno):
    citi_df['Code'] = ''
    t_value = (citi_df['Amount'] > 0)
    citi_df['Filter'] = citi_df['Filter'].where(t_value, 'N')
    citi_df = append_rejection_reason(citi_df, t_value, "Got negative Amount in transaction")
    citi_df['Posted Time'] = pd.to_datetime(citi_df['Posted Time'],  errors='coerce')
    #citi_df['Entry Date'] =  pd.to_datetime(citi_df['Entry Date'],dayfirst=True,errors='coerce').dt.strftime("%d-%m-%Y")
    try:
        citi_df['Time'] = citi_df['Posted Time'].dt.time
    except:
        citi_df['Posted Time'] = pd.to_datetime(citi_df['Posted Time'],  errors='coerce', utc=True)
        citi_df['Time'] = citi_df['Posted Time'].dt.time
    citi_df['Created Date'] = pd.to_datetime(citi_df['Posted Time']).dt.strftime('%Y-%m-%d')
    citi_df = citi_df.apply(citi_code_apply, axis=1)
    
    account_no_filter_map = {
            "0036498676":["MF SUBN IN MIRAE FUND","SUBN"],
            "036498307":["PAYOUTS"],
            "36498676":["MF SUBN IN MIRAE FUND"],
            "36498307":["PAYOUTS"]
        }
    sub_feed_map = {
            "0036498676":{"SUBN":"CPDIRECT_CB99"}
            }
    #citi_df = generate_feed_id(citi_df, 'HDFC', tenant_id, type_='Bank')
    citi_df = filter_rows(citi_df, accno, account_no_filter_map, 'Narrative')
    citi_df = sub_feed_generation(citi_df, accno, sub_feed_map, 'Narrative')
    return citi_df

def sbi_code_apply(row):
    if row['Filter'] == 'N':
        return row
    if str(row['Account Number']) == '00000035931670955' or str(row['Account Number']) == '35931670955' :
            row['Code'] = str(row['Ref No./Cheque No.'])[14:23]
    if str(row['Debit']) == 'nan' or (not str(row['Debit']).strip()):
        row['Created Amount'] = row['Credit']
    else:
        row['Created Amount'] = row['Debit']
    
    return row

def sbi_bank(sbi_df, tenant_id, accno):
    sbi_df['Created Amount'] = ''
    sbi_df['Value Date'] = pd.to_datetime(sbi_df['Value Date'],  errors='coerce')
    sbi_df['Time'] = sbi_df['Value Date'].dt.time
    sbi_df['Created Date'] = pd.to_datetime(sbi_df['Value Date']).dt.strftime('%Y-%m-%d')
    t_value = (sbi_df['Credit'].astype(str).str.strip().isnull())
    sbi_df['Filter'] = sbi_df['Filter'].where(t_value, 'N')
    sbi_df = append_rejection_reason(sbi_df, t_value, "Transaction is a credit transaction")
    sbi_df = sbi_df.apply(sbi_code_apply,axis=1)
    t_value = ~(sbi_df['Created Amount'].isnull())
    sbi_df['Filter'] = sbi_df['Filter'].where(t_value, 'N')
    sbi_df = append_rejection_reason(sbi_df, t_value, "Created amount is empty")
    account_no_filter_map1 = {
            "00000038526350026":["/"]
        }
    sub_feed_map1 = {
            "00000038526350026":{"/":"CPDIRECT_ME99"}
            }
    account_no_filter_map2 = {
            "00000038526350026":["SBI WEALTH"],
            "00000035931670955":["BY TRANSFER-INB Mirae Asset Global Invest BILL_RTCOI Payments"]
        }
    sub_feed_map2 = {
            "00000038526350026":{"SBI WEALTH":"CPDIRECT_ME99"}
            }
    sbi_df = filter_rows(sbi_df, accno, account_no_filter_map1, 'Ref No./Cheque No.')
    sbi_df = filter_rows(sbi_df, accno, account_no_filter_map2, 'Description')
    sbi_df = sub_feed_generation(sbi_df, accno, sub_feed_map1, 'Ref No./Cheque No.')
    sbi_df = sub_feed_generation(sbi_df, accno, sub_feed_map2, 'Description')
    return sbi_df

def yes_code_apply(row):
    if row['Filter'] == 'N':
        return row
    try:
        if str(row['Account Number']) == '026885700000743' or str(row['Account Number']) == '026885700000729':
            row['Code'] = row['Transaction Description']
            return row
        if str(row['Account Number']) == '026885700000814':
            row['Code'] = 'YB99'
            return row
        else:
            row['Code'] = row['Reference No.']
    except:
        return row
    
    return row

def yes_bank(yes_df, tenant_id, accno):
    yes_df['Transaction Date'] = pd.to_datetime(yes_df['Transaction Date'], dayfirst='True', errors='coerce')
    yes_df['Time'] = yes_df['Transaction Date'].dt.time
    yes_df['Created Date'] = pd.to_datetime(yes_df['Transaction Date']).dt.strftime('%Y-%m-%d')
    yes_df['Code'] = ''
    yes_df = consider_amount_negative(yes_df, 'Transaction Amount', 'Debit / Credit', 'D')
    t_value = (yes_df['Debit / Credit'] == 'C')
    yes_df['Filter'] = yes_df['Filter'].where(t_value, 'N')
    yes_df = append_rejection_reason(yes_df, t_value, "Transaction is not a credit transaction")
    yes_df = yes_df.apply(yes_code_apply, axis=1)
    account_no_filter_map = {
            "26885700000814":["Funds Trf from XX3012/IB: 117"]
        }
    sub_feed_map = {
            "26885700000814":{"Funds Trf from XX3012/IB: 117":"CPDIRECT_YB99"}
            }
    yes_df = filter_rows(yes_df, accno, account_no_filter_map, 'Transaction Description')
    yes_df = sub_feed_generation(yes_df, accno, sub_feed_map, 'Transaction Description')
    return yes_df

def kotak_code_apply(row):
    if row['Filter'] == 'N':
        return row
    
    if str(row['Withdrawal']) == 'nan' or (not str(row['Withdrawal']).strip()):
        row['Created Amount'] = row['Deposit']
    else:
        row['Created Amount'] = row['Withdrawal']
    
    try:
        
        if str(row['Account Number']) == '09582540006664':
            row['Code'] = str(row['Chq / Ref number']).split("-")[1]
            return row
        elif str(row['Account Number']) == '09582540006786':
            row['Code'] = 'KB99'
            return row
        else:
            row['Code'] = row['Description']
            
    except:
        return row
    
    return row

def kotak_bank(kotak_df,tenant_id, accno):
    kotak_df['Code'] = ''
    kotak_df['Created Amount'] = ''
    t_value = ~(kotak_df['Deposit'].isnull())
    kotak_df['Filter'] = kotak_df['Filter'].where(t_value, 'N')
    kotak_df = append_rejection_reason(kotak_df, t_value, "Deposit is null")
    kotak_df['Date'] = pd.to_datetime(kotak_df['Date'],  errors='coerce')
    kotak_df['Time'] = kotak_df['Date'].dt.time
    kotak_df['Created Date'] = pd.to_datetime(kotak_df['Date']).dt.strftime('%Y-%m-%d')
    kotak_df = kotak_df.apply(kotak_code_apply, axis=1)
    t_value = ~(kotak_df['Created Amount'].isnull())
    kotak_df['Filter'] = kotak_df['Filter'].where(t_value, 'N')
    kotak_df = append_rejection_reason(kotak_df, t_value, "Created Amount is null")
    account_no_filter_map = {
            "09582540006786":["06410910000043","25242001"]
        }
    sub_feed_map = {
            "09582540006786":{"06410910000043":"KB99","25242001":"CPDIRECT_KB99"}
            }
    kotak_df = filter_rows(kotak_df, accno, account_no_filter_map, 'Transaction Description')
    kotak_df = sub_feed_generation(kotak_df, accno, sub_feed_map, 'Transaction Description')
    logging.debug(f"returning kotak df : {kotak_df.head(5)}")
    return kotak_df

def hdfc_code_apply(row):
    if str(row['Account Number']) == '00600350113104' and ((str(row['Transaction Description'])[0:3]).lower() == "ipl" or (str(row['Transaction Description'])[0:3]).lower() == "mir"):
        row['Sub_Feed'] = 'CPVIRTUAL_IT99'

    if row['Feed'].lower() == 'CPVIRTUAL'.lower():
        row['Code'] = row['Reference No.']
        return row
    if row['Feed'].lower() == 'CMS'.lower():
        try:
            account_nos = ['00600350117285', '57500000197162', '00600350087152', '57500000207581']
            if str(row['Account Number']) in account_nos:
                row['Filter'] = 'Y'
            row['Code'] = str(row['Transaction Description']).split("-")[1]
        except Exception as e:
            logging.error("Index out of bond")
            row['Code'] = row['Transaction Description']
            logging.error(e)
        return row
    if str(row['Account Number']) == '00990620012131' or str(row['Account Number']) == '00600350051109' or str(row['Account Number']) == '00990610016361' or str(row['Account Number']) == '57500000090687' or str(row['Account Number']) == '57500000090303':
        row['Code'] = ''
    elif str(row['Account Number']) == '50200005161316' or str(row['Account Number']) == '00602090003198' or str(row['Account Number']) == '602090003198':
        row['Code'] = row['Reference No.']
    elif str(row['Account Number']) == '00602090003027':
        row['Code'] = 'RT99'
    elif str(row['Account Number']) == '00602090003051':
        row['Code'] = 'HD99'
    elif str(row['Account Number']) == '00602090003363':
        row['Code'] = 'SK99'
    else:
        row['Code'] =  row['Transaction Description']
    
    return row
        

def hdfc_bank(hdfc_df, tenant_id, accno):
    #hdfc_df['Filter'] = 'Y'
    hdfc_df['Code'] = ''
    t_value = (hdfc_df['Debit / Credit'] == 'C')
    hdfc_df['Filter'] = hdfc_df['Filter'].where(t_value, 'N')
    hdfc_df = append_rejection_reason(hdfc_df, t_value, "Transaction is not a credit transaction")
    hdfc_df = consider_amount_negative(hdfc_df, 'Transaction Amount', 'Debit / Credit', 'D')
    account_no_filter_map = {
            "00600350117285":["MAMFIO","MAMFCM","MAMFEB","MAMFPF","MAMFFS","MAMFTF","MAMFSB","MAMFGF","MAMFUS","MIRAEFNX","MAMFHC","MAMFEQSA","MAMFDB","MAMFMC"],
            "57500000197162":["MAMFIO","MAMFCM","MAMFEB","MAMFPF","MAMFFS","MAMFTF","MAMFSB","MAMFGF","MAMFUS","MIRAEFNX","MAMFHC","MAMFEQSA","MAMFDB","MAMFMC"],
            "57500000207581":["MAMFIO","MAMFCM","MAMFEB","MAMFPF","MAMFFS","MAMFTF","MAMFSB","MAMFGF","MAMFUS","MIRAEFNX","MAMFHC","MAMFEQSA","MAMFDB","MAMFMC"],
            "00600350087152":["MAMFIO","MAMFCM","MAMFEB","MAMFPF","MAMFFS","MAMFTF","MAMFSB","MAMFGF","MAMFUS","MIRAEFNX","MAMFHC","MAMFEQSA","MAMFDB","MAMFMC"],
            "00990620012131":["PAY-OUT","00990640000982"],
            "00600350051109":["50200009635552"],
            "00990610016361":["MFSS Normal Pay-out S"],
            "00602090003198":["KARVY DATA MAN"],
            "57500000090687":["Mirae Asset ISIP Collection"],
            "57500000090303":["MIRAE ASSET SIP COLLECTION"],
            "00600350113104":["DEUT","IPL","Mir","Cholaman","DC99","Getclar","Kotak","OF99","Reliance","Wealth N","WU99","CI99","FI99","GW99","RBL","FN99","WL99","RR Finan","BW99","MIRAEMFAA99","WF99","RAZORPAY SOFTWARE PRIVATE LIMITED","MFU SIP"]
        }
    sub_feed_map = {
            "00600350113104":{"DEUT":"CPVIRTUAL_DU99","Cholaman":"CPVIRTUAL_CL99","DC99":"CPVIRTUAL_DC99","Getclar":"CPVIRTUAL_GT99","Kotak":"CPVIRTUAL_KT99","OF99":"CPVIRTUAL_OF99","Reliance":"CPVIRTUAL_RE99","Wealth N":"CPVIRTUAL_WN99","WU99":"CPVIRTUAL_WU99","CI99":"CPVIRTUAL_CI99",
            "FI99":"CPVIRTUAL_FI99","GW99":"CPVIRTUAL_GW99","RBL":"CPVIRTUAL_TL99","FN99":"CPVIRTUAL_FN99","WL99":"CPVIRTUAL_WL99","RR Finan":"CPVIRTUAL_RR99","BW99":"CPVIRTUAL_BW99","MIRAEMFAA99":"CPVIRTUAL_FR99","WF99":"CPVIRTUAL_WF99","RAZORPAY SOFTWARE PRIVATE LIMITED":"CPVIRTUAL_DA99","MFU SIP":"MFU_SIP"},
            "00990620012131":{"PAY-OUT:CR":"EXCHANGE_BSE_PAYOUT","00990640000982":"EXCHANGE_BSE_L0L1"}
            }

    hdfc_df = filter_rows(hdfc_df, accno, account_no_filter_map, 'Transaction Description')
    hdfc_df = sub_feed_generation(hdfc_df, accno, sub_feed_map, 'Transaction Description')
    hdfc_df = hdfc_df.apply(hdfc_code_apply, axis=1)
    hdfc_df['Transaction Date'] = pd.to_datetime(hdfc_df['Transaction Date'],  errors='coerce')
    hdfc_df['Time'] = hdfc_df['Transaction Date'].dt.time
    hdfc_df['Created Date'] = pd.to_datetime(hdfc_df['Transaction Date']).dt.strftime('%Y-%m-%d')
    return hdfc_df

def icici_code_apply(row):
    if row['Filter'] == 'N':
        return row
    try:
        
        if str(row['Account Number']) == '039305001824' or str(row['Account Number']) == '39305001824' or str(row['Account Number']) == '39305001822' or str(row['Account Number']) == '039305001822':
            row['Code'] = str(row['Description']).split("/")[2][2:]
            return row
        elif str(row['Account Number']) == '39305001943' or str(row['Account Number']) == '039305001943':
            row['Code'] = str(row['Description']).split("/")[1]
            return row
        elif str(row['Account Number']) == '039305001855':
            row['Code'] = 'IW99'
            return row
        else:
            row['Code'] = row['Description']
    except:
        return row
    
    return row

def icici_bank(icici_df,tenant_id):
    icici_df['Code'] = ''
    icici_df['Txn Posted Date'] = pd.to_datetime(icici_df['Txn Posted Date'],  errors='coerce')
    icici_df['Time'] = icici_df['Txn Posted Date'].dt.time
    icici_df['Created Date'] = pd.to_datetime(icici_df['Txn Posted Date']).dt.strftime('%Y-%m-%d')
    t_value = (icici_df['Cr/Dr'] == 'CR')
    icici_df['Filter'] = icici_df['Filter'].where(t_value, 'N')
    icici_df = append_rejection_reason(icici_df, t_value, "Transaction is not a credit transaction")
    icici_df = consider_amount_negative(icici_df, 'Transaction Amount(INR)', 'Cr/Dr', 'DR')
    icici_df = icici_df.apply(icici_code_apply, axis=1)
    return icici_df

def idbi_code_apply(row):
    if row['Filter'] == 'N':
        return row
    try:
        
        if str(row['Account Number']) == '004103000026965' or str(row['Account Number']) == '4103000026965':
            row['Code'] = str(row['Description']).split("/")[3]
            return row
        else:
            row['Code'] = row['Description']
    except:
        return row
    
    return row

def idbi_bank(idbi_df,tenant_id):
    idbi_df['Code'] = ''
    t_value = (idbi_df['CR/DR'] == 'Cr.')
    idbi_df['Filter'] = idbi_df['Filter'].where(t_value, 'N')
    idbi_df = append_rejection_reason(idbi_df, t_value, "Transaction is not a credit transaction")
    idbi_df['Time'] = pd.to_datetime(idbi_df['Txn Date'], format= '%H:%M:%S', errors='coerce').dt.time
    idbi_df['Txn Time'] = idbi_df['Time']
    idbi_df['Txn Date_'] = pd.to_datetime(idbi_df['Txn Date']).dt.strftime('%Y-%m-%d')
    idbi_df['Txn Date'] = pd.to_datetime(idbi_df['Txn Date'],  errors='coerce')
    idbi_df['Created Date'] = idbi_df['Value Date']
    idbi_df = consider_amount_negative(idbi_df, 'Amount (INR)', 'CR/DR', 'DR')
    idbi_df = idbi_df.apply(idbi_code_apply, axis=1)
    return idbi_df

def db_code_apply(row):
    try:
        if float(row) > 0:
            return True
        else:
            return False
    except Exception as e:
        logging.error(e)
        return False

def db_bank(db_df,tenant_id):
    t_value = db_df['AMOUNT'].apply(db_code_apply)
    db_df['Filter'] = db_df['Filter'].where(t_value, 'N')
    db_df = append_rejection_reason(db_df, t_value, "Transaction is debit transaction")
    db_df['CONROL DATE'] = pd.to_datetime(db_df['CONROL DATE'],  errors='coerce')
    db_df['Time'] = db_df['CONROL DATE'].dt.time
    db_df['Created Date'] = pd.to_datetime(db_df['CONROL DATE']).dt.strftime('%Y-%m-%d')
    return db_df
    
def syndi_code_apply(row):
    if row['Filter'] == 'N':
        return row
    if str(row['Debit']) == 'nan' or (not str(row['Debit']).strip()):
        row['Created Amount'] = row['Credit']
    else:
        row['Created Amount'] = row['Debit']
    return row

def syndi_bank(syndi_df, tenant_id, accno):
    logging.info(f"syndicate bank is :{syndi_df}")
    syndi_df['Created Amount'] = ''
    t_value = ~(syndi_df['Credit'].isnull())
    syndi_df['Filter'] = syndi_df['Filter'].where(t_value, 'N')
    syndi_df = append_rejection_reason(syndi_df, t_value, "Transaction is not a credit transaction")
    syndi_df = syndi_df.apply(syndi_code_apply, axis=1)
    t_value = ~(syndi_df['Created Amount'].isnull())
    syndi_df['Filter'] = syndi_df['Filter'].where(t_value, 'N')
    syndi_df = append_rejection_reason(syndi_df, t_value, "Created amount is null")
    account_no_filter_map = {
            "50001010022338":["ARMY GROUP INSURAN"]
        }
    syndi_df = filter_rows(syndi_df, accno, account_no_filter_map, 'Description')

    return syndi_df

def dbs_bank(dbs_df, tenant_id, accno):
    account_no_filter_map = {
            "811200209304":["DGB","IN"]
        }
    sub_feed_map = {
            "811200209304":{"DGB":"CPDIRECT_DN99","IN":"CPDIRECT_DN99"}
            }
    dbs_df = filter_rows(dbs_df, accno, account_no_filter_map, 'Transaction Description 2')
    dbs_df = sub_feed_generation(dbs_df, accno, sub_feed_map, 'Transaction Description 2')

    return dbs_df

def idfc_bank(idfc_df, tenant_id, accno):
    idfc_df = strip_date_time(idfc_df, 'Transaction Date', 'Created Date', 'Time')
    account_no_filter_map = {
            "10034385204":["10034385204"]
        }
    sub_feed_map = {
            "10034385204":{"10034385204":"CPDIRECT_ID99"}
            }
    idfc_df = filter_rows(idfc_df, accno, account_no_filter_map, 'Narrative')
    idfc_df = sub_feed_generation(idfc_df, accno, sub_feed_map, 'Narrative')
    return idfc_df

def apply_karvy_specific_bank_rules(df, map_stage, tenant_id, account_number):
    df['Bank_Name'] = map_stage+" Bank"
    print(f"account number :{account_number}")
    df['Account Number'] = account_number
    tenant_id = 'karvy'
    if map_stage == 'AXIS':
        return axis_bank(df,tenant_id,account_number)
    elif map_stage == 'CITI':
        return citi_bank(df,tenant_id,account_number)
    elif map_stage == 'SBI':
        return sbi_bank(df,tenant_id, account_number)
    elif map_stage == 'HDFC':
        return hdfc_bank(df,tenant_id,account_number)
    elif map_stage == 'YES':
        return yes_bank(df,tenant_id, account_number)
    elif map_stage == 'ICICI':
        return icici_bank(df,tenant_id)
    elif map_stage == 'IDBI':
        return idbi_bank(df,tenant_id)
    elif map_stage == 'SYNDICATE':
        return syndi_bank(df, tenant_id, account_number)
    elif map_stage == 'KOTAK':
        return kotak_bank(df,tenant_id, account_number)
    elif map_stage == 'DB':
        return db_bank(df,tenant_id)
    elif map_stage == 'DBS':
        return dbs_bank(df, tenant_id, account_number)
    elif map_stage == 'IDFC':
        return idfc_bank(df, tenant_id, account_number)
    else:
        return df

# as of now run this...you can combine the run_chained_rules and column_chained rules with small changes
def run_chained_rules_column(file_path, chain_rules, tenant_id, map_stage, look_ups={}, start_rule_id=None, unique_id=None):
    """Execute the chained rules column wise"""

    # get the mapping of the rules...basically a rule_id maps to a rule
    rule_id_mapping = {}
    for ind, rule in chain_rules.iterrows():
        rule_id_mapping[rule['rule_id']] = [rule['rule_string'], rule['next_if_sucess'], rule['next_if_failure'], rule['stage'], rule['description'], rule['data_source']]
    logging.info(f"\n rule id mapping is \n{rule_id_mapping}\n")
    
    # evaluate the rules one by one as chained
    # start_rule_id = None
    if start_rule_id is None:
        if rule_id_mapping.keys():
            start_rule_id = list(rule_id_mapping.keys())[0]
    #map_stage = str(map_stage).lower()

    # getting data from preprocessing master for particular feed
    get_master_query = "SELECT * FROM [preprocessing_master] WHERE [folder_name] = %s "
    params = [map_stage]
    business_rules_db = DB('business_rules', tenant_id=tenant_id, **db_config)
    biz_master_df = business_rules_db.execute_default_index(get_master_query, params=params)
    biz_master_df = biz_master_df.reset_index(drop=True)
    logging.info(F"Got data from preprocessing master is : {biz_master_df}")

    # generate the final file name
    file_path_junk_removed = file_path[:-4]+"_junk_removed_"+ str(time.mktime(datetime.datetime.today().timetuple()))[:-2]+".csv"
    file_path_raw_ = file_path[:-4]+"_raw_processed_"+ str(time.mktime(datetime.datetime.today().timetuple()))[:-2]+".csv"
    file_path_standard = file_path[:-4]+"_standard_processed_"+ str(time.mktime(datetime.datetime.today().timetuple()))[:-2]+".csv"
    file_path_rejected = file_path[:-4]+"_rejected_processed_"+ str(time.mktime(datetime.datetime.today().timetuple()))[:-2]+".csv"
    
    BR  = BusinessRules(None, [], {})
    file_name = path_leaf(file_path)[:-4] # stripping the .csv
    BR.file_path = file_name
    BR.junk_removed_file_name_generated = file_path_junk_removed
    BR.raw_file_name_generated = file_path_raw_
    BR.standard_file_name_generated = file_path_standard
    BR.rejected_file_name_generated = file_path_rejected

    # reading input file...
    read_file(BR, file_path, biz_master_df, map_stage, look_ups, unique_id=unique_id, file_name=file_name, tenant_id=tenant_id)
    
    type_ = biz_master_df['type_'][0]

    # removing junk present on the top of the dataframe...
    BR.data_source['master'] = get_new_df(biz_master_df, BR.data_source['master'], map_stage, file_path)

    # stripping column names of input dataframe...
    master_df_columns = list(BR.data_source['master'].columns)
    master_df_columns_strip_map = {ele:str(ele).strip() for ele in master_df_columns}    
    BR.data_source['master'] =  BR.data_source['master'].rename(columns=master_df_columns_strip_map)
    logging.info(f"converting column names : {BR.data_source['master'].columns}")

    # storing original df after removing junk and stripping column names...
    junk_removed_df = BR.data_source['master']

    # droping all the rows which are empty...
    BR.data_source['master'] = BR.data_source['master'].dropna(axis=0, how='all')

    # storing original data frame before droping columns,to generate rejected file ...
    #should maintain if required columns are not same as input columns...
    old_df = BR.data_source['master']
    # selecting only required columns from the dataframe...
    BR.data_source['master']  = select_req_columns_df(biz_master_df, BR.data_source['master'])
    # assigning required default values... 
    BR.data_source['master'] = assign_default_value(biz_master_df, BR.data_source['master'])

    BR.data_source['master'] = ffill_values(biz_master_df, BR.data_source['master'])

    # converting specified columns to required formats...
    BR.data_source['master'] = type_conversion(biz_master_df, BR.data_source['master'], 'required_column_type_mapping')

    # converting dates to single format...
    BR.data_source['master'] = dateConvertion(biz_master_df, BR.data_source['master'])

    # ffill columns 

    if str(type_).lower().strip() == 'bank':
        #df = get_new_df(biz_master_df, BR.data_source['master'], map_stage)
        # Getting account number from file path...
        BR.data_source['master'], account_number = get_account_no(BR.file_path, BR.data_source['master'])
        logging.info(f"account_number is {account_number}")
        # Generating bank feed sub_feed...(should check the logic)
        BR.data_source['master'] = generate_feed_sub_feed_bank(biz_master_df, BR.data_source['master'], account_number, map_stage, tenant_id)
        # Applying bank specific rules...
        BR.data_source['master'] = apply_karvy_specific_bank_rules(BR.data_source['master'], map_stage, tenant_id, account_number)

    else:
        # feed wise ....any specific functions ..apply here....
        # for exchange_bse_payout generating Effective Date...
        if map_stage.lower() == 'exchange_bse_payout':
            try:
                # ideally configurable from database
                data = {"dates": ["2019-07-06", "2019-07-07", "2019-07-13", "2019-07-14", "2019-07-20", "2019-07-21", "2019-07-27", "2019-07-28", "2019-08-03", "2019-08-04", "2019-08-10", "2019-08-11", "2019-08-12", "2019-08-15", "2019-08-17", "2019-08-18", "2019-08-24", "2019-08-25", "2019-08-31", "2019-09-01", "2019-09-02", "2019-09-07", "2019-09-08", "2019-09-10", "2019-09-14", "2019-09-15", "2019-09-21", "2019-09-22", "2019-09-28", "2019-09-29", "2019-10-02", "2019-10-05", "2019-10-06", "2019-10-08", "2019-10-12", "2019-10-13", "2019-10-19", "2019-10-20", "2019-10-21", "2019-10-26", "2019-10-27", "2019-10-28", "2019-11-02", "2019-11-03", "2019-11-09", "2019-11-10", "2019-11-12", "2019-11-16", "2019-11-17", "2019-11-23", "2019-11-24", "2019-11-30", "2019-12-01", "2019-12-07", "2019-12-08", "2019-12-14", "2019-12-15", "2019-12-21", "2019-12-22", "2019-12-25", "2019-12-28", "2019-12-29"], "holidays": ["SATURDAY", "SUNDAY", "SATURDAY", "SUNDAY", "SATURDAY", "SUNDAY", "SATURDAY", "SUNDAY", "SATURDAY", "SUNDAY", "SATURDAY", "SUNDAY", "BAKRI ID", "INDEPENDENCE DAY/RAKSHABANDHAN", "SATURDAY", "SUNDAY", "SATURDAY", "SUNDAY", "SATURDAY", "SUNDAY", "GANESH CHATURTHI", "SATURDAY", "SUNDAY", "MOHARRAM", "SATURDAY", "SUNDAY", "SATURDAY", "SUNDAY", "SATURDAY", "SUNDAY", "MAHATMA GANDHI JAYANTI", "SATURDAY", "SUNDAY", "DASSERA", "SATURDAY", "SUNDAY", "SATURDAY", "SUNDAY", "ASSEMBLY ELECTIONS IN MAHARASHTRA", "SATURDAY", "SUNDAY", "DIWALI-BALIPRATIPADA", "SATURDAY", "SUNDAY", "SATURDAY", "SUNDAY", "GURUNANAK JAYANTI", "SATURDAY", "SUNDAY", "SATURDAY", "SUNDAY", "SATURDAY", "SUNDAY", "SATURDAY", "SUNDAY", "SATURDAY", "SUNDAY", "SATURDAY", "SUNDAY", "CHRISTMAS", "SATURDAY", "SUNDAY"]}
                holidays = data['dates']
                def get_effective_date(row, holidays):
                    start_date = pd.to_datetime(row['ReturnDate']).date() + pd.DateOffset(1)

                    while str(start_date.date()) in holidays:
                        # print (row['DATE'].date(), type(row['DATE'].date()), row['DATE'].date()+pd.DateOffset(1), str(start_date) in holidays)
                        start_date += pd.DateOffset(1)
                    row['Effective Date'] = start_date.strftime('%Y-%m-%d')
                    return row
                BR.data_source['master']['Effective Date'] = ''
                BR.data_source['master'] = BR.data_source['master'].apply(get_effective_date, args=(holidays,), axis=1)
            except Exception as e:
                logging.error("error in generating effective date")
                logging.error(e)
        BR.data_source['master'] = generate_feed_sub_feed(biz_master_df, BR.data_source['master'], map_stage)
        # changing dict in a row to make column and values...making more columns with one column..
        BR.data_source['master'] = dict_column_split_value(biz_master_df, BR.data_source['master'])
        # assigning to a column based on a conditions...
        BR.data_source['master'] = assign_basedon_compare(biz_master_df, BR.data_source['master'])
    logging.info(f"\nStart rule id got is {start_rule_id}\n ")

    try:
        while start_rule_id != "END" and start_rule_id != None:
            # get the rules, next rule id to be evaluated
            rule_to_evaluate, next_if_sucess, next_if_failure, stage, description, data_source = rule_id_mapping[str(start_rule_id)]  
        
            logging.info(f"\nInside the loop \n rule_to_evaluate  {rule_to_evaluate}\n \
                        \nnext_if_sucess {next_if_sucess}\n \
                        \nnext_if_failure {next_if_failure}\n ")
            
            # evaluate the rule
            BR.rules = [json.loads(rule_to_evaluate)] 
            BR.evaluate_business_rules() # apply the business rules
            BR.description = description
            BR.rule_id = rule_to_evaluate
            BR.folder_name = biz_master_df['folder_name'][0]
            start_rule_id = next_if_sucess # no matter what go with next if success
            logging.info(f"\n next rule id to execute is {start_rule_id}\n")
            
        logging.info("\n Applied chained rules successfully")
        
        logging.info("\n Getting Mapping data")
        

        raw_map = None
        # get raw column mapping...
        try: 
            raw_map = json.loads(biz_master_df['raw_column_mapping'][0])
            logging.info(f"Got raw mapping is : {raw_map}")
        except Exception as e:
            logging.error("Failed to get raw mapping data")
            logging.error(e)

        # get standard column mapping...
        if str(type_).lower().strip() == 'feed':
            try:
                standard_map = json.loads(biz_master_df['standard_column_mapping'][0])
                logging.info(f"Got standard mapping is : {standard_map}")
            except Exception as e:
                logging.error("Failed to get standard mapping data")
                logging.error(e)
        else:
            feed = list(BR.data_source['master']['Feed'])[0]
            sub_feed = list(BR.data_source['master']['Sub_Feed'])[0]
            logging.info(f"banking feed and subfeed are {feed} and {sub_feed}")
            try:
                standard_map = json.loads(biz_master_df['standard_column_mapping'][0])
                logging.info(f"Got standard mapping is : {standard_map}")
            except Exception as e:
                logging.error("Failed to get standard mapping data")
                logging.error(e)

        logging.debug(f"columns of main data frame is : {BR.data_source['master'].columns}")
        logging.debug(type_)
        map_stage = map_stage
        file_name  = file_name
        description = 'Data manipulation rules applied successfully'
        stage = 'Data Manipulation'
        case_id = None
        # Uploading the progess stage of business rules ...
        progress_bar(unique_id,file_name,tenant_id,map_stage,case_id,stage,description)

        # finally writing data to the csv files...
        logging.debug(f"LENGTH OF DF BEFORE CONVERTING TO CSV IS : {len(BR.data_source['master'])}")
        writeToCsv(biz_master_df, BR.data_source['master'], junk_removed_df, old_df, file_path_junk_removed, file_path_raw_, file_path_standard, file_path_rejected, tenant_id, map_stage, raw_map, standard_map , type_, unique_id=unique_id, file_name=file_name)
    except Exception as e:
        map_stage = map_stage
        file_name  = file_name
        description = 'Data manipulation rules failed'
        stage = 'Data Manipulation'
        case_id = None
        # Uploading the progess stage of business rules ...
        progress_bar(unique_id,file_name,tenant_id,map_stage,case_id,stage,description)
        logging.debug("error in applying business rules check rules")
        logging.debug(str(e))

    return BR, file_path_raw_, file_path_standard, file_path_rejected, type_


def run_chained_rules(case_id, tenant_id, chain_rules, start_rule_id=None, updated_tables=False, trace_exec=None, rule_params=None):
    """Execute the chained rules"""
    
    # get the mapping of the rules...basically a rule_id maps to a rule
    rule_id_mapping = {}
    for ind, rule in chain_rules.iterrows():
        rule_id_mapping[rule['rule_id']] = [rule['rule_string'], rule['next_if_sucess'], rule['next_if_failure'], rule['stage'], rule['description'], rule['data_source']]
    logging.info(f"\n rule id mapping is \n{rule_id_mapping}\n")
    
    # evaluate the rules one by one as chained
    # start_rule_id = None
    if start_rule_id is None:
        if rule_id_mapping.keys():
            start_rule_id = list(rule_id_mapping.keys())[0]
            trace_exec = []
            rule_params = {}
            
    # if start_rule_id then. coming from other service 
    # get the existing trace and rule params data
    business_rules_db = DB('business_rules', tenant_id=tenant_id, **db_config)
    rule_data_query = "SELECT * from [rule_data] where [case_id]=%s"
    params = [case_id]
    df = business_rules_db.execute(rule_data_query, params=params)
    try:
        trace_exec = json.loads(list(df['trace_data'])[0])
        logging.info(f"\nexistig trace exec is \n{trace_exec}\n")
    except Exception as e:
        logging.info(f"no existing trace data")
        logging.info(f"{str(e)}")
        trace_exec = []
    
    try:
        rule_params = json.loads(list(df['rule_params'])[0])
        logging.info(f"\nexistig rule_params is \n{rule_params}\n")
    except Exception as e:
        logging.info(f"no existing rule params data")
        logging.info(f"{str(e)}")
        rule_params = {}
       
    logging.info(f"\nStart rule id got is {start_rule_id}\n ")
    while start_rule_id != "END":
        # get the rules, next rule id to be evaluated
        rule_to_evaluate, next_if_sucess, next_if_failure, stage, description, data_source = rule_id_mapping[str(start_rule_id)]  
    
        logging.info(f"\nInside the loop \n rule_to_evaluate  {rule_to_evaluate}\n \
                      \nnext_if_sucess {next_if_sucess}\n \
                      \nnext_if_failure {next_if_failure}\n ")
        
        # update the data_table if there is any change
        case_id_data_tables = get_data_sources(tenant_id, case_id, 'case_id_based')
        master_updated_tables = {} 
        if updated_tables:
            master_updated_tables = get_data_sources(tenant_id, case_id, 'updated_tables')
        # consolidate the data into data_tables
        data_tables = {**case_id_data_tables,**master_updated_tables} 
        
        # evaluate the rule
        rules = [json.loads(rule_to_evaluate)] 
        BR  = BusinessRules(case_id, rules, data_tables)
        decision = BR.evaluate_rule(rules[0])
        
        logging.info(f"\n got the decision {decision} for the rule id {start_rule_id}")
        logging.info(f"\n updates got are {BR.changed_fields}")
        
        updates = {}
        # update the updates if any
        if BR.changed_fields:
            updates = BR.changed_fields
            update_tables(case_id, tenant_id, updates)

        
        # update the trace_data
        trace_exec.append(start_rule_id)

        logging.info(f"\n params data used from the rules are \n {BR.params_data}\n")
        # update the rule_params
        trace_dict = {
                        str(start_rule_id):{
                            'description' : description if description else 'No description available in the database',
                            'output' : "",
                            'input' : to_DT_data(BR.params_data['input'])
                            }
                        }
        rule_params.update(trace_dict)
        # update the start_rule_id based on the decision
        if decision:
            start_rule_id = next_if_sucess
        else:
            start_rule_id = next_if_failure
        logging.info(f"\n next rule id to execute is {start_rule_id}\n")
        
    
    # off by one updates...
    trace_exec.append(start_rule_id)
    
    # store the trace_exec and rule_params in the database
    update_rule_params_query = f"INSERT INTO [rule_data](id, case_id, rule_params) VALUES ('NULL',%s,%s) ON DUPLICATE KEY UPDATE [rule_params]=%s"
    params = [case_id, json.dumps(rule_params), json.dumps(rule_params)]
    business_rules_db.execute(update_rule_params_query, params=params)
    
    update_trace_exec_query = f"INSERT INTO [rule_data] (id, case_id, trace_data) VALUES ('NULL',%s,%s) ON DUPLICATE KEY UPDATE [trace_data]=%s"
    params = [case_id, json.dumps(trace_exec), json.dumps(trace_exec)]
    business_rules_db.execute(update_trace_exec_query, params=params)
    
    logging.info("\n Applied chained rules successfully")
    return 'Applied chained rules successfully'

def run_group_rules(case_id, rules, data):
    """Run the rules"""
    rules = [json.loads(rule) for rule in rules] 
    BR  = BusinessRules(case_id, rules, data)
    updates = BR.evaluate_business_rules()
    logging.info(f"\n updates from the group rules are \n{updates}\n")
    return updates

def apply_business_rule(case_id, function_params, tenant_id, file_path, stage=None, look_ups={}, unique_id = None):
    """Run the business rules based on the stage in function params and tenant_id
    Args:
        case_id: Unique id that we pass
        function_params: Parameters that we get from the configurations
        tenant_id: Tenant on which we have to apply the rules
    Returns:
    """
    updates = {} # keep a track of updates that are being made by business rules
    try:
        # get the stage from the function_parameters...As of now its first ele..
        # need to make generic or key-value pairs
        logging.info(f"\n case_id {case_id} \nfunction_params {function_params} \ntenant_id {tenant_id}\n")
        if not stage:
            try:
                stage = function_params['stage'][0]
            except Exception as e:
                logging.error(f"\n error in getting the stage \n")
                
                stage = 'cms'
        logging.info(f"\n got the stage {stage} \n")

        
        # no case id passed meaning the operations we are doing on the column wise not case_id wise .
        # feature developed for karvy
        column = False
        if not case_id:
            column = True
        
        logging.info(f"\n Had to apply the column rules because column bool is {stage} \n")
        # get the rules
        rules = get_rules(tenant_id, stage)
        

        # get the mapping of the rules...basically a rule_id maps to a rule.
        # useful for the chain rule evaluations
        rule_id_mapping = {}
        for ind, rule in rules.iterrows():
            rule_id_mapping[rule['rule_id']] = [rule['rule_string'], rule['next_if_sucess'], rule['next_if_failure'], rule['stage'], rule['description'], rule['data_source']]

        # if no case_id then processing column wise...
        if column:
            output, file_name_raw,file_name_standard, file_name_rejected, type_  = run_chained_rules_column(file_path, rules, tenant_id, stage, look_ups=look_ups, unique_id=unique_id)
            kafka_data = {'standard_processed_file_path':file_name_standard, 'raw_processed_file_path':file_name_raw, 'rejected_file_path':file_name_rejected, 'stage':stage, 'type':type_}
            return {'flag': True, 'message': 'Applied business rules columnwise successfully.', 'produce_data':kafka_data, 'output':output}

            
        # making it generic takes to take a type parameter from the database..
        # As of now make it (all others  or chained) only
        is_chain_rule = '' not in rule_id_mapping
        
        # get the required table data on which we will be applying business_rules  
        case_id_data_tables = get_data_sources(tenant_id, case_id, 'case_id_based') 
        master_data_tables = get_data_sources(tenant_id, case_id, 'master', master=True)
        
        # consolidate the data into data_tables
        data_tables = {**case_id_data_tables, **master_data_tables} 
        
        logging.info(f"\ndata got from the tables is\n")
        logging.info(data_tables)
        
        updates = {}
        # apply business rules
        if is_chain_rule:
            run_chained_rules(case_id, tenant_id, rules)
        else:
            updates = run_group_rules(case_id, list(rules['rule_string']), data_tables)
            
        # update in the database, the changed fields eventually when all the stage rules were got
        update_tables(case_id, tenant_id, updates)
        
        #  return the updates for viewing
        return {'flag': True, 'message': 'Applied business rules successfully.', 'updates':updates}
    except Exception as e:
        logging.exception('Something went wrong while applying business rules. Check trace.')
        return {'flag': False, 'message': 'Something went wrong while applying business rules. Check logs.', 'error':str(e)}