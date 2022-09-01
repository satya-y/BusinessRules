# karvy_business_rules_flow
## Business rules flow on input file

At start apply_business_rule function will be triggered with the bellow parameters:

```
def apply_business_rule(case_id, function_params, tenant_id, file_path, stage=None, look_ups={}, unique_id = None):
```
  
  * we are reading the `business_rules` for that specific `stage` from `sequence_rule_data` table in database
  
  * Then run_chained_rules_column function was called with the bellow parameters:
  
    ```
    output, file_name_raw,file_name_standard, file_name_rejected, type_  = run_chained_rules_column(file_path, rules, tenant_id, stage, look_ups=look_ups, unique_id=unique_id)
    ```
    
    * Reading the `preprocessing master data` for that specific `stage` from preprocessing_master table in database.
    
    * Generating the file names for raw processed, standard processed, rejected, junk removed files by using input file path.
    
    * Now we are reading the `Input file` by calling `read_file` function with given parameters as below,
    
      ```
      read_file(BR, file_path, biz_master_df, map_stage, look_ups, unique_id=unique_id, file_name=file_name, tenant_id=tenant_id)
      ```
      * Accepts extentions - xlsx, xls, csv, txt, xlsb.
      * Reads a specific sheet from file, if mentioned. Or else reads first sheet from the file.
      * If unable to read the file it will get rejected.
     
    * We will decide the type of file like whether it was a feed input file or bank input file based on details mentioned in database.

    * Then we are removing the junk in input file by calling `get_new_df` function with the bellow parameters,
    
      ```
          BR.data_source['master'] = get_new_df(biz_master_df, BR.data_source['master'], map_stage, file_path)
      ```
         * Removes upper unwanted lines on base of configured headers.
         * If any input header is not in configured header, file will be rejected.
         
     * Then we strip the column names if column names contains extra spaces at starting or ending.
     
     * Then we droping all the rows in input data which are empty...
     
     * Then selecting only required columns(if any required columns mentioned) from the dataframe...By calling `select_req_columns_df function`.
     
     * Then we are creating extra columns like Filter, Matched, Queue..etc(which are required according to development) and assigning required default values to them.
     
     * Then we are splitting merged columns if mentioned for specific columns.
     
     * Then we are converting datatypes specificly for mentioned columns.
        * If a specific column value of a particular row got failed in conversion, then that row will get rejected.
     
     * Then we are converting dates in mentioned columns to single format.
        * Converting all the dates to YYYY-MM-DD.
        * If a specific column value of a particular row got failed in converting date, then that row will get rejected.
     
      If type is bank
        
        * Then we are extracting account number from file name.
        * Then we are generating feed, sub_feed and scheme. On base of account number.
        * If unable to fetch sub_feed then by checking some keywords in transacion description column we assign sub_feed.
        * Then applying bank specific rules.
      
      If type is feed
    
        * Then we are generating feed and Sub_feed for that file, and assigning those values into feed and Sub_feed columns.

        * Then from dict in a row to make column and values by calling `def dict_column_split_value function`...making more columns with one column.. (specific requirement)

        * we assigning some values into given columns based on given condition by calling `def colassign_basedon_compare function`  (specific requirement)
    
     * Then we are applying specific business rules for individual feed which were written in `sequence_rule_data table`.

     * Finally we are writing that resultant dataframe to .csv files (junk_removed, rejected, raw, standard) with specific mappings.
        * Junk removed file - The data after removing top unwanted line from the input, Has all the columns.
        * Rejected file - The rows in data which are rejected, Has all the columns.
        * Raw file - Raw output file with all the data, Generated with the help of raw mapping(only containes required columns).
        * Standard file - Standard output file with all the data, Generated with the help of Standard mapping(only containes required columns).
        
     * for Flow chart click https://app.diagrams.net/?libs=general;flowchart#G1_B2y9868cWyceXJlMt6zP2tfCM870ao8  
