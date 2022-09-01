import Lib
import pandas as pd
import sys
import numpy as np
from db_utils import DB
import os

try:
    # comment below two for local testing
    from ace_logger import Logging
    logging = Logging()    
except Exception as e:
    # uncomment these below lines for local testing
    import logging 
    logger=logging.getLogger() 
    logger.setLevel(logging.DEBUG) 

db_config = {
    'host': os.environ['HOST_IP'],
    'user': os.environ['LOCAL_DB_USER'],
    'password': os.environ['LOCAL_DB_PASSWORD'],
    'port': os.environ['LOCAL_DB_PORT']
} 
__methods__ = [] # self is a BusinessRules Object
register_method = Lib.register_method(__methods__)

@register_method
def evaluate_static(self, function, parameters):
    if function == 'Assign':
        return self.doAssign(parameters)
    if function == 'AssignQ':
        return self.doAssignQ(parameters)
    if function == 'CompareKeyValue':
        return self.doCompareKeyValue(parameters)
    if function == 'GetLength':
        return self.doGetLength(parameters)
    if function == 'GetRange':
        return self.doGetRange(parameters)
    if function == 'Select':
        return self.doSelect(parameters)
    if function == 'Transform':
        return self.doTransform(parameters)
    if function == 'Count':
        return self.doCount(parameters)
    if function == 'Contains':
        return self.doContains(parameters)
    if function == 'Read':
        return self.doRead(parameters)
    if function == 'Filter':
        return self.doFilter(parameters)
    if function == 'TransformDF':
        return self.doTransformDF(parameters)
    if function == 'WhereClause':
        return self.doWhereClause(parameters)
    if function == 'Split':
        return self.doSplit(parameters)
    if function == 'GetTruthValues':
        return self.doGetTruthValues(parameters)
    if function == 'GetTruthValuesOR':
        return self.doGetTruthValuesOR(parameters)
    if function == 'IsAlpha':
        return self.doIsAlpha(parameters)
    if function == 'IsAlnum':
        return self.doIsAlnum(parameters)
    if function == 'DateTransform' :
        return self.doDateTransform(parameters)
    if function == 'getInHouseNo':
        return self.dogetInHouseNo(parameters)
    if function == 'DateTransformstr' :
        return self.doDateTransformstr(parameters)
    if function == 'MergedRows':
        return self.doMergedRows(parameters)
    if function == 'ExtractDate' :
        return self.doExtractDate(parameters)
    if function == 'Replace':
        return self.doReplace(parameters)
    """if function == 'CaptureMetrics':
        return self.doCaptureMetrics(parameters)"""

@register_method
def doGetLength(self, parameters):
    """Returns the lenght of the parameter value.
    Args:
        parameters (dict): The parameter from which the needs to be taken. 
    eg:
       'parameters': {'param':{'source':'input', 'value':5},
                      }
    Note:
        1) Recursive evaluations of rules can be made.
    
    """
    try:
        value = len(self.get_param_value(parameters['param']))
    except Exception as e:
        logging.error(e)
        logging.error(f"giving the defalut lenght 0")
        value = 0
    return value

@register_method
def doGetRange(self, parameters):
    """Returns the parameter value within the specific range.
    Args:
        parameters (dict): The source parameter and the range we have to take into. 
    eg:
       'parameters': {'value':{'source':'input', 'value':5},
                        'range':{'start_index': 0, 'end_index': 4}
                      }
    Note:
        1) Recursive evaluations of rules can be made for the parameter value.
        2) Range is the python range kind of (exclusive of the end_index)
    """
    logging.info(f"parameters got are {parameters}")
    value = self.get_param_value(parameters['value'])
    range_ = parameters['range']
    start_index = range_['start_index']
    end_index = range_['end_index']
    try:
        return (value.str[start_index: end_index])
    except Exception as e:
        logging.error(f"some error in the range function")
        logging.error(e)
    return ""

@register_method
def doSelect(self, parameters):
    """Returns the vlookup value from the tables.
    Args:
        parameters (dict): The table from which we have to select and the where conditions. 
    eg:
        'parameters': {
            'from_table': 'master',
            'select_column': 'highlight',
            'lookup_filters':[
                {
                    'column_name': 'Vendor GSTIN',
                    'compare_with':  {'source':'input', 'value':5}
                },
                {
                    'column_name': 'DRL GSTIN',
                    'compare_with':  {'source':'input', 'value':5}
                },
            ]
        }
    Note:
        1) Recursive evaluations of rules can be made for the parameter value.
        2) Its like vlook up in the dataframe and the from_table must have the primary key...case_id.
    """
    logging.info(f"parameters got are {parameters}")
    from_table = parameters['from_table']
    column_name_to_select = parameters['select_column']
    lookup_filters = parameters['lookup_filters']

    # convert the from_table dictionary into pandas dataframe
    try:
        master_data = self.data_source[from_table]
    except Exception as e:
        logging.error(f"data source does not have the table {from_table}")
        logging.error(e)
        master_data = {}

    master_df = pd.DataFrame(master_data) 

    # build the query
    query = ""
    for lookup in lookup_filters:
        lookup_column = lookup['column_name']
        compare_value = self.get_param_value(lookup['compare_with'])
        query += f"{lookup_column} == {compare_value} & "
    query = query.strip(' & ') # the final strip for the extra &
    result_df = master_df.query(query)

    # get the wanted column from the dataframe
    if not result_df.empty:
        try:
            return result_df[column_name_to_select][0] # just return the first value of the matches
        except Exception as e:
            logging.error(f"error in selecting the required data from the result")
            logging.error(e)
            return ""

@register_method
def doTransform(self,parameters) :
    """Returns the evalated data of given equations
    Args:
        parameters (dict): The source parameter which includes values and operators.
    eg:
        'parameters':[
            {'param':{'source':'input', 'value':5}},
            {'operator':'+'},
            {'param':{'source':'input', 'value':7}},
            {'operator':'-'},
            {'param':{'source':'input', 'value':1}},
            {'operator':'*'},
            {'param':{'source':'input', 'value':3}}
        ]
    Note:
        1) Recursive evaluations of rules can be made.
    """
    equation = ''
    logging.info(f"parameters got are {parameters}")
    for dic in parameters :
        for element,number_operator in dic.items() :
            if element == 'param' :
                value = f'{self.get_param_value(number_operator)}'
            elif element == 'operator' :
                value = f' {number_operator} '
        equation = equation + value
    print("in transaform $$$$$$$$$$$$$$$$$$$")
    print(equation)

    return(eval(equation))

@register_method
def doContains(self,parameters):
    """ Returns true value if the data is present in the data_source
    Args:
        parameters (dict): The source parameter which includes values that should be checked.
    eg:
            cpt_check_rule = {'rule_type': 'static',
                'function': 'Contains',
                'parameters': { 'table_name': 'master','column_name': 'cpt_codes',
                                'value':{'source':'input', 'value':92610}
                        }
            }
    """

    logging.info(f"parameters got are {parameters}")
    table_name = parameters['table_name']
    column_name = parameters['column_name']
    value = self.get_param_value(parameters['value'])
    # print(value,table_name,column_name)
    # print(self.data_source['data'])
    if value in list(self.data_source[table_name][column_name]):
        # print(table_name,column_name)
        return True
    else :
        return False

@register_method
def doCount(self, parameters):
    """Returns the count of records from the tables.
    Args:
        parameters (dict): The table from which we have to select and the where conditions. 
    eg:
        'parameters': {
            'from_table': 'master',
            'lookup_filters':[
                {
                    'column_name': 'Vendor GSTIN',
                    'compare_with':  {'source':'input', 'value':5}
                },
                {
                    'column_name': 'DRL GSTIN',
                    'compare_with':  {'source':'input', 'value':5}
                },
            ]
        }
    Note:
        1) Recursive evaluations of rules can be made for the parameter value.
        2) Its like vlook up in the dataframe and the from_table must have the primary key...case_id.
    """
    logging.info(f"parameters got are {parameters}")
    from_table = parameters['from_table']
    lookup_filters = parameters['lookup_filters']

    # convert the from_table dictionary into pandas dataframe
    try:
        master_data = self.data_source[from_table]
    except Exception as e:
        logging.error(f"data source does not have the table {from_table}")
        logging.error(e)
        master_data = {}

    master_df = pd.DataFrame(master_data) 

    # build the query
    query = ""
    for lookup in lookup_filters:
        lookup_column = lookup['column_name']
        compare_value = self.get_param_value(lookup['compare_with'])
        query += f"{lookup_column} == {compare_value} & "
    query = query.strip(' & ') # the final strip for the extra &
    result_df = master_df.query(query)

    # get the wanted column from the dataframe
    if not result_df.empty:
        try:
            return len(result_df) # just return the first value of the matches
        except Exception as e:
            logging.error(f"error in selecting the required data from the result")
            logging.error(e)
            return 0
    else:
        return 0

@register_method
def doRead(self,parameters):
    """Reads the csv file from the filepath and stores into the data source
    """
    logging.info(f"\n got the parameters {parameters}\n")
    table_name = self.get_param_value(parameters['table_name'])
    try:
        df = pd.read_csv(self.get_param_value(parameters['path']))
        df = df.replace(np.nan, '', regex=True)
    except Exception as e:
        logging.error("\nError in reading the file\n")
        logging.error(e)
        df = pd.DataFrame({})
    
    self.data_source[table_name] = df
    
    return df

@register_method
def doFilter1(self, parameters):
    """Returns the vlookup value from the tables.
    Args:
        parameters (dict): The table from which we have to select and the where conditions. 
    eg:
        'parameters': {
            'from_table': 'master',
            'select_column': 'highlight',
            'lookup_filters':[
                {
                    'column_name': 'Vendor GSTIN',
                    'compare_with':  {'source':'input', 'value':5}
                },
                {
                    'column_name': 'DRL GSTIN',
                    'compare_with':  {'source':'input', 'value':5}
                },
            ]
        }
    Note:
        1) Recursive evaluations of rules can be made for the parameter value.
        2) Its like vlook up in the dataframe and the from_table must have the primary key...case_id.
    """
    logging.info(f"parameters got are {parameters}")
    from_table = self.get_param_value(parameters['from_table'])
    #column_name_to_select = parameters['select_column']
    lookup_filters = parameters['lookup_filters']

    # convert the from_table dictionary into pandas dataframe
    """try:
        master_data = self.data_source[from_table]
    except Exception as e:
        logging.error(f"data source does not have the table {from_table}")
        logging.error(e)
        master_data = {}"""

    #master_df = pd.DataFrame(master_data) 

    # build the query
    query = ""
    for lookup in lookup_filters:
        lookup_column = lookup['column_name']
        compare_value = self.get_param_value(lookup['compare_with'])
        operator_value = lookup['operator']
        master2 = self.data_source['master']
        query += f"({master2}['{lookup_column}'] == {compare_value}) {operator_value}"
    query = query.strip(f" {operator_value} ") # the final strip for the extra &
    print(query)
    self.data_source[from_table] = self.data_source[from_table][query]
    # get the wanted column from the dataframe
    """if not self.data_source[from_table].empty:
        try:
            return self.data_source[from_table] # just return the first value of the matches
        except Exception as e:
            logging.error(f"error in selecting the required data from the result")
            logging.error(e)
            return e"""


######################################### 05-10-2019 #######################################
@register_method
def doIsAlpha(self,parameters):
    """ Returns series of boolean values for given data
    Args:
        parameters (dict): The table from which we have to select and the column name. 
    eg:
        'parameters':{
            'from_table': 'master',
            'column_name':'',
            }
    """
    logging.info(f"parameters got are {parameters}")
    from_table = parameters['from_table']
    column_name = parameters['column_name']
    t_value = pd.Series([True]*len(self.data_source[from_table]))
    t_value = (t_value & (self.data_source[from_table][column_name]).str.isalpha())
    
    return t_value

@register_method
def doIsAlnum(self,parameters):
    """ Returns series of boolean values for given data
    Args:
        parameters (dict): The table from which we have to select and the column name. 
    eg:
        'parameters':{
            'from_table': 'master',
            'column_name':'',
            }
    """
    logging.info(f"parameters got are {parameters}")
    from_table = parameters['from_table']
    column_name = parameters['column_name']
    t_value = pd.Series([True]*len(self.data_source[from_table]))
    # print(f"start t values{t_value}")
    self.data_source[from_table][column_name] = (self.data_source[from_table][column_name]).replace(np.nan, '', regex=True)
    self.data_source[from_table][column_name] = self.data_source[from_table][column_name].astype(str).str.strip()
    # print(f"true values{self.data_source[from_table][column_name]}")
    t_value = (t_value & (self.data_source[from_table][column_name]).astype(str).str.isalnum()) & ~((self.data_source[from_table][column_name]).astype(str).str.isalpha())     
    #print (self.data_source[from_table])
   
    # print (t_value)
    return t_value


@register_method
def doGetTruthValues(self,parameters):
    """ Returns series of boolean values for given data
    Args:
        parameters (dict): The table from which we have to select and the where conditions. 
    eg:
        'parameters':{
            'from_table': 'master',
                'lookup_filters':[
                    {
                        'column_name': 'Plan Code',
                        'lookup_operator' : '==',
                        'compare_with':  {'source':'input', 'value': 'GP'}
                    }
                    
                ]
        }
    """
    logging.info(f"parameters got are {parameters}")
    from_table = parameters['from_table']
    lookup_filters = parameters['lookup_filters']
    t_value = pd.Series([True]*len(self.data_source[from_table]))
    for lookup in lookup_filters:
        lookup_column = lookup['column_name']
        lookup_operator = lookup['lookup_operator']
        compare_value = self.get_param_value(lookup['compare_with'])
        self.data_source[from_table][lookup_column] = (self.data_source[from_table][lookup_column]).replace(np.nan, '', regex=True)
        self.data_source[from_table][lookup_column] = self.data_source[from_table][lookup_column].astype(str).str.strip()
        #assuming amounts are always round figure....
        if 'amount' in lookup_column.lower() :
            self.data_source[from_table][lookup_column] = (self.data_source[from_table][lookup_column]).str.replace(".00", "", regex=False)
            self.data_source[from_table][lookup_column] = (self.data_source[from_table][lookup_column]).str.replace(".0", "", regex=False)

        # our own ==
        if lookup_operator == '==':
            t_value = (t_value & ((self.data_source[from_table])[lookup_column] == compare_value))

        # our own ==
        if lookup_operator == '!=':
            t_value = (t_value & ((self.data_source[from_table][lookup_column]) != compare_value))
            # print(t_value)
        
        # our own >
        if lookup_operator == '>':
            t_value = (t_value & ((self.data_source[from_table])[lookup_column] > compare_value))
        
        # our own <
        if lookup_operator == '<':
            t_value = (t_value & ((self.data_source[from_table])[lookup_column] < compare_value))
            
        # our own >=
        if lookup_operator == '>=':
            t_value = (t_value & ((self.data_source[from_table])[lookup_column] >= compare_value))
            
        # our own <=
        if lookup_operator == '<=':
            t_value <= (t_value & ((self.data_source[from_table])[lookup_column] <= compare_value))

    return t_value

@register_method
def doGetTruthValuesOR(self,parameters):
    """ Returns series of boolean values for given data
    Args:
        parameters (dict): The table from which we have to select and the where conditions. 
    eg:
        'parameters':{
            'from_table': 'master',
                'lookup_filters':[
                    {
                        'column_name': 'Plan Code',
                        'lookup_operator' : '==',
                        'compare_with':  {'source':'input', 'value': 'GP'}
                    }
                    
                ]
        }
    """
    logging.info(f"parameters got are {parameters}")
    from_table = parameters['from_table']
    lookup_filters = parameters['lookup_filters']
    t_value = pd.Series([False]*len(self.data_source[from_table]))

    for lookup in lookup_filters:
        lookup_column = lookup['column_name']
        lookup_operator = lookup['lookup_operator']
        compare_value = self.get_param_value(lookup['compare_with'])
        self.data_source[from_table][lookup_column] = (self.data_source[from_table][lookup_column]).replace(np.nan, '', regex=True)
        self.data_source[from_table][lookup_column] = self.data_source[from_table][lookup_column].astype(str).str.strip()

        # our own ==
        if lookup_operator == '==':
            t_value = (t_value | ((self.data_source[from_table])[lookup_column] == compare_value))

        # our own ==
        if lookup_operator == '!=':
            t_value = (t_value | ((self.data_source[from_table])[lookup_column] != compare_value))
            # print(t_value)
        
        # our own >
        if lookup_operator == '>':
            t_value = (t_value | ((self.data_source[from_table])[lookup_column] > compare_value))
        
        # our own <
        if lookup_operator == '<':
            t_value = (t_value | ((self.data_source[from_table])[lookup_column] < compare_value))
            
        # our own >=
        if lookup_operator == '>=':
            t_value = (t_value | ((self.data_source[from_table])[lookup_column] >= compare_value))
            
        # our own <=
        if lookup_operator == '<=':
            t_value <= (t_value | ((self.data_source[from_table])[lookup_column] <= compare_value))

    return t_value


@register_method
def doTransformDF(self,parameters) :
    """Returns the evalated data of given equations
    Args:
        parameters (dict): The source parameter which includes values and operators.
    eg:
        'parameters':[
            {'param':{'source':'input', 'value':5}},
            {'operator':'+'},
            {'param':{'source':'input', 'value':7}},
            {'operator':'-'},
            {'param':{'source':'input', 'value':1}},
            {'operator':'*'},
            {'param':{'source':'input', 'value':3}},
            {'operator':'broadcast'},
            {'param':{'source':'input', 'value':Value}}
        ]
    Note:
        1) Recursive evaluations of rules can be made.
    """
    logging.info(f"parameters got are {parameters}")
    value1_column = self.get_param_value(parameters['value1_column'])
    value2 = self.get_param_value(parameters['value2'])
    operator_ = parameters['operator']
    table = parameters['table']
    
    try:
        if operator_ == "*":
            return (self.data_source[table][value1_column] * float(value2))
        if operator_ == "+":
            return (self.data_source[table][value1_column] + float(value2))
        if operator_ == "-":
            return (self.data_source[table][value1_column] - float(value2))
        if operator_ == "/":
            return (self.data_source[table][value1_column] / float(value2))
        if operator_ == "broadcast":
            return pd.Series([value2]*2*len(self.data_source[table]))
        
    except Exception as e:
        logging.error("\n error in transform function \n")
        logging.error(e)
    
@register_method
def doFilter(self, parameters):
    """Returns the vlookup value from the tables.
    Args:
        parameters (dict): The table from which we have to select and the where conditions. 
    eg:
        'parameters': {
            'from_table': 'master',
            'lookup_filters':[
                {
                    'column_name': 'Vendor GSTIN',
                    'lookup_operator' : '==',
                    'compare_with':  {'source':'input', 'value':5}
                },
                {
                    'column_name': 'DRL GSTIN',
                    'lookup_operator' : '==',
                    'compare_with':  {'source':'input', 'value':5}
                },
            ]
        }
    Note:
        1) Recursive evaluations of rules can be made for the parameter value.
        2) Its like vlook up in the dataframe and the from_table must have the primary key...case_id.
    """
    logging.info(f"parameters got are {parameters}")
    from_table = parameters['from_table']
    lookup_filters = parameters['lookup_filters']
    t_value = pd.Series([True]*len(self.data_source[from_table]))
    # print(t_value)
    for lookup in lookup_filters:
        lookup_column = lookup['column_name']
        lookup_operator = lookup['lookup_operator']
        compare_value = self.get_param_value(lookup['compare_with'])

        # our own ==
        if lookup_operator == '==':
            t_value = (t_value & ((self.data_source[from_table])[lookup_column] == compare_value))
        # our own !=
        if lookup_operator == '!=':
            t_value = (t_value & ((self.data_source[from_table])[lookup_column] != compare_value))

    try:
        self.data_source[from_table] = self.data_source[from_table][t_value]
        return True
    except Exception as e:
        logging.error("Couldn't update data after filtering")
        logging.error(e)
        return False

@register_method
def doWhereClause(self,parameters):
    """Returns the vlookup value from the tables.
    Args:
        parameters (dict): The table from which we have to select and the boolean series. 
    eg:
        'parameters': {
            'data_frame1':{'source':'rule', 'value': Amount_Debit},
            't_value': {'source':'rule', 'value': Amount_Debit},
            'data_frame2':{'source':'rule', 'value': transfrom_rule}
        }
    }"""

    logging.info(f"parameters got are {parameters}")
    t_value = self.get_param_value(parameters['t_value'])
    data_frame1 = self.get_param_value(parameters['data_frame1'])
    data_frame2 = self.get_param_value(parameters['data_frame2'])
    try:
        rejection_reason = self.get_param_value(parameters['rejection_reason'])
        logging.info(f"rejection reason is : {rejection_reason}")
    except:
        rejection_reason = False
    try:
        filter_output = data_frame1.where(t_value,data_frame2)
    except Exception as e:
        logging.error(e)
        logging.error("\n error in where doWhereClause") 
    if rejection_reason:
        try:
            logging.info(f"=====>{self.data_source['master']['rejection_reason']}")
            self.data_source['master']['rejection_reason'] = self.data_source['master']['rejection_reason'].where(t_value,self.data_source['master']['rejection_reason']+'; '+rejection_reason).astype(str).str.lstrip(';')
        except Exception as e:
            logging.error(e)
            logging.error("\n error in where doWhereClause for generating rejection_reason")
    return filter_output
@register_method
def doSplit(self,parameters):
    """
    eg:
        'parameters': {
            'value_tosplit':{'source':'rule', 'value': Amount_Debit},
            'symbol': '-',
            'index': 0
            }
    """
    logging.info(f"parameters got are {parameters}")
    data = self.get_param_value(parameters['value_tosplit'])
    symbol = parameters['symbol']
    index = parameters['index']
    try:
        print((data.str.split(symbol).str[index]).str.strip())

        return (data.where(~(data.str.split(symbol).str.len() > index), data.str.split(symbol).str[index].str.strip()))

        # return((data.str.split(symbol).str[index]).str.strip())
    except Exception as e:
        logging.error(e)
        logging.error("returning the data first column")
        #return ((data.str.split(symbol).str[0]).str.strip())
        try:
            return (data.str.split(symbol).str[0]).str.strip()
        except:
            logging.error('got nonetype value')
            return data

@register_method
def doDateTransform(self,parameters):
    """Returns dates of single formate.
    Args:
        parameters (dict): The table and column names which contain dates.
    eg:
        'parameters': {
                    'from_table':'master',
                    'from_column':'Nav Date'
                    'format':'',
                    'separator':''
                }

    """
    logging.info(f"parameters got are {parameters}")
    from_table = parameters['from_table']
    from_column = parameters['from_column']
    try:
        format_ = parameters['format']
        separator = parameters['separator']
    except:
        format_ = ''
        separator = ''

    def date_apply(row):
        try:
            date_list = row.split(separator)
            print(f"date list is {date_list}")
            row = date_list[1]+separator+date_list[0]+separator+date_list[2][0:4]
            print(f"row is {row}")
            return row
        except Exception as e:
            print(e)
            print("Month Date Conversion failed")
            return row

    if format_ == 'mm-dd-yyyy':
        try:
            self.data_source[from_table][from_column] = self.data_source[from_table][from_column].apply(date_apply)
            self.data_source[from_table][from_column] = self.data_source[from_table][from_column].astype(str)
            self.data_source[from_table][from_column] =  pd.to_datetime(self.data_source[from_table][from_column],dayfirst=True,errors='coerce').dt.strftime('%Y-%m-%d')
            self.data_source[from_table][from_column] = self.data_source[from_table][from_column].astype(str).str.replace('NaT','')
        except Exception as e:
            logging.error("DATE CONVERSION FAILED")
            logging.error(e)
    else:
        try:
            self.data_source[from_table][from_column] = self.data_source[from_table][from_column].astype(str)
            self.data_source[from_table][from_column] =  pd.to_datetime(self.data_source[from_table][from_column],dayfirst=True,errors='coerce').dt.strftime('%Y-%m-%d')
            self.data_source[from_table][from_column] = self.data_source[from_table][from_column].astype(str).str.replace('NaT','')
        except Exception as e:
            logging.error("DATE CONVERSION FAILED")
            logging.error(e)


@register_method
def doDateTransformstr(self,parameters):
    """Returns dates of single formate.
    Args:
        parameters (dict): The table and column names which contain dates.
    eg:
        'parameters': {
                    'from_table':{'source':'input', 'value': 'master'},
                    'from_column':{'source':'input', 'value': 'nav date'},
                    'format':'dmy'
                }
    note:
        input of format can be dmy or ymd or mdy

    """
    logging.info(f"parameters got are {parameters}")
    
    def change_date_year(date_string):
        try:
            date_string = (date_string[0:4] + '-' + date_string[4:6] + '-' + date_string[6:8])
            return date_string
        except Exception as e:
            logging.error("Date string Convertion failed")
            logging.error(e)
            return ''
    
    def change_date_date(date_string):
        try:
            date_string = (date_string[:-6] + '-' + date_string[-6:-4] + '-' + date_string[-4:])
            return date_string
        except Exception as e:
            logging.error("Date string Convertion failed")
            logging.error(e)
            return ''
    
    from_table = self.get_param_value(parameters['from_table'])
    from_column = self.get_param_value(parameters['from_column'])
    date_format = parameters['format']
    self.data_source[from_table][from_column] = self.data_source[from_table][from_column].astype(str)
    # print(self.data_source[from_table][from_column])

    try:
        if date_format == 'dmy' :
            self.data_source[from_table][from_column] = self.data_source[from_table][from_column].apply(change_date_date)
            self.data_source[from_table][from_column] =  pd.to_datetime(self.data_source[from_table][from_column],dayfirst=True,errors='coerce').dt.strftime('%Y-%m-%d')
            self.data_source[from_table][from_column] = self.data_source[from_table][from_column].astype(str).str.replace('NaT','')
            
        if date_format == 'yyyy-mm-dd' :
            self.data_source[from_table][from_column] = self.data_source[from_table][from_column].apply(change_date_year)
            self.data_source[from_table][from_column] =  pd.to_datetime(self.data_source[from_table][from_column],errors='coerce').dt.strftime('%Y-%m-%d')
            self.data_source[from_table][from_column] = self.data_source[from_table][from_column].astype(str).str.replace('NaT','')

        logging.debug(f"Changed date for {from_column} is {self.data_source[from_table][from_column].head(5)}")
    except Exception as e:
        logging.error("DATE CONVERSION FAILED")
        logging.error(e)
       
@register_method
def dogetInHouseNo(self,parameters):
    """ Returns series of boolean values for given data
    Args:
        parameters (dict): The table from which we have to select and the column name. 
    eg:
        'parameters':{
            'take_up': 'IN_HOUSE_NO',
            'lookup_key':'ENTRY_ID',
            }
    """
    logging.info(f"parameters got are {parameters}")
    
    def process_ihno(ihno):
        try:
            if ihno.isalpha():
                return ''
            ihno_words = ihno.split()
            max_word = ihno_words[0]
            for word in ihno_words:
                if word.isnumeric() and len(word) >= 9:
                    # print (f"inho correct is .............{word}")
                    return word
        except:
            return ''    
        return ''
    
    try:
        
        lookup_key = parameters['lookup_key']
        take_up = parameters['take_up']
        master_df = self.data_source['master'] 
        master_df[take_up] = ''
        for table in self.data_source:
            if table == 'master':
                continue
            df = self.data_source[table]
            #joined = master_df.join(df, lsuffix='_caller', rsuffix='_other')
            joined = master_df.join(df.set_index(lookup_key), on=lookup_key, lsuffix='_caller', rsuffix='_other')
            master_df[take_up] =  master_df[take_up].where(((master_df[take_up] != '')), joined[take_up+'_other'])
            master_df[take_up] = master_df[take_up].replace(np.nan, '', regex=True)
            

            master_df[take_up] = master_df[take_up].astype(str)
            master_df[take_up] = master_df[take_up].str.replace(".0","", regex=False)
            master_df[take_up] = master_df[take_up].apply(process_ihno)
        print(f"take up : {master_df[take_up]}")
    except Exception as e:
        logging.error("error in join and generating the inhouse no")
        logging.error(str(e))
    
    return "Done with in house no generation"


@register_method
def doMergedRows(self,parameters):
    """ Returns series of boolean values for given data
    Args:
        parameters (dict): The table from which we have to select and the column name. 
    eg:
        'parameters':{
            'column_key':'ENTRY_ID',
            }
    """
    logging.info(f"parameters got are {parameters}")
    
    try:
        column_key = self.get_param_value(parameters['column_key'])
        master_df = self.data_source['master']
        master_df[column_key] = master_df[column_key].fillna(method='ffill') 
    except Exception as e:
        logging.error("error in ~ do merged rows")
        logging.error(str(e))
    return "Done with merged columns problem"

@register_method
def doExtractDate(self, parameters):
    """Extracts a substring from the requirements
        'parameters':{
                    'split_by':'_',
                    'split_index':'',
                    'start_index':'',
                    'end_index':'',
                    'to_table':'',
                    'to_column':''
        }
    """
    split_by = parameters['split_by']
    split_index = parameters['split_index']
    to_table = parameters['to_table']
    to_column = parameters['to_column']
    print(f"to column{to_column}")
    file_path = self.file_path
    print(f"file path is {file_path}")
    try:
        self.data_source[to_table][to_column] = str(file_path).split(split_by)[split_index]
        print(f"extracted_date{self.data_source[to_table][to_column]}")
        return True
    except Exception as e:
        logging.error("Date split failed")
        logging.error(e)
    return False

@register_method
def doCaptureMetrics(self,parameters):
    """
        'parameters': {'table_name' : 'master'}
    """
    
    logging.info(f"parameters got are {parameters}")
    table_name = parameters['table_name']
    raw_file_name_generated = self.raw_file_name_generated
    standard_file_name_generated = self.standard_file_name_generated
    folder_name = self.folder_name
    description = self.description
    data = self.data_source[table_name]
    rule_id = self.rule_id
    total_count = len(data.index)
    accepted_count = len(data[data['Filter'] == 'Y'])
    rejected_count = total_count-accepted_count
    logging.info("Done till assigning")
    query = f"INSERT INTO [capture_metrics](raw_file_name_generated, standard_file_name_generated, folder_name, description, total_rows, accepted_rows, rejected_rows,rule_id) VALUES ('{raw_file_name_generated}', '{standard_file_name_generated}', '{folder_name}', '{description}', {total_count}, {accepted_count}, {rejected_count},{rule_id})"
    try:
        db = DB('business_rules', tenant_id=self.tenant_id, **db_config)
        db.execute(query,'karvy_business_rules')
    except Exception as e:
        logging.error("Error in updating the table of capture_metrics")
        logging.error("check whether the table exists")
        logging.error(e)

    return True

@register_method
def doReplace(self, parameters):
    """
    'parameters': {'table_name' : 'master',
            'column_name':'',
            'to_replace':'',
            'replace_with':''
    }
    """
    def replace_apply(row, to_replace, replace_with):
        try:
            return(str(row).replace(to_replace, replace_with))
        except Exception as e:
            logging.error(f"Unable to replace {to_replace}")
            logging.error(e)
            return row
    logging.info(f"parameters got are : {parameters}")
    table_name = parameters['table_name']
    column_name = parameters['column_name']
    data = self.data_source[table_name][column_name]
    logging.debug(f"Got data is : {data} and type is : {type(data)}")
    to_replace = self.get_param_value(parameters['to_replace'])
    replace_with = self.get_param_value(parameters['replace_with'])
    try:
        logging.info(f"data before apply : {data.head(5)}")
        data = data.apply(replace_apply, to_replace= to_replace, replace_with= replace_with)
        logging.info(f"data after apply : {data.head(5)}")
        #self.data_source[table_name][column_name] = data
    except Exception as e:
        logging.error(f"Unable to apply replace on column {column_name}")
        logging.error(e)

    return data
