import Lib
import _StaticFunctions
import _BooleanReturnFunctions
import _AssignFunction
from datetime import datetime
import pandas as pd

try:
    # comment below two for local testing
    from ace_logger import Logging
    logging = Logging()    
except Exception as e:
    # uncomment these below lines for local testing
    import logging 
    logger=logging.getLogger() 
    logger.setLevel(logging.DEBUG) 



@Lib.add_methods_from(_StaticFunctions, _BooleanReturnFunctions, _AssignFunction) 
class BusinessRules():
    
    def __init__(self, case_id, rules, table_data, decision = False):
        self.case_id = case_id
        self.rules = rules
        self.data_source = table_data
        self.is_decision = decision

        # fields which we are maintaining
        self.changed_fields = {}
        self.params_data = {}
        self.params_data['input'] = []

    def evaluate_business_rules(self):
        """Evaluate all the rules"""
        for rule in self.rules:
            logging.info("\n Evaluating the rule: " +f"{rule} \n")
            output = self.evaluate_rule(rule)
            logging.info("\n Output: " +f"{output} \n")
        # update the changes fields in database
        logging.info(f"\nchanged fields are \n{self.changed_fields}\n")
        return self.changed_fields
    
    def evaluate_rule(self,rule):
        """Evaluate the rule"""
        logging.info(f"\nEvaluating the rule \n{rule}\n")

        rule_type = rule['rule_type']
        
        if  rule_type == 'static':
            function_name = rule['function']
            parameters = rule['parameters']
            return  self.evaluate_static(function_name, parameters)
    
        if rule_type == 'condition':
            evaluations = rule['evaluations']
            return self.evaluate_condition(evaluations)
    
    def conditions_met(self, conditions):
        """Evaluate the conditions and give out the final decisoin
        
        """
        eval_string = ''
        # return True if there are no conditions...that means we are doing else..
        if not conditions:
            return True
        # evaluate the conditions
        for condition in conditions:
            logging.info(f"Evaluting the condition {condition}")
            if condition == 'AND' or condition == 'OR':
                eval_string += ' '+condition.lower()+' '
            else:
                eval_string += ' '+str(self.evaluate_rule(condition))+' '
        logging.info(f"\n eval string is {eval_string} \n output is {eval(eval_string)}")
        return eval(eval_string)

    def evaluate_condition(self, evaluations):
        """Execute the conditional statements.

        Args:
            evaluations(dict) 
        Returns:
            decision(boolean) If its is_decision.
            True If conditions met and it is done with executions.
            False For any other case (scenario).
        """
        for each_if_conditions in evaluations:
            conditions = each_if_conditions['conditions']
            executions = each_if_conditions['executions']
            logging.info(f'\nconditions got are \n{conditions}\n')
            logging.info(f'\nexecutions got are \n{executions}\n')
            decision = self.conditions_met(conditions)
            
            """
            Why this self.is_decision and decision ?
                In decison tree there are only one set of conditions to check
                But other condition rules might have (elif conditions which needs to be evaluate) 
            """
            if self.is_decision:
                if decision:
                    for rule in executions:
                        self.evaluate_rule(rule)
                logging.info(f"\n Decision got for the (for decision tree) condition\n {decision}")    
                return decision
            if decision:
                for rule in executions:
                    self.evaluate_rule(rule)
                return True
        return False

    def get_param_value(self, param_object):
        """Returns the parameter value.

        Args:
            param_object(dict) The param dict from which we will parse and get the value.
        Returns:
            The value of the parameter
        Note:
            It returns a value of type we have defined. 
            If the parameter is itself a rule then it evaluates the rule and returns the value.
        """
        logging.info(f"\nPARAM OBJECT IS {param_object}\n")
        param_source = param_object['source']
        if param_source == 'input_config':
            table_key = param_object['table']
            column_key = param_object['column']
            table_key = table_key.strip() # strip for extra spaces
            column_key = column_key.strip() # strip for extra spaces
            logging.debug(f"\ntable is {table_key} and column key is {column_key}\n")
            try:
                data = {}
                # update params data
                data['type'] = 'from_table'
                data['table'] = table_key
                data['column'] = column_key
                data['value'] = self.data_source[table_key][column_key]
                self.params_data['input'].append(data)
                return data['value']
            except Exception as e:
                logging.error(f"\ntable or column key not found\n")
                logging.error(str(e))
                logging.info(f"\ntable data is {self.data_source}\n")
        if param_source == 'rule':
            param_value = param_object['value']
            return self.evaluate_rule(param_value)
        if param_source == 'input':
            param_value = param_object['value']
            # param_value = str(param_value).strip() # converting into strings..need to check
            return  param_value

##############################################################################################################

read_rule = {'rule_type':'static',
              'function':'Read',
              'parameters':{'path':'D:\\AlgonoX\\Python\\BUSINESS RULES\\KARVY_BusinessRules\\sample.csv'}
}

DR_CR_D_check = {'rule_type': 'static',
        'function': 'CompareKeyValue',
        'parameters': {'left_param':{'source': 'rule', 'value':read_rule},
                       'operator':'==',
                       'right_param':{'source':'input', 'value':'D'}
                      }
       }

Amount_Debit = {'rule_type': 'static',
                'function': 'Transform',
                'parameters':[
            {'param':{'source':'rule', 'value':read_rule}},
            {'operator':'*'},
            {'param':{'source':'input', 'value':"-1"}}
        ]
}


Amount_Assign_rule = {'rule_type': 'static',
                        'function': 'Assign',
                        'parameters': {'assign_table':{'table':'karvy_feed_copy', 'column':'Amount'}, 
                                                'assign_value':{'source':'rule', 'value':Amount_Debit}
                      }
}

updating_Amount = {'rule_type': 'condition',
                    'evaluations': [{ 'conditions':[DR_CR_D_check],'executions':[Amount_Assign_rule]

        }]
}


########################################### Assign_Rules ###############################################################################

Feed_Assign_rule = {'rule_type': 'static',
                        'function': 'Assign',
                        'parameters': {'assign_table':{'table':'karvy_feed_copy', 'column':'Feed'}, 
                                                'assign_value':{'source':'input', 'value':'CMS'}
                      }
}

Sub_Feed_Assign_rule = {'rule_type': 'static',
                        'function': 'Assign',
                        'parameters': {'assign_table':{'table':'karvy_feed_copy', 'column':'Sub_Feed'}, 
                                                'assign_value':{'source':'input', 'value':'CMS'}
                      }
}

ID_Assign_rule = {'rule_type': 'static',
                        'function': 'Assign',
                        'parameters': {'assign_table':{'table':'karvy_feed_copy', 'column':'ID'}, 
                                                'assign_value':{'source':'input', 'value':'0'}
                      }
}

Feed_ID_Assign_rule = {'rule_type': 'static',
                        'function': 'Assign',
                        'parameters': {'assign_table':{'table':'karvy_feed_copy', 'column':'Feed_ID'}, 
                                                'assign_value':{'source':'input', 'value':''}
                      }
}

########################################### Filtering_rule ###################################################################

Code_check = {'rule_type': 'static',
        'function': 'CompareKeyValue',
        'parameters': {'left_param':{'source': 'input_config', 'table': 'karvy_feed_copy', 'column': 'Code'},
                       'operator':'==',
                       'right_param':{'source':'input', 'value':''}
                      }
       }

Queue_Assign_rule = {'rule_type': 'static',
                        'function': 'Assign',
                        'parameters': {'assign_table':{'table':'Process_queue', 'column':'queue'}, 
                                                'assign_value':{'source':'input', 'value':'Maker'}
                      }
}    

updating_Queue_Code = {'rule_type': 'condition',
                    'evaluations': [{ 'conditions':[Code_check],'executions':[Queue_Assign_rule]

        }]
}

########################################### Filtering_Date ###################################################################
Filtering_Date = { 'rule_type': 'static',
    'function'  : 'InCheckDate',
    'parameters':{'start_date':{'source':'input', 'value':'2009-01-01'},
                'end_date':{'source':'input', 'value':'2029-01-01'},
                'date':{'source':'input_config','table': 'karvy_feed_copy', 'column': 'Date'}
                }
}

###################################################################################################################################


db_tables = {
                    "karvy_extraction" : ["karvy_feed_copy"],
                    #"queues":["process_queue"]
                }
unique_id = 'Cms_10'

###################################################################################################################################
###################################### Based on Dataframe #########################################################################
"""data = {
    'NAME':['Rachel','Ross','joy','Monica','Phoebe'],
    'ROLL':[497,498,499,500,501],
    'AGE':[23,24,25,26,27]
}

data = pd.DataFrame(data)"""

####################################################################################################################################
count_df_rule = {'rule_type':'static',
                  'function':'Contains',
                  'parameters':{'table_name':'data','column_name':'NAME','value':{'source':'input','value':'Rachel'}}
}

assign_df_rule = {'rule_type':'static',
                  'function':'Assign',
                  'parameters':{'assign_table':{'table':'data', 'column':'ROLL'}, 
                                'assign_value':{'source':'input', 'value':'roll is'}
}
}
#####################################################################################################################################
############################################# Karvy_BusinessRules #########################################################
#####################################################################################################################################

read_rule = {'rule_type':'static',
              'function':'Read',
              'parameters':{'path':'D:\\AlgonoX\\Python\\BUSINESS RULES\\KARVY_BusinessRules\\sample.csv'}
}

#####################################################################################################################################
filtering_rule = {'rule_type': 'static',
                  'function': 'Filter',
                  'parameters': {
                  'from_table': {'source':'input','value':'ocr'},
                 'lookup_filters':[
                {
                    'column_name': 'Amount',
                    'compare_with':  {'source':'input', 'value':1000},
                    'operator':'&'
                },
                {
                    'column_name': 'Plan Code',
                    'compare_with':  {'source':'input', 'value':'AG'},
                    'operator':'|'
                }
            ]
        }

}
########################################## 05-10-2019 #######################################################################

# ocr = pd.read_csv('D:\\AlgonoX\\Python\\BUSINESS RULES\\KARVY_BusinessRules\\sample.csv')

Amount_Debit = {'rule_type': 'static',
                'function': 'TransformDF',
                'parameters':{
                                'table':'ocr',
                                'ocr_copy':'ocr_copy',
                                'value1_colomn':'Amount',
                                'operator':'*',
                                'value2':-1
                            }
}

# ignore filter_test rule
filter_test = {'rule_type': 'static',
    'function': 'Filter',
    'parameters': {
            'from_table': 'ocr',
            'lookup_filters':[
                {
                    'column_name': 'Amount',
                    'lookup_operator' : '==',
                    'compare_with':  {'source':'input', 'value': 10}
                }
            ]
        }
}

where_test = {'rule_type': 'static',
    'function': 'WhereClause',
    'parameters': {
            'data_frame':{'source':'rule', 'value': Amount_Debit},
            'from_table': 'ocr',
            'colomn':'Amount',
            'lookup_filters':[
                {
                    'column_name': 'Plan Code',
                    'lookup_operator' : '==',
                    'compare_with':  {'source':'input', 'value': 'GP'}
                }
                
            ]

        }
}

#####################################################################################################################################
# a = BusinessRules('1234',[where_test],{'ocr':ocr,'ocr_copy':ocr.copy()},decision = True)
# print(a.evaluate_business_rules())
# print(a.data_source['ocr'])