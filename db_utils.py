"""
Author: Ashyam Zubair
Created Date: 14-02-2019
"""
import json
import pandas as pd
import traceback
import sqlalchemy
import os
import pyodbc
import sys
import pymssql
import numpy as np

import logging

from MySQLdb._exceptions import OperationalError
from sqlalchemy import create_engine, exc
from time import time

#connection_string = "DRIVER={ODBC Driver 17 for SQL Server};SERVER=192.168.15.126;UID=BRS;PWD=Fint$123;Trusted Connection=yes;DATABASE="
connection_string = None
#try:
#    from app.ace_logger import Logging
#except:
#    from ace_logger import Logging
#    pass
    
#logging = Logging()

class DB(object):
    def __init__(self, database, host='127.0.0.1', user='root', password='', port='3306', tenant_id=None):
        """
        Initialization of databse object.
        Args:
            databse (str): The database to connect to.
            host (str): Host IP address. For dockerized app, it is the name of
                the service set in the compose file.
            user (str): Username of MySQL server. (default = 'root')
            password (str): Password of MySQL server. For dockerized app, the
                password is set in the compose file. (default = '')
            port (str): Port number for MySQL. For dockerized app, the port that
                is mapped in the compose file. (default = '3306')
        """

        if host in ["common_db","extraction_db", "queue_db", "template_db", "table_db", "stats_db", "business_rules_db", "reports_db"]:
            self.HOST = os.environ['HOST_IP']
            self.USER = 'sa'
            self.PASSWORD = os.environ['LOCAL_DB_PASSWORD']
            self.PORT = os.environ['LOCAL_DB_PORT']
            self.DATABASE = f'{tenant_id}_{database}' if tenant_id is not None and tenant_id else database
        else:
            self.HOST = os.environ['HOST_IP']
            self.USER = 'sa'
            self.PASSWORD = os.environ['LOCAL_DB_PASSWORD']
            self.PORT = os.environ['LOCAL_DB_PORT']
            self.DATABASE = f'{tenant_id}_{database}' if tenant_id is not None and tenant_id else database
        
        logging.info(f'Host: {self.HOST}')
        logging.info(f'User: {self.USER}')
        logging.info(f'Password: {self.PASSWORD}')
        logging.info(f'Port: {self.PORT}')
        logging.info(f'Database: {self.DATABASE}')
        # self.connect()
    def connect(self, max_retry=5):
#         retry = 1

#         try:
#             start = time()
#             logging.debug(f'Making connection to {self.DATABASE}...')
#             config = f'mysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8'
#             self.db_ = create_engine(config, connect_args={'connect_timeout': 2}, pool_recycle=300)
#             logging.info(f'Engine created for {self.DATABASE}')
#             while retry <= max_retry:
#                 try:
#                     self.engine = self.db_.connect()
#                     logging.info(f'Connection established succesfully to {self.DATABASE}! ({round(time() - start, 2)} secs to connect)')
#                     break
#                 except Exception as e:
#                     logging.warning(f'Connection failed. Retrying... ({retry}) [{e}]')
#                     retry += 1
#                     self.db_.dispose()
#         except:
#             logging.exception(f'Something went wrong while connecting. Check trace.')
#             return
        data = []
        inds = [i for i in range(len(sql)) if sql[i] == '']
        print(inds)
        for pos, ind in enumerate(inds):
            if pos % 2 == 0:
                sql = sql[:ind] + '[' + sql[ind+1:]
            else:
                sql = sql[:ind] + ']' + sql[ind + 1:]
        if connection_string:
            try:
                conn = pyodbc.connect(connection_string + self.DATABASE)
            except Exception as e:
                print('Connection string invalid. ', e)
        else:
            try:
                if user_ or password_:
                    conn = pyodbc.connect('DRIVER={' + driver + '};SERVER=' + host_ + ';DATABASE=' + database+ ';UID=' + user_ + ';PWD=' + password_ + ';Trusted Connection=yes;')
                else:
                    conn = pyodbc.connect('DRIVER={' + driver + '};SERVER=' + host_ + ';DATABASE=' + database + ';Trusted Connection=yes;')
            except Exception as e:
                print("Error establishing connection to DB. ", e)
                conn = pyodbc.connect('DRIVER={' + driver + '};SERVER=' + host_ + ';DATABASE=' + database + ';Trusted Connection=yes;')

    def convert_to_mssql(self, query):
        inds = [i for i in range(len(query)) if query[i] == '`']
        for pos, ind in enumerate(inds):
            if pos % 2 == 0:
                query = query[:ind] + '[' + query[ind+1:]
            else:
                query = query[:ind] + ']' + query[ind + 1:]
        
        query = query.replace('%s', '?')

        return query

    def execute(self, query, database=None, index_col='id', **kwargs):
        logging.debug(f'Before converting: {query}')
        query = self.convert_to_mssql(query)
        logging.debug(f'After converting: {query}')

        logging.debug('Connecting to DB')
        conn = pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={self.HOST};UID={self.USER};PWD={self.PASSWORD};Trusted Connection=yes;DATABASE={self.DATABASE}', as_dict=True)
        logging.debug(f'Connection established with {self.DATABASE}. [{conn}]')
        curs = conn.cursor()
        logging.debug(f'Cursor object created. [{curs}]')
        params = tuple(kwargs.get('params', []))
        
        logging.debug(f'Params: {params}')
        logging.debug(f'Params Type: {type(params)}')
        params = [int(i) if isinstance(i, np.int64) else i for i in params]
        curs.execute(query, params)
        logging.debug(f'Query executed.')
        
        data = None

        try:
            logging.debug(f'Fetching all data.')
            data = curs.fetchall()
            # logging.debug(f'Data fetched: {data}')
            columns = [column[0] for column in curs.description]
            logging.debug(f'Columns: {columns}')
            result = []
            for row in data:
                result.append(dict(zip(columns, row)))
            # logging.debug(f'Zipped result: {result}')
            if result:
                data = pd.DataFrame(result)
            else:
                data = pd.DataFrame(columns=columns)
            # logging.debug(f'Data to DF: {data}')
        except:
            logging.debug('Update Query')
        conn.commit()
        conn.close()
        if not isinstance(data, pd.DataFrame):
            logging.debug(f'Data is not a DataFrame. Returning True. [{type(data)}]')
            return True
        
        try:
            if index_col is not None:
                logging.debug(f'Setting ID as index')
                return data.where((pd.notnull(data)), None).set_index('id')
            else:
                return data.where((pd.notnull(data)), None)
        except:
            logging.exception(f'Failed to set ID as index')
            return data.where((pd.notnull(data)), None)

    def execute__(self, query, database=None, **kwargs):
        """
        Executes an SQL query.
        Args:
            query (str): The query that needs to be executed.
            database (str): Name of the database to execute the query in. Leave
                it none if you want use database during object creation.
            params (list/tuple/dict): List of parameters to pass to in the query.
        Returns:
            (DataFrame) A pandas dataframe containing the data from the executed
            query. (None if an error occurs)
        """
        data = None

#         # Use new database if a new databse is given
#         if database is not None:
#             try:
#                 config = f'mysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{database}?charset=utf8'
#                 engine = create_engine(config, pool_recycle=300)
#             except:
#                 logging.exception(f'Something went wrong while connecting. Check trace.')
#                 return False
#         else:
#             engine = self.engine
        
        print('query', query)
        if database is None:
            database = 'karvy'
        data = None
        sql = query
        user_ = self.USER
        host_ = self.HOST
        database = self.DATABASE
        password_ = self.PASSWORD
        inds = [i for i in range(len(sql)) if sql[i] == '']
        for pos, ind in enumerate(inds):
            if pos % 2 == 0:
                sql = sql[:ind] + '[' + sql[ind+1:]
            else:
                sql = sql[:ind] + ']' + sql[ind + 1:]
               
        if connection_string:
            print('connection string', connection_string)
            print('database', database)
            print(type(connection_string + database))
            print(type(connection_string + database))

            try:
                conn = pyodbc.connect(connection_string + database)
            except Exception as e:
                print('Connection string invalid. ', e)
        else:
            try:
                if user_ or password_:
                    conn = pymssql.connect(host=host_,database=database,user=user_,password=password_)
                else:
                    conn = pymssql.connect(host=host_,database=database)
            except Exception as e:
                print("Error establishing connection to DB. ", e)
                conn = pymssql.connect(host=host_,database=database)
        try:
            logging.debug(f'Query: {query}')
            logging.debug(f'Query: {query}')
            curs = conn.cursor(as_dict = True)
            params = kwargs.get('params', [])
            logging.debug(f'Params: {params}')
            curs.execute(sql, tuple(params))
            print('query executed')
            try:
                data = curs.fetchall()
                data = pd.DataFrame(data)
                # print(data)
            except Exception as e:
                logging.debug('Update Query')
            # data = pd.read_sql(sql, conn, index_col='id', **kwargs)
        except exc.ResourceClosedError:
            logging.warning('Query does not have any value to return.')
            return True
        except (exc.StatementError, OperationalError) as e:
            logging.warning(f'Creating new connection. Engine/Connection is probably None. [{e}]')
            self.connect()
            data = pd.read_sql(query, self.engine, index_col='id', **kwargs)
        except:
            logging.exception('Something went wrong executing query. Check trace.')
            params = kwargs['params'] if 'params' in kwargs else None
            conn.rollback() 
            return False
        conn.close()
        return data.where((pd.notnull(data)), None).set_index('id')

    def execute_(self, query, database=None, **kwargs):
        """
        Executes an SQL query.
        Args:
            query (str): The query that needs to be executed.
            database (str): Name of the database to execute the query in. Leave
                it none if you want use database during object creation.
            params (list/tuple/dict): List of parameters to pass to in the query.
        Returns:
            (DataFrame) A pandas dataframe containing the data from the executed
            query. (None if an error occurs)
        """
        logging.debug(f'Executing `execute` instead of `execute_`')
        return self.execute(query, index_col=None, **kwargs)
        
        data = None

#         # Use new database if a new database is given
#         if database is not None:
#             try:
#                 config = f'mysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{database}?charset=utf8'
#                 engine = create_engine(config, pool_recycle=300)
#             except:
#                 logging.exception(f'Something went wrong while connecting. Check trace.')
#                 return False
#         else:
#             engine = self.engine

#         try:
#             data = pd.read_sql(query, engine, **kwargs)
#         except exc.ResourceClosedError:
#             return True
#         except:
#             logging.exception(f'Something went wrong while connecting. Check trace.')
#             params = kwargs['params'] if 'params' in kwargs else None
#             return False
        print('query', query)
        if database is None:
            database = 'karvy'
        data = None
        sql = query
        user_ = self.USER
        database = self.DATABASE
        host_ = self.HOST
        password_ = self.PASSWORD
        inds = [i for i in range(len(sql)) if sql[i] == '']
        # for pos, ind in enumerate(inds):
        #     if pos % 2 == 0:
        #         sql = sql[:ind] + '[' + sql[ind+1:]
        #     else:
        #         sql = sql[:ind] + ']' + sql[ind + 1:]
               
        if connection_string:
            print('connection string', connection_string)
            print('database', database)
            print(type(connection_string + database))
            print(type(connection_string + database))

            try:
                conn = pyodbc.connect(connection_string + database)
            except Exception as e:
                print('Connection string invalid. ', e)
        else:
            try:
                if user_ or password_:
                    conn = pymssql.connect(host=host_,database=database,user=user_,password=password_)
                else:
                    conn = pymssql.connect(host=host_,database=database)
            except Exception as e:
                print("Error establishing connection to DB. ", e)
                conn = pymssql.connect(host=host_,database=database)
        try:
            logging.debug(f'Query: {query}')
            curs = conn.cursor(as_dict = True)
            params = kwargs.get('params', [])
            logging.debug(f'Params: {params}')
            curs.execute(sql, params)
            print('query executed')
            try:
                data = curs.fetchall()
                data = pd.DataFrame(data)
                print(data)
            except Exception as e:
                logging.debug('Update Query')

            #data = pd.read_sql(sql, conn,**kwargs)
        except exc.ResourceClosedError:
            logging.warning('Query does not have any value to return.')
            return True
        except (exc.StatementError, OperationalError) as e:
            logging.warning(f'Creating new connection. Engine/Connection is probably None. [{e}]')
            self.connect()
            data = pd.read_sql(query, conn,**kwargs)
        except:
            logging.exception('Something went wrong executing query. Check trace.')
            params = kwargs['params'] if 'params' in kwargs else None
            conn.rollback() 
            return False
        conn.commit()
        conn.close()
#         return data.where((pd.notnull(data)), None)
        try:
            return data.replace({pd.np.nan: None}).set_index('id')
        except AttributeError as e:
            return True


    def insert(self, data, table, database=None, **kwargs):
        """
        Write records stored in a DataFrame to a SQL database.
        Args:
            data (DataFrame): The DataFrame that needs to be write to SQL database.
            table (str): The table in which the rcords should be written to.
            database (str): The database the table lies in. Leave it none if you
                want use database during object creation.
            kwargs: Keyword arguments for pandas to_sql function.
                See https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_sql.html
                to know the arguments that can be passed.
        Returns:
            (bool) True is succesfully inserted, else false.
        """
        logging.info(f'Inserting into {table}')

        # # Use new database if a new databse is given
        # if database is not None:
        #     try:
        #         config = f'mysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{database}?charset=utf8'
        #         engine = create_engine(config, pool_recycle=300)
        #     except:
        #         logging.exception(f'Something went wrong while connecting. Check trace.')
        #         return False
        # else:
        #     engine = self.engine

        print('query', query)
        if database is None:
            database = 'karvy'
        data = None
        sql = query
        user_ = self.USER
        database = self.DATABASE
        host_ = self.HOST
        password_ = self.PASSWORD
        inds = [i for i in range(len(sql)) if sql[i] == '']
        # for pos, ind in enumerate(inds):
        #     if pos % 2 == 0:
        #         sql = sql[:ind] + '[' + sql[ind+1:]
        #     else:
        #         sql = sql[:ind] + ']' + sql[ind + 1:]
               
        if connection_string:
            print('connection string', connection_string)
            print('database', database)
            print(type(connection_string + database))
            print(type(connection_string + database))

            try:
                conn = pyodbc.connect(connection_string + database)
            except Exception as e:
                print('Connection string invalid. ', e)
        else:
            try:
                if user_ or password_:
                    conn = pymssql.connect(host=host_,database=database,user=user_,password=password_)
                else:
                    conn = pymssql.connect(host=host_,database=database)
            except Exception as e:
                print("Error establishing connection to DB. ", e)
                conn = pymssql.connect(host=host_,database=database)
        try:
            logging.debug(f'Query: {query}')
            # data.to_sql(table, conn, **kwargs)
            curs = conn.cursor(as_dict = True)
            curs.execute(sql, tuple(kwargs.get('params', [])))
            print('query executed')
            try:
                data = curs.fetchall()
                data = pd.DataFrame(data)
                print(data)
            except Exception as e:
                logging.debug('Update Query')

            try:
                self.execute(f'ALTER TABLE {table} ADD PRIMARY KEY (id);')
            except:
                pass
            conn.commit()
            conn.close()
            return True
        except:
            logging.exception('Something went wrong inserting. Check trace.')
            return False

    def insert_dict(self, data, table):
        """
        Insert dictionary into a SQL database table.
        Args:
            data (DataFrame): The DataFrame that needs to be write to SQL database.
            table (str): The table in which the rcords should be written to.
        Returns:
            (bool) True is succesfully inserted, else false.
        """
        logging.info(f'Inserting dictionary data into {table}...')
        logging.debug(f'Data:\n{data}')

        try:
            column_names = []
            params = []

            for column_name, value in data.items():
                column_names.append(f'{column_name}')
                params.append(value)

            logging.debug(f'Column names: {column_names}')
            logging.debug(f'Params: {params}')

            columns_string = ', '.join(column_names)
            param_placeholders = ', '.join(['%s'] * len(column_names))

            query = f'INSERT INTO {table} ({columns_string}) VALUES ({param_placeholders})'

            return self.execute(query, params=params)
        except:
            logging.exception('Error inserting data.')
            return False

    def update(self, table, update=None, where=None, database=None, force_update=False):
        # Use new database if a new databse is given
        # if database is not None:
        #     try:
        #         config = f'mysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{database}?charset=utf8'
        #         self.engine = create_engine(config, pool_recycle=300)
        #     except:
        #         logging.exception(f'Something went wrong while connecting. Check trace.')
        #         return False

        logging.info(f'Updating table: {table}')
        logging.info(f'Update data: {update}')
        logging.info(f'Where clause data: {where}')
        logging.info(f'Force update flag: {force_update}')

        try:
            set_clause = []
            set_value_list = []
            where_clause = []
            where_value_list = []

            if where is not None and where:
                for set_column, set_value in update.items():
                    set_clause.append(f'{set_column}=%s')
                    set_value_list.append(set_value)
                set_clause_string = ', '.join(set_clause)
            else:
                logging.error(f'Update dictionary is None/empty. Must have some update clause.')
                return False

            if where is not None and where:
                for where_column, where_value in where.items():
                    where_clause.append(f'{where_column}=%s')
                    where_value_list.append(where_value)
                where_clause_string = ' AND '.join(where_clause)
                query = f'UPDATE {table} SET {set_clause_string} WHERE {where_clause_string}'
            else:
                if force_update:
                    query = f'UPDATE {table} SET {set_clause_string}'
                else:
                    message = 'Where dictionary is None/empty. If you want to force update every row, pass force_update as True.'
                    logging.error(message)
                    return False

            params = set_value_list + where_value_list
            self.execute(query, params=params)
            return True
        except:
            logging.exception('Something went wrong updating. Check trace.')
            return False

    def get_column_names(self, table, database=None):
        """
        Get all column names from an SQL table.
        Args:
            table (str): Name of the table from which column names should be extracted.
            database (str): Name of the database in which the table lies. Leave
                it none if you want use database during object creation.
        Returns:
            (list) List of headers. (None if an error occurs)
        """
        try:
            logging.info(f'Getting column names of table {table}')
            return list(self.execute(f'SELECT * FROM {table}', database))
        except:
            logging.exception('Something went wrong getting column names. Check trace.')
            return

    def execute_default_index(self, query, database=None, **kwargs):
        """
        Executes an SQL query.
        Args:
            query (str): The query that needs to be executed.
            database (str): Name of the database to execute the query in. Leave
                it none if you want use database during object creation.
            params (list/tuple/dict): List of parameters to pass to in the query.
        Returns:
            (DataFrame) A pandas dataframe containing the data from the executed
            query. (None if an error occurs)
        """

        logging.debug(f'Executing `execute` instead of `execute_default_index`')
        return self.execute(query, index_col=None, **kwargs)
        data = None

        # # Use new database if a new databse is given
        # if database is not None:
        #     try:
        #         config = f'mysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{database}?charset=utf8'
        #         engine = create_engine(config, pool_recycle=300)
        #     except:
        #         logging.exception(f'Something went wrong while connecting. Check trace.')
        #         return False
        # else:
        #     engine = self.engine

        print('query', query)
        if database is None:
            database = 'karvy'
        data = None
        sql = query
        user_ = self.USER
        host_ = self.HOST
        database = self.DATABASE
        password_ = self.PASSWORD
        inds = [i for i in range(len(sql)) if sql[i] == '']
        for pos, ind in enumerate(inds):
            if pos % 2 == 0:
                sql = sql[:ind] + '[' + sql[ind+1:]
            else:
                sql = sql[:ind] + ']' + sql[ind + 1:]
               
        if connection_string:
            print('connection string', connection_string)
            print('database', database)
            print(type(connection_string + database))
            print(type(connection_string + database))

            try:
                conn = pyodbc.connect(connection_string + database)
            except Exception as e:
                print('Connection string invalid. ', e)
        else:
            try:
                if user_ or password_:
                    conn = pymssql.connect(host=host_,database=database,user=user_,password=password_)
                else:
                    conn = pymssql.connect(host=host_,database=database)
            except Exception as e:
                print("Error establishing connection to DB. ", e)
                conn = pymssql.connect(host=host_,database=database)

        try:
            logging.debug(f'Query: {query}')
            # data.to_sql(table, conn, **kwargs)
            curs = conn.cursor(as_dict = True)
            
            curs.execute(sql, tuple(kwargs.get('params', [])))
            print('query executed')
            try:
                data = curs.fetchall()
                data = pd.DataFrame(data)
                print(data)
            except Exception as e:
                logging.debug('Update Query')
            # data = pd.read_sql(query, conn, **kwargs)
            conn.commit()
            conn.close()
        except exc.ResourceClosedError:
            return True
        except:
            logging.exception(f'Something went wrong while executing query. Check trace.')
            params = kwargs['params'] if 'params' in kwargs else None
            return False

        return data.where((pd.notnull(data)), None).set_index('id')


    def get_all(self, table, database=None, discard=None):
        """
        Get all data from an SQL table.
        Args:
            table (str): Name of the table from which data should be extracted.
            database (str): Name of the database in which the table lies. Leave
                it none if you want use database during object creation.
            discard (list): columns to be excluded while selecting all
        Returns:
            (DataFrame) A pandas dataframe containing the data. (None if an error
            occurs)
        """
        logging.info(f'Getting all data from {table}')
        if discard:
            logging.info(f'Discarding columns {discard}')
            columns = list(self.execute_default_index(f'SHOW COLUMNS FROM {table}',database).Field)
            columns = [col for col in columns if col not in discard]
            columns_str = json.dumps(columns).replace("'",'').replace('"','')[1:-1]
            return self.execute(f'SELECT {columns_str} FROM {table}', database)

        return self.execute(f'SELECT * FROM {table}', database)

    def get_latest(self, data, group_by_col, sort_col):
        """
        Group data by a column containing repeated values and get latest from it by
        taking the latest value based on another column.
        Example:
        Get the latest products
            id     product   date
            220    6647     2014-09-01
            220    6647     2014-10-16
            826    3380     2014-11-11
            826    3380     2015-05-19
            901    4555     2014-09-01
            901    4555     2014-11-01
        The function will return
            id     product   date
            220    6647     2014-10-16
            826    3380     2015-05-19
            901    4555     2014-11-01
        Args:
            data (DataFrame): Pandas DataFrame to query on.
            group_by_col (str): Column containing repeated values.
            sort_col (str): Column to identify the latest record.
        Returns:
            (DataFrame) Contains the latest records. (None if an error occurs)
        """
        try:
            logging.info('Grouping data...')
            logging.info(f'Data: {data}')
            logging.info(f'Group by column: {group_by_col}')
            logging.info(f'Sort column: {sort_col}')
            return data.sort_values(sort_col).set_index('id').groupby(group_by_col).tail(1)
        except KeyError as e:
            logging.error(f'Column {e.args[0]} does not exist.')
            return None
        except:
            logging.exception('Something went wrong while grouping data.')
            return None