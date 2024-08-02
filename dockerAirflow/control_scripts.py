import psycopg2
import json
import pandas as pd
from datetime import datetime
from pathlib import Path
from math import ceil


#==============================CLASS: Masterdata================================#
#===============================================================================#
class Masterdata():
    def __init__(self, conf:json=None):
        if (conf is None):
            txt = f'#>{datetime.now()}_User@StatDep: Failed to create '
            txt = txt + f'messageEmailRobot: valid config is not privided' 
            print (txt)
            return 
        self.db_host = conf["db_host"]
        self.db_port = conf["db_port"]
        self.db_name = conf["db_name"]
        self.db_user = conf["db_user"]
        self.db_pass = conf["db_pass"]

        self.master_data_conf = {'type_of_org':{'BNK': 1,
                                                'NBK': 2,
                                                'MDO': 3,
                                                'MKO': 4,
                                                'MKF': 5
                                                },
                                  'type_of_period':{
                                                   'DLY':1,
                                                   'WLY':2,
                                                   '10D':3,
                                                   'MTY':4,
                                                   'QTY':5,
                                                   'ANY':6     
                                  }
        }
        print (f'#>{datetime.now()}_User@StatDep: Masterdata is created:')

#===============================CREATING PERIOD===============================#        
    def create_period (self, type_of_period:int, year:int, order_number:int):
        m = f'   #>{datetime.now()}_creating period with type {type_of_period}'
        print (m)

        period_details = self.__get_period_details__(type_of_period, \
                                                     year, order_number)

        insert_query = "INSERT INTO sma_stat_dep.tbl_period \
        (id, ""type"", from_date, to_date, order_number) \
        VALUES(nextval('sma_stat_dep.tbl_period_id_seq'::regclass) \
        , %s, %s, %s, %s);"

        insert_values = [type_of_period, period_details[0] \
                         , period_details[1],order_number]

        # Inserting into database 
        with psycopg2.connect(dbname=self.db_name, 
                user=self.db_user, password=self.db_pass, \
                host=self.db_host) as conn:
            print (f'   #>{datetime.now()}_conneciton to db ' 
                    'is set.') 
            try:
                with conn.cursor() as cursor:
                    cursor.execute (insert_query, insert_values)
                    print (f'   #>{datetime.now()}_cursor '
                            'execution is successful.')
                    return True
            except psycopg2.errors.UniqueViolation as e: 
                print(f'   #>{datetime.now()}_the period already exists! exit')
        return False

    def __get_period_details__(self, type_of_period:int, year:int, period_number:int):

        if (type_of_period == 1):
            #TODO

            print (f'#>{datetime.now()}_User@StatDep:' + \
                   'period is not implemented!')
            return

        if (type_of_period == 2):
             #TODO
            print (f'#>{datetime.now()}_User@StatDep:' + \
            'daily period not implemented!')
            return 
        
        if (type_of_period == 3):
             #TODO
            print (f'#>{datetime.now()}_User@StatDep:' + \
            'daily period not implemented!')
            return 
            
        if (type_of_period == 4):
            period = pd.Period (f'{year}-{period_number}-1','M')
        
        if (type_of_period == 5):
            period = pd.Period (f'{year}-{3*period_number}-1','Q')

        if (type_of_period == 6):
            period = pd.Period (f'{year}-1-1','A')

        from_date = period.start_time.strftime('%Y-%m-%d')
        to_date = period.end_time.strftime('%Y-%m-%d')

        return from_date, to_date

        
#===============================CREATING BANK==================================#
    def create_bank (self, type_of_org:int, unique_code:str, bic4:str, \
                     name:str, label_id:str=None, status:int=1):
        m = f'   #>{datetime.now()}_creating bank {name} with ID'
        m += f'{unique_code}:{bic4}'
        print (m)

        insert_query = "INSERT INTO sma_stat_dep.tbl_entities (id, code, bic4,\
        ""name"", ""type"", label_id, status) \
        VALUES(nextval('sma_stat_dep.tbl_entities_id_seq'::regclass) \
        , %s, %s, %s, %s, %s, %s);"

        insert_values = [unique_code, bic4, name, type_of_org, label_id, status]


        # Inserting into database 
        with psycopg2.connect(dbname=self.db_name, 
                user=self.db_user, password=self.db_pass, \
                host=self.db_host) as conn:
            print (f'   #>{datetime.now()}_conneciton to db ' 
                    'is set.') 
            try:
                with conn.cursor() as cursor:
                    cursor.execute (insert_query, insert_values)
                    print (f'   #>{datetime.now()}_cursor '
                            'execution is successful.')
                    return True
            except psycopg2.errors.UniqueViolation as e: 
                print(f'   #>{datetime.now()}_the entity already exists! exit')
        return False

#================================REPORT TYPE ==================================#        
    def create_report_type (self, code:str, version:str, name:str, \
                            validation_config: str, period_type: int):
        m = f'   #>{datetime.now()}_creating report_type {code}'
        print (m + f"in version {version}")

        period_check_query = "SELECT id FROM sma_stat_dep.tbl_period \
                              WHERE ""type"" = %s limit 1;"

        insert_query = "INSERT INTO sma_stat_dep.tbl_report_type\
                        (id, code, ""version"", ""name"", validation_config, \
                        report_period_type)\
                        VALUES(nextval\
                            ('sma_stat_dep.tbl_report_type_id_seq'::regclass)\
                    , %s, %s, %s, %s, %s);"

        insert_values = [code, version, name, validation_config,period_type]

        # Inserting into database 
        with psycopg2.connect(dbname=self.db_name, 
                user=self.db_user, password=self.db_pass, \
                host=self.db_host) as conn:
            print (f'   #>{datetime.now()}_conneciton to db ' 
                    'is set.') 
            try:
                with conn.cursor() as cursor:
                    cursor.execute (period_check_query, (period_type, ))
                    if cursor.fetchone() == None: 
                        print(f'   #>{datetime.now()}_period_type doesnt exist!')
                        return False
                                        
                    cursor.execute (insert_query, insert_values)
                    print (f'   #>{datetime.now()}_cursor '
                            'execution is successful.')
                    return True
            except psycopg2.errors.UniqueViolation as e: 
                m = f'   #>{datetime.now()}_the report type in this vesion'
                m+= 'already exists! exit'
                print(m)
        return False
    
#================================VERSIONS==================================#        
    def create_version (self, code:str, name:str):
        m = f'   #>{datetime.now()}_creating version {code}'
        print (m + f"with name {name}")

        insert_query = "INSERT INTO sma_stat_dep.tbl_version \
                        (id, code, ""name"") \
            VALUES(nextval('sma_stat_dep.tbl_version_id_seq'::regclass),\
                        %s, %s)"

        insert_values = [code, name]

        # Inserting into database 
        with psycopg2.connect(dbname=self.db_name, 
                user=self.db_user, password=self.db_pass, \
                host=self.db_host) as conn:
            print (f'   #>{datetime.now()}_conneciton to db ' 
                    'is set.') 
            try:
                with conn.cursor() as cursor:
                    cursor.execute (insert_query, insert_values)
                    print (f'   #>{datetime.now()}_cursor '
                            'execution is successful.')
                    return True
            except psycopg2.errors.UniqueViolation as e: 
                m = f'   #>{datetime.now()}_the this vesion'
                m+= ' already exists! exit'
                print(m)
        return False
    
#===============================CREATE SCHEDULE===============================#        
    def create_shedule (self, report_type_id:int, bank_id:int, period_id:int\
                        , version_id:int, reporting_window:int = 6):
        if reporting_window > 1200 and reporting_window < 0  :
            pritn (f'   #>{datetime.now()}_report_window is not valid ! ')

        m = f'   #>{datetime.now()}_creating shedule for '
        m += f' report_type_id-bank_id-period_id-version_id: '
        m += f'{report_type_id}-{bank_id}-{period_id}-{version_id}'
        print (m)

        # queries to ensure IDs for the bank, period and version exists

        report_type_query = "SELECT id FROM sma_stat_dep.tbl_report_type \
                             WHERE id = %s;" 
        bank_query =        "SELECT id FROM sma_stat_dep.tbl_entities \
                             WHERE id = %s;"
        period_query =      "SELECT id FROM sma_stat_dep.tbl_period \
                             WHERE id = %s;"
        version_query =     "SELECT id FROM sma_stat_dep.tbl_version \
                             WHERE id = %s;"
    
        insert_query = "INSERT INTO sma_stat_dep.tbl_schedule \
        (id, report_type_id, bank_id, period_id, version_id, reporting_window)\
        VALUES(nextval('sma_stat_dep.tbl_schedule_id_seq'::regclass),\
                         %s, %s, %s, %s, %s);"
        
        insert_values = [report_type_id, bank_id, period_id, version_id \
                         , reporting_window]

        # Inserting into database 
        with psycopg2.connect(dbname=self.db_name, 
                user=self.db_user, password=self.db_pass, \
                host=self.db_host) as conn:
            print (f'   #>{datetime.now()}_conneciton to db ' 
                    'is set.') 
            try:
                with conn.cursor() as cursor:
                    #check if IDs for the bank, period and version exists    
                    
                    cursor.execute (report_type_query, (report_type_id,))
                    if cursor.fetchone() == None: 
                        print(f'   #>{datetime.now()}_rep.type doesnt exist!')
                        return False    
                    cursor.execute (bank_query, (bank_id,))
                    if cursor.fetchone() == None: 
                        print(f'   #>{datetime.now()}_bank doesnt exist!')
                        return False    
                    cursor.execute (period_query, (period_id,))
                    if cursor.fetchone() == None: 
                        print(f'   #>{datetime.now()}_period doesnt exist!')
                        return False    
                    cursor.execute (version_query, (version_id,))
                    if cursor.fetchone() == None: 
                        print(f'   #>{datetime.now()}_version doesnt exist!')
                        return False    
                    
                    # after checking bank, period, version exist 
                    # then create schedule...
                    cursor.execute (insert_query, insert_values)
                    print (f'   #>{datetime.now()}_cursor '
                            'execution is successful.')
                    return True
            except psycopg2.errors.UniqueViolation as e: 
                m = f'   #>{datetime.now()}_the combination of'
                m+= ' report_type_id-bank_id-period_id-version_id'
                m+= f' already exists (schedule exists)! exit'
                print(m)
        return False


if (__name__ == '__main__'):

    path = Path(__file__).parent
    print (f'path of config file:  {path}')
    email_conf = json.load (open(path / ("config/email_conf.json")))

    master_date = Masterdata(email_conf)

    
    
    # master_date.create_version('ORIG', 'ҳисоботҳои дар шакли ҳамадавраҳа')
    
    # master_date.create_bank(1, '000000001', '5707', 'ҶСК "Бонки Эсхата"', 0 ,1)
    
    # master_date.create_period(4,2024,1)

    # master_date.create_report_type("5HKO", "v0.000","TEST", r"{example_conf}", 4)
    
    # master_date.create_shedule (1, 1, 16, 1, 9)


