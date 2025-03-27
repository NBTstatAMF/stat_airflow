import psycopg2
import json
import pandas as pd
import xlwings 
from datetime import datetime
from pathlib import Path
from math import ceil
from collections import Counter as cnt
from io import BytesIO
import openpyxl


#==============================CLASS: Masterdata==============================#
#=============================================================================#
class Masterdata():
    def __init__(self, conf:json=None, rules:json = None):
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
        
        if rules != None:self.rules = None
        else:
            self.rules = {1:{"config_version","tables", "report_name"},
                          2:{"sheet_name", "table_type", "table_id"},
                          "3fx":{"attribute", "cell_address", "code", 
                                 "data_type","is_empty_allowed", 
                                 "is_negative_allowed", "length"
                              },
                          "3ux":{"attribute", "code"}, 
                          
                          "4fx":{"attr_type", "attr_value"},
                          "4ux":{"attr_type","cell_address", "data_type", 
                                 "is_negative_allowed", 
                                 "length"
                                 }
                         }
                            
            self.nodes = dict()                        
                                  
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

    def __get_period_details__(self, type_of_period:int, year:int, 
                               period_number:int):

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

#===============================CREATING BANK=================================#
    def create_bank (self, type_of_org:int, unique_code:str, bic4:str, \
                     name:str, label_id:str=None, status:int=1):
        m = f'   #>{datetime.now()}_creating bank {name} with ID'
        m += f'{unique_code}:{bic4}'
        print (m)

        insert_query = "INSERT INTO sma_stat_dep.tbl_entities (id, code, bic4,\
        ""name"", ""type"", label_id, status) \
        VALUES(nextval('sma_stat_dep.tbl_entities_id_seq'::regclass) \
        , %s, %s, %s, %s, %s, %s);"

        insert_values = [unique_code, bic4, name, type_of_org, label_id, 
                         status]


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

#================================REPORT TYPE =================================#        
    def create_report_type (self, code:str, version:str, name:str, \
                            validation_config: str, period_type: int,
                            submition_mode:int=0):
        m = f'   #>{datetime.now()}_creating report_type {code}'
        print (m + f" in version {version}")
        
        v_rslt = self.validate_config(validation_config)
        if (not v_rslt[0]):
            print ("   " + str(v_rslt[1]))
            return False
        validation_config = json.dumps (validation_config, ensure_ascii=False)
        period_check_query = "SELECT id FROM sma_stat_dep.tbl_period \
                              WHERE ""type"" = %s limit 1;"

        insert_query = "INSERT INTO sma_stat_dep.tbl_report_type\
                        (id, code, ""version"", ""name"", validation_config, \
                        report_period_type, submition_mode)\
                        VALUES(nextval\
                            ('sma_stat_dep.tbl_report_type_id_seq'::regclass)\
                    , %s, %s, %s, %s, %s, %s);"
        
        insert_values = [code, version, name, validation_config, period_type,
                         submition_mode]

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
                        m = f'   #>{datetime.now()}_period_type doesnt exist!'
                        print(m)
                        return False
                                        
                    cursor.execute (insert_query, insert_values)
                    print (f'   #>{datetime.now()}_cursor '
                            'execution is successful.')
                    self.__add_metadata_from_config__(cursor, mode=1)
                    m = f'   #>{datetime.now()}_report_type {code}'
                    print (m + f" in version {version} is successful.")
                    return True
            except psycopg2.errors.UniqueViolation as e: 
                m = f'   #>{datetime.now()}_the report type in this vesion'
                m+= 'already exists! exit'
                print(m)
        return False
#===============================CREATING ENT==================================#
    def create_ent (self,name:str):
        insert_query = f"INSERT INTO sma_stat_dep.tbl_ent (code) VALUES('{name}');"


        # Inserting into database 
        with psycopg2.connect(dbname=self.db_name, 
                user=self.db_user, password=self.db_pass, \
                host=self.db_host) as conn:
            print (f'   #>{datetime.now()}_conneciton to db ' 
                    'is set.') 
            try:
                with conn.cursor() as cursor:
                    cursor.execute (insert_query)
                    print (f'   #>{datetime.now()}_cursor '
                            'execution is successful.')
                    return True
            except psycopg2.errors.UniqueViolation as e: 
                print(f'   #>{datetime.now()}_the entity already exists! exit')
        return False

#===============================GET FILE===============================#        
    def get_file (self, file_id:int):
        report_type_query = f"SELECT file_name,file FROM sma_stat_dep.tbl_files \
                             WHERE id = {file_id};"

        # Inserting into database 
        with psycopg2.connect(dbname=self.db_name, 
                user=self.db_user, password=self.db_pass, \
                host=self.db_host) as conn:
            print (f'   #>{datetime.now()}_conneciton to db ' 
                    'is set.') 
            try:
                with conn.cursor() as cursor:
                    #check if IDs for the bank, period and version exists    
                    cursor.execute (report_type_query)
                    res = cursor.fetchone()
                    file_name = f'{file_id}_{res[0]}'
                    file = res[1]
                    workbook_xml = BytesIO(bytes(file)) 
                    workbook_xml.seek(0)
                        # print(openpyxl.load_workbook(workbook_xml))
                    
                    wb = openpyxl.load_workbook(workbook_xml)
                    file_path = Path(__file__).parent.parent /'files'/ file_name
                    wb.save(file_path)
                    print (f'   #>{datetime.now()}_file was created: {file_path}')
                    wb.close()
                    return wb
            except(Exception, psycopg2.Error) as error: 
                print(error)
                return error
            

#===============================GET FILE_LOGS===============================#        
    def get_file_logs (self, file_id:int):
        report_type_query = f"SELECT file_name,logs FROM sma_stat_dep.tbl_files \
                             WHERE id = {file_id};"

        # Inserting into database 
        with psycopg2.connect(dbname=self.db_name, 
                user=self.db_user, password=self.db_pass, \
                host=self.db_host) as conn:
            print (f'   #>{datetime.now()}_conneciton to db ' 
                    'is set.') 
            try:
                with conn.cursor() as cursor:
                    #check if IDs for the bank, period and version exists    
                    cursor.execute (report_type_query)
                    res = cursor.fetchone() 
                    res = {
                        "file_id":file_id,
                        "file_name":res[0],
                        "file_logs":res[1]
                    }
                    # print(res[0])
                    file_path = Path(__file__).parent.parent /'files'/ f'{res["file_id"]}_{res['file_name'][:-5]}.txt'
                    with open(file_path,'w') as file:
                        file.write(res['file_logs'])

                    return f'{res["file_id"]}_{res['file_name'][:-5]}.txt','w'
            except(Exception, psycopg2.Error) as error: 
                print(error)
                return error
                
#================================VERSIONS=====================================#        
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
            print (f'   #>{datetime.now()}_report_window is not valid ! ')

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
    
    #===============================MONITOR REPORTS==============================#
    def monitor_report (self,bank_object=None):
        arguments = ''
        for el in bank_object.keys():
            if(el=='from_date'):
                arguments += f" and {el}>=%s"
            elif(el=='to_date'):
                arguments += f" and {el}<=%s"
            elif(el=='report_code'):
                arguments += f" and db_stat_dep.sma_stat_dep.tbl_report_type.code=%s"
            else:
                arguments += f" and {el}=%s"
        arguments = arguments[4:]
        print(f'   #>{datetime.now()}_monitor_report arguments: {arguments}')
        select_query = f"select db_stat_dep.sma_stat_dep.tbl_schedule.id as schedule_id,\
            db_stat_dep.sma_stat_dep.tbl_file_per_schedule.file_id as fps_file_id,\
            db_stat_dep.sma_stat_dep.tbl_files.upload_status as upload_status,\
            db_stat_dep.sma_stat_dep.tbl_report_type.code as report_code,\
            db_stat_dep.sma_stat_dep.tbl_entities.type as entity_type,\
            db_stat_dep.sma_stat_dep.tbl_entities.bic4 as bic4,\
            db_stat_dep.sma_stat_dep.tbl_entities.name as name,\
            db_stat_dep.sma_stat_dep.tbl_period.type as period_type,\
            db_stat_dep.sma_stat_dep.tbl_period.from_date as from_date,\
            db_stat_dep.sma_stat_dep.tbl_period.to_date as to_date,\
            db_stat_dep.sma_stat_dep.tbl_schedule.reporting_window as reporting_window\
            from db_stat_dep.sma_stat_dep.tbl_schedule\
            left join db_stat_dep.sma_stat_dep.tbl_file_per_schedule\
                on db_stat_dep.sma_stat_dep.tbl_schedule.id=db_stat_dep.sma_stat_dep.tbl_file_per_schedule.schedule_id \
            left join db_stat_dep.sma_stat_dep.tbl_files\
                on db_stat_dep.sma_stat_dep.tbl_files.id=db_stat_dep.sma_stat_dep.tbl_file_per_schedule.file_id\
            left join db_stat_dep.sma_stat_dep.tbl_entities\
                on db_stat_dep.sma_stat_dep.tbl_schedule.bank_id=db_stat_dep.sma_stat_dep.tbl_entities.id\
            left join db_stat_dep.sma_stat_dep.tbl_report_type\
                on db_stat_dep.sma_stat_dep.tbl_schedule.report_type_id=db_stat_dep.sma_stat_dep.tbl_report_type.id\
            left join db_stat_dep.sma_stat_dep.tbl_period\
                on db_stat_dep.sma_stat_dep.tbl_schedule.period_id=db_stat_dep.sma_stat_dep.tbl_period.id \
        where {arguments} "
        
        query_values = list(bank_object.values())
        print(f'   #>{datetime.now()}_monitor_report arguments: {query_values}')
        # insert_values = ['2024-08-01', '2024-08-31', '4915', '1A']
        # print(insert_query)
        # print(insert_values)
        # Inserting into database 
        with psycopg2.connect(dbname=self.db_name, 
                user=self.db_user, password=self.db_pass, \
                host=self.db_host) as conn:
            print (f'   #>{datetime.now()}_conneciton to db ' 
                    'is set.') 
            try:
                with conn.cursor() as cursor:
                    cursor.execute (select_query, query_values)
                    # cursor.execute (insert_query)
                    print (f'   #>{datetime.now()}_cursor '
                            'execution is successful.')
                    return cursor.fetchall()
            except psycopg2.errors.UniqueViolation as e: 
                print(f'   #>{datetime.now()}_the entity already exists! exit')
        return False
    

    #===============================UPDATE BANK==============================#
    def update_bank (self,bank_object):
        m = f'   #>{datetime.now()}_creating bank with ID {bank_object['entity_id']}'
        print (m)
        arguments = ''
        for el in bank_object.keys():
            if(el=='entity_id'):
                continue
            arguments += f",{el}=%s"
        arguments = arguments[1:]
        insert_query = f"UPDATE sma_stat_dep.tbl_entities SET {arguments} WHERE id={bank_object['entity_id']}"
        insert_values = list(bank_object.values())[1:]

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
    
    #===============================UPDATE REPORTing WINDOW==============================#
    def update_schedule (self,schedule_id,reporting_window):
        m = f'   #>{datetime.now()}_updating schedule with ID {schedule_id}'
        print (m)
        insert_query = "UPDATE sma_stat_dep.tbl_schedule SET reporting_window=%s WHERE id=%s"
        insert_values = [reporting_window,schedule_id]

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
                print(f'   #>{datetime.now()}_the schedule already exists! exit')
        return False
    
    #===============================UPDATE ENT NAME==============================#
    def update_ent (self,ent_id,ent_name):
        m = f'   #>{datetime.now()}_updating schedule with ID {ent_id}'
        print (m)
        insert_query = "UPDATE sma_stat_dep.tbl_ent SET name=%s WHERE id=%s"
        insert_values = [ent_name,ent_id]

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
                print(f'   #>{datetime.now()}_the schedule already exists! exit')
        return False

    #===============================DELETE SCHEDULe==============================#
    def delete_schedule (self,schedule_id):
        m = f'   #>{datetime.now()}_updating schedule with ID {schedule_id}'
        print (m)
        
        # insert_values = [reporting_window,schedule_id]

        # Inserting into database 
        with psycopg2.connect(dbname=self.db_name, 
                user=self.db_user, password=self.db_pass, \
                host=self.db_host) as conn:
            print (f'   #>{datetime.now()}_conneciton to db ' 
                    'is set.') 
            try:
                with conn.cursor() as cursor:
                    get_file_per_sch_query = f"select * from sma_stat_dep.tbl_file_per_schedule WHERE schedule_id={schedule_id}"
                    cursor.execute (get_file_per_sch_query)
                    if(cursor.fetchone()==None):
                        # cursor.execute (insert_query, insert_values)
                        delete_sch_query = f"delete from sma_stat_dep.tbl_schedule WHERE id={schedule_id}"
                        cursor.execute (delete_sch_query)
                        print (f'   #>{datetime.now()}_cursor '
                                'execution is successful.')
                    else:
                        raise Exception(f'   #>{datetime.now()}_cursor '
                                'for this schedule exist datas.')
                    return True
            except psycopg2.errors.UniqueViolation as e: 
                print(e)
        return False
    
    #===============================DELETE PERIOD==============================#
    def delete_period (self,period_id):
        m = f'   #>{datetime.now()}_updating schedule with ID {period_id}'
        print (m)
        
        # insert_values = [reporting_window,schedule_id]

        # Inserting into database 
        with psycopg2.connect(dbname=self.db_name, 
                user=self.db_user, password=self.db_pass, \
                host=self.db_host) as conn:
            print (f'   #>{datetime.now()}_conneciton to db ' 
                    'is set.') 
            try:
                with conn.cursor() as cursor:
                    get_sch_query = f"select * from sma_stat_dep.tbl_schedule WHERE period_id={period_id}"
                    cursor.execute (get_sch_query)
                    if(cursor.fetchone()==None):
                        # cursor.execute (insert_query, insert_values)
                        delete_sch_query = f"delete from sma_stat_dep.tbl_period WHERE id={period_id}"
                        cursor.execute (delete_sch_query)
                        print (f'   #>{datetime.now()}_cursor '
                                'execution is successful.')
                    else:
                        raise Exception(f'   #>{datetime.now()}_cursor '
                                'for this period exist schedules.')
                    return True
            except psycopg2.errors.UniqueViolation as e: 
                print(e)
        return False
    
    #===============================DELETE ENTITIE==============================#
    def delete_entitie (self,entitie_id):
        m = f'   #>{datetime.now()}_updating schedule with ID {entitie_id}'
        print (m)
        
        # insert_values = [reporting_window,schedule_id]

        # Inserting into database 
        with psycopg2.connect(dbname=self.db_name, 
                user=self.db_user, password=self.db_pass, \
                host=self.db_host) as conn:
            print (f'   #>{datetime.now()}_conneciton to db ' 
                    'is set.') 
            try:
                with conn.cursor() as cursor:
                    get_sch_query = f"select * from sma_stat_dep.tbl_schedule WHERE period_id={entitie_id}"
                    cursor.execute (get_sch_query)
                    if(cursor.fetchone()==None):
                        # cursor.execute (insert_query, insert_values)
                        delete_sch_query = f"delete from sma_stat_dep.tbl_entities WHERE id={entitie_id}"
                        cursor.execute (delete_sch_query)
                        print (f'   #>{datetime.now()}_cursor '
                                'execution is successful.')
                    else:
                        raise Exception(f'   #>{datetime.now()}_cursor '
                                'for this entitie exist schedules.')
                    return True
            except psycopg2.errors.UniqueViolation as e: 
                print(e)
        return False
    
#=============================================================================#
#===============================CONFIG_VALIDATOR==============================#
#=============================================================================#
    def validate_config (self, config):
    # mehtod validates config for each reports by applying different 
    # validation sequence depending on the type of the reports (fixed or 
    # unfixed) agains self.rules. Return tuple (True, None) if executed 
    # successfully and tuple (False, Exception) if failed in execution
        self.nodes = dict()    
        try:
            d_nodes = dict()
            l_attr = []
            cells = []
            # m for error message
            m = "compulsary top-level fields is asbencent"
            self.__try_rule__ (1, config, m, 1)
        
            for tbl in config["tables"]:
                m = r'compulsary "tables"-child-fields is asbencent'
                self.__try_rule__(2,tbl, m)
                
                if(tbl["table_type"] == "fixed"):
                    if tbl.get("nodes"):
                        for node in tbl["nodes"]:
                            m = r'compulsary "node"-child-fields is '
                            'asbencent'
                            self.__try_rule__ ("3fx", node, m)

                            for attr in node["attribute"]:
                                m = r'compulsary "node"''s attribute'
                                m += "-child-fields is asbencent"
                                self.__try_rule__ ('4fx', attr, m )

                                temp = {
                                    'attr_type':attr['attr_type']
                                }
                                if not attr.get('unit') is None: 
                                    temp.update({'unit':\
                                                tuple(attr['unit'])})
                                l_attr.append(temp)
                            
                            # this is specific routine for "fixed" tables 
                            # adding the attribute "value" with its
                            # properties from the config file 
                            temp = {
                                'data_type':node['data_type'],
                                'length':node['length'],
                                'is_empty_allowed':\
                                    node['is_empty_allowed'],
                                'is_negative_allowed':\
                                    node['is_negative_allowed'],
                                'attr_type':'value'
                            }   
                            l_attr.append(temp)

                            d_nodes.update({node["code"]: l_attr})
                            l_attr = []
                            cells.append(node['cell_address'])
 
                elif (tbl["table_type"] == "unfixed"):
                    if tbl.get("nodes") and len(tbl["nodes"]) == 1:
                        node = tbl.get("nodes")[0]
                        m = f'compulsary {node["code"]}''s-child-fields' 
                        ' are asbencent'
                        self.__try_rule__ ("3ux", node, m)
                        for attr in node["attribute"]:
                            m = f'compulsary {node["code"]} attr'
                            "'s-child-fields are asbencent"
                            self.__try_rule__("4ux", attr, m)
                            temp = {
                                'data_type':attr['data_type'],
                                'length':attr['length'],
                                'is_empty_allowed':attr['is_empty_allowed'],
                                'is_negative_allowed':\
                                    attr['is_negative_allowed'],
                                'attr_type':attr['attr_type']
                            }
                            if not attr.get('attr_allowed_value') is None: 
                                temp.update({'attr_allowed_value':\
                                            tuple(attr['attr_allowed_value'])})

                            l_attr.append(temp)
                            cells.append(attr['cell_address'])
                        d_nodes.update({node["code"]: l_attr})
                        l_attr = []
  
                    elif (len(tbl["nodes"]) != 1):
                        m = "more that one notes in unfixed table is not"  
                        m += "allowed"
                        raise Exception(m)    
                        
             # checking for non unique cell addressed in nodes          
            if len(cells) != len(set(cells)):
                dict_cells = dict()
                for cell in cells:
                    if not dict_cells.get(cell):
                        dict_cells.update ({cell:1})
                    else:
                        i = dict_cells[cell]
                        dict_cells.update ({cell:i+1})
                rep_cells = [(k,v) for k,v in dict_cells.items() if v>1]
                 
                raise Exception(f"  wrong config, repetative reffernce to" \
                                 f" a sinlge cell address:\n   {rep_cells}")

            
            # if everything is ok    
            self.nodes = d_nodes
            return True, None
        except Exception as e:
            print (e)
            return False, e
        
    def __try_rule__ (self, rule_key, var_list:dict, msg, compare_type:int=0):
            if compare_type == 0:
                l1 = self.rules[rule_key]
                l2 = set(var_list.keys())
                list_bool = [True for i in l1 if i in l2]
                if sum(list_bool) != len(l1):
                    raise Exception (msg+f"...\n search failed:" 
                                     f"{l1}, in: {l2}")
                    
            elif compare_type == 1:
                if cnt(set(var_list.keys())) != cnt(self.rules[rule_key]):
                    raise Exception (msg + f"\n {cnt(set(var_list.keys()))}"\
                                     + f"!= {cnt(self.rules[rule_key])}" )
            else: raise Exception("config_validator.try_rule:bad " 
                                  "compare_type")
            
    def __add_metadata_from_config__ (self, cursor=None, mode:int = 0):
    # take the node code (mode 0) and node code with attribute type code 
    # (mode 1) and insert into database. Return True if successfull, or False
    # TODO insert for atributes 
        if not mode in [0,1]:
            raise Exception("bad parameter for arg. mode")          
        
        print (f'#>{datetime.now()}_add_metadata_from_config is started...')
        
        inst_ent_query = "INSERT INTO sma_stat_dep.tbl_ent \
            (id, code, tstp) \
            VALUES (nextval('sma_stat_dep.tbl_ent_id_seq'::regclass) \
                    , %s, NOW());"                          

        inst_attr_query = "INSERT INTO sma_stat_dep.tbl_attrs \
            (id, code, tstp, properties) \
            VALUES (nextval('sma_stat_dep.tbl_attrs_id_seq'::regclass) \
                    , %s, NOW(), %s);"  
        
        for node in self.nodes:
            try:
                cursor.execute('SAVEPOINT sp1')
                cursor.execute(inst_ent_query, (node,))
                print (f'   #>{datetime.now()}_cursor execution is ' \
                        'successful.')
            except psycopg2.errors.UniqueViolation: 
                print(f'   #>{datetime.now()}_the ent {node} is ' \
                        'already exists! going to next')
                cursor.execute('ROLLBACK TO SAVEPOINT sp1')
                pass

            if mode == 1:        
                for attr in self.nodes[node]:
                    attr_type = attr.pop("attr_type")
                    attr = json.dumps (attr)
                    cursor.execute('SAVEPOINT sp1')
                    
                    try:   
                        cursor.execute(inst_attr_query, (attr_type, attr))
                        print (f'   #>{datetime.now()}_' \
                               'cursor execution is successful.')

                    except psycopg2.errors.UniqueViolation: 
                        print(f'   #>{datetime.now()}_the ent {attr_type} is' \
                                ' already exists! going to next')
                        cursor.execute('ROLLBACK TO SAVEPOINT sp1')
                        pass
                     
                cursor.execute('RELEASE SAVEPOINT sp1')
        self.nodes = {}
        print (f'#>{datetime.now()}_add_metadata_from_config is over.') 

    #=========================================================================#
    #=========================   MAP_TO_TAMPLATE  ============================#
    #=========================================================================#
    def map_to_template (self, conf:json, wb_path:Path = None, \
                         outbook_name:str=None, worksheet_pass:str=None):
    # map metadata to reporting template, allows to visually test correctness
    # of conig file 
              
        m = f'   #>{datetime.now()}_map_to_template is start'
        print (m)
        
        v_rslt = self.validate_config(conf)
        if (not v_rslt[0]):
            print ("   " + str(v_rslt[1]))
            return False

        report_name = conf['report_name']
        tables = [e for e in conf['tables']]

        if outbook_name is None: 
            book_name = f"test_{report_name}.xlsx"
        else: book_name = outbook_name    

        try:
            xl = xlwings.App(visible=False)
            if (wb_path is None):
                wb =  xl.books.add()
            else:
                wb = xl.books.open(wb_path)
                
            for tbl in tables:
                sheet_name = tbl['sheet_name']
                if (not sheet_name in wb.sheet_names):
                    wb.sheets.add(sheet_name)
                ws = wb.sheets(sheet_name)
                if not worksheet_pass is None:
                    ws.api.Unprotect(Password=worksheet_pass)
                # if it's empty conf's tbl:
                if tbl.get('nodes') is None: continue 
                tbl_type = tbl['table_type']    
                i = 0
                for node in tbl['nodes']:
                    node_code = node['code']
                    print (f'processing {node_code}')
                    if tbl_type == 'fixed':
                        cell_address = node['cell_address']
                        data_type = node['data_type']
                        val = f'"{cell_address}"_"{node_code}"'
                        for attr in node['attribute']:
                            attr_type = attr['attr_type']
                            attr_value = attr['attr_value']
                            val += f': "{attr_type}:{attr_value}, "'
                        ws.range(cell_address).value = val
                        ws.range(cell_address).color = \
                            self.__type_to_hexcolor__(data_type,i)
                        i+=1
                        if (i>4):i=0

                    elif tbl_type == 'unfixed':
                        for attr in node['attribute']:
                            attr_type = attr['attr_type']
                            cell_address = attr['cell_address']
                            data_type = attr['data_type']
                            val = f'"{cell_address}"_"{node_code}"'\
                                    + f': "{attr_type}"'
                            ws.range(cell_address).value = val
                            row = ws.range(cell_address).row
                            column = ws.range(cell_address).column
                            for i in range (0, 5):
                                ws.cells(row + i, column).color = \
                                self.__type_to_hexcolor__(data_type,i)

        except Exception as e:
            print (e)
            wb.close()
            xl.visible = True
            xl.quit()
        finally: 
            xl.visible = True
            pass
                
        print ("finished")
        
    def __type_to_hexcolor__(self, datatype:str, brightness_lvl:int=0):
        if (brightness_lvl < 0) and (brightness_lvl > 4):
            raise Exception ('__type_to_hexcolor__ provide with incorr. param.')
        type_colors ={
            'int':['#3498db','#5dade2','#85c1e9','#aed6f1','#d6eaf8'],
            'float':['#9b59b6','#af7ac5','#c39bd3','#d7bde2','#ebdef0'],
            'str':['#d35400','#dc7633','#e59866','#edbb99','#f6ddcc'],
            'bool':['#b3b6b7','#bdc3c7','#cacfd2','#bfc9ca','#d5dbdb'],
            'date':['#27ae60','#52be80','#7dcea0','#a9dfbf','#d4efdf']
        }
        return type_colors[datatype][brightness_lvl]
        


if (__name__ == '__main__'):

    path = Path(__file__).parent
    print (f'path of config file:  {path}')
    email_conf = json.load (open(path / ("config/email_conf.json")))
    
    
    master_date = Masterdata(email_conf)
    

    # master_date.create_version('ORIG', 'ҳисоботҳои дар шакли ҳамадавраҳа')
    
    # master_date.create_bank(2, '000000005','1805','ҶСП "Аввалин бонки молиявии хурд"',0, 1)
    
    #for i in range (1,13):
    #    master_date.create_period(4,2024,i)
  
    # master_date.create_shedule (1, 2, 23, 1, 9)

    conf = json.load (open(path / ("config/report_configs/1HK_config.json"),encoding="utf-8"))
    tmpl = path / ("config/report_configs/templates/1HK.v0.1101.31082024.xlsx")
    # tmpl = None
   # master_date.create_report_type("1A", "v0.000","Ҳайяти кормандон", conf, 4, 0)

    #print (master_date.validate_config())
    # master_date.map_to_template(conf=conf,wb_path=tmpl, worksheet_pass="stat4omor")

    # master_date.get_file(890)





        
    # master_date.update_bank({'entity_id':80,'type':1, 'code':'0000000086','bic4':'1111','name':'ҶСП "Мой банк!"','label_id':0, 'status':1})
    # master_date.update_bank({'entity_id':80,'bic4':'112','status':1})
    # master_date.update_schedule (1,350)
    # master_date.update_ent (1,"")
    # master_date.delete_schedule(1)
    # master_date.delete_period(1)
    # master_date.delete_entitie(80)
    # master_date.monitor_report({'from_date':'2024-08-01', 'bic4':'4915','to_date':'2024-08-31','report_code':'1A'})
    # master_date.monitor_report({'bic4':'4915'})
    master_date.get_file_logs(890)



bics = [1,'00000001',1101,'Бонки миллии Тоҷикистон',0,1],[1,'00000002',1369,'ҶСК "Ориёнбонк"',0,1],[1,'00000003',1626,'БДА ҶТ "Амонатбонк"',0,1],[1,'00000004',5707,'ҶСК "Бонки Эсхата"',0,1],[1,'00000005',1805,'ҶСП "Аввалин бонки молиявии хурд"',0,1],[1,'00000006',1736,'ҶСП "Бонки рушди Тоҷикистон"',0,1],[1,'00000007',1706,'Филиали бонки "Тиҷорат"-и  ҶИЭ дар ш. Душанбе',0,1],[1,'00000008',1779,'ҶСП "Халиқ Бонк Тоҷикистон"',0,1],[1,'00000009',1799,'ҶСП "Кафолатбонк"',0,1],[1,'000000010',5848,'ҶСП Бонки "Арванд"',0,1],[1,'000000011',1808,'ҶСП "Спитамен Бонк" ',0,1],[1,'000000012',1803,'ҶСП "Бонки байналмилалии Тоҷикистон"',0,1],[1,'000000013',1858,'ҶСК "Коммерсбонки Тоҷикистон" ',0,1],[1,'000000014',1900,'ҶСК "Алиф Бонк"',0,1],[1,'000000015',1655,'КВДБССТ "Саноатсодиротбонк"',0,1],[2,'000000016',1841,'ҶСП "Душанбе Сити Бонк"',0,1],[1,'000000017',1820,'ҶДММ ТҚҒ "Васл" ',0,1],[3,'000000018',1720,'ҶСК "Тавҳидбонк"',0,1],[3,'000000019',1823,'ҶДММ ТАҚХ"Зудамал"',0,1],[3,'000000020',5859,'ҶДММ ТАҚХ "Азизӣ-Молия"',0,1],[3,'000000021',1890,'ҶДММ ТАҚХ "Сарват М"',0,1],[3,'000000022',1891,'ҶДММ ТАҚХ "Тезинфоз"',0,1],[3,'000000023',1895,'ҶСП ТАҚХ "Ардо-капитал"',0,1],[3,'000000024',1899,'ҶДММ ТАҚХ "Пайванд гурух"',0,1],[3,'000000025',1875,'ҶДММ ТАҚХ "ФИНКА"',0,1],[3,'000000026',1892,'ҶСП ТАҚХ "Ҳумо"',0,1],[3,'000000027',1878,'ҶДММ ТАҚХ "Фазо С"',0,1],[3,'000000028',1817,'ҶСП ТАҚХ "Ҳамров"',0,1],[3,'000000029',1872,'ҶДММ ТАҚХ "Сомон-Тиҷорат"',0,1],[3,'000000030',1970,'ҶДММ ТАҚХ "Шукр Молия"',0,1],[3,'000000031',1971,'ҶДММ ТАҚХ "ЭМИН-сармоя"',0,1],[3,'000000032',1972,'ҶДММ ТАҚХ "Баракат Молия"',0,1],[3,'000000033',4910,'ЧДММ  ТАКХ  "Фуруз"',0,1],[3,'000000034',5876,'ҶСП ТАҚХ "Имон Интернешнл"',0,1],[3,'000000035',1857,'ҶДММ ТАҚХ "Арғун"',0,1],[3,'000000036',5849,'ҶДММ ТАҚХ "МАТИН"',0,1],[3,'000000037',5880,'ҶДММ ТАҚХ "Сандуқ"',0,1],[4,'000000038',1870,'ҶДММ ТАҚХ "Тамвил"',0,1],[4,'000000039',1907,'ҶДММ ТҚХ "ОКСУС"',0,1],[4,'000000040',4909,'ҶДММ ТҚХ "Меҳнатобод"',0,1],[5,'000000041','0904','ҶДММ ТҚХ "Рушди Куҳистон"',0,1],[5,'000000042',1901,'ФҚХ "НУРИ ҲУМО"',0,1],[5,'000000043',1904,'ФҚХ "Зар"',0,1],[5,'000000044',1924,'ФҚХ "ИМДОДИ ХУТАЛ"',0,1],[5,'000000045',1925,'ФҚХ "Эҳёи кӯҳистон"',0,1],[5,'000000046',1932,'ФҚХ "Қуллаи Умед"',0,1],[5,'000000047',1965,'ФҚХ "Фонди бозтамвил"',0,1],[5,'000000048',2902,'ФҚХ "СОЛИҲИН"',0,1],[5,'000000049',2906,'ФҚХ "Мададгор-Д"',0,1],[5,'000000050',4901,'ФҚХ "Боршуд"',0,1],[5,'000000051',4902,'ФҚХ "Имконият"',0,1],[5,'000000052',4903,'ФҚХ "Чилучор Чашма"',0,1],[5,'000000053',4911,'ФҚХ "ЗАЙНАЛОБИДДИН 1"',0,1],[5,'000000054',4914,'ФҚХ "ПАХТАОБОД"',0,1],[5,'000000055',4915,'ФҚХ "ДЕХКОНАРИК"',0,1],[5,'000000056',4916,'ФҚХ "САРВАТИ ВАХШ"',0,1],[5,'000000057',4917,'ФҚХ "ТУГАРАКИЁН"',0,1],[5,'000000058',5901,'ФҚХ "Имон"',0,1],[5,'000000059',5902,'ФҚХ "Сарпараст"',0,1],[5,'000000060',5903,'ФҚХ "МикроИнвест"',0,1],[5,'000000061',5906,'ФҚХ "Равнақ"',0,1],[5,'000000062',5907,'ФҚХ "Ҳамёрӣ"',0,1],[5,'000000063',5908,'ФҚХ "Барор"',0,1],[5,'000000064',5912,'ФҚХ "Рушди Суғд"',0,1],[5,'000000065',5913,'ФҚХ "Рушди Водии Зарафшон"',0,1],[5,'000000066',1957,'ФҚХ "Роҳнамо"',0,1],[5,'000000067','0901','ФҚХ "Мадина"',0,1]

periods = [1,2,3,4,5,6,7,8,9,10,11,23]

# master_date.create_shedule (2, 78, 1, 1, 300)
# for period in periods:
    # master_date.create_shedule (2, 76, period, 1, 350)
#     for bank_id in range(2,76):
#         master_date.create_shedule (3, bank_id, period, 1, 350)




# for el in [
#     "4k.000_prev",
#     "4k.000_curr",
#     "4k.100_prev",
#     "4k.100_curr",
#     "4k.125_prev",
#     "4k.125_curr",
#     "4k.150_prev",
#     "4k.150_curr",
#     "4k.200_prev",
#     "4k.200_curr",
#     "4k.225_prev",
#     "4k.225_curr",
#     "4k.250_prev",
#     "4k.250_curr",
#     "4k.300_prev",
#     "4k.300_curr",
#     "4k.305_prev",
#     "4k.305_curr",
#     "4k.310_prev",
#     "4k.310_curr",
#     "4k.315_prev",
#     "4k.315_curr",
#     "4k.320_prev",
#     "4k.320_curr",
#     "4k.325_prev",
#     "4k.325_curr",
#     "4k.330_prev",
#     "4k.330_curr",
#     "4k.335_prev",
#     "4k.335_curr",
#     "4k.340_prev",
#     "4k.340_curr",
#     "4k.345_prev",
#     "4k.345_curr",
#     "4k.350_prev",
#     "4k.350_curr",
#     "4k.400_prev",
#     "4k.400_curr",
#     "4k.500_prev",
#     "4k.500_curr",
#     "4k.600_prev",
#     "4k.600_curr",
#     "4k.625_prev",
#     "4k.625_curr",
#     "4k.700_prev",
#     "4k.700_curr"
# ]:master_date.create_ent (el)





# def misol_list(val,list=list()):
#     list.append(val)
#     return list
# list1 = misol_list(10)
# list2 = misol_list(111,[])
# list3 = misol_list('a')

# # print(list1)
# print(list2)
# print(list3)
