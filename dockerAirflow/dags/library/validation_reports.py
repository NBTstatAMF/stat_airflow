from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.python import PythonOperator
from pathlib import Path
from datetime import datetime
import psycopg2
from io import BytesIO
import openpyxl
import traceback
import collections 
# from  modules.my_excel_conf import excel_my_cnfg
# from  modules.conf import exportFile  

import io
import pandas as pd
import json 


def get_hello():
    path = Path(__file__).parent.parent.parent
    print (f'path of config file:  {path}')
    conf = json.load (open(path / ("config/email_conf.json")))  
    db_host = conf["db_host"]
    db_port = conf["db_port"]
    db_name = conf["db_name"]
    db_user = conf["db_user"]
    db_pass = conf["db_pass"]

    connection = psycopg2.connect(user=db_user,
                                                password=db_pass,
                                                host=db_host,
                                                port=db_port,
                                                database=db_name)
    cursor = connection.cursor()

    configs = {
        "errors":{
        "0001":{
            "en":"",
            "ru":"Неверное значение в ячейке, возможно есть ссылка, формула с ошибкой",
            "tj":"",
        },
        "0002":{
            "en":"",
            "ru":"Неверный тип данных в ячейке",
            "tj":"",
        },
        "0003":{
            "en":"",
            "ru":"Ячейке не должна быть пустой",
            "tj":"",
        },
        "0004":{
            "en":"",
            "ru":"Значение не должен быть отрицательной",
            "tj":"",
        },
        "0005":{
            "en":"",
            "ru":"В ячейке указанно слишком длинное значение",
            "tj":"",
        },
    }
    } 
    emc = {
        "cells_codes":[
            ["A",1],
            ["B",2],
            ["C",3],
            ["D",4],
            ["E",5],
            ["F",6],
            ["G",7],
            ["H",8],
            ["I",9],
            ["J",10],
            ["K",11],
            ["L",12],
            ["M",13],
            ["N",14],
            ["O",16],
            ["P",17],
            ["Q",18],
            ["R",19],
            ["S",20],
            ["T",21],
            ["U",22],
            ["V",23],
            ["W",24],
            ["X",25],
            ["Y",26],
            ["Z",27],
            ["AA",28],
            ["AB",29],
            ["AC",30],
            ["AD",31],
            ["AE",32],
            ["AF",33],
            ["AG",34],
            ["AH",35],
            ["AI",36],
            ["AJ",37],
            ["AK",38],
            ["AL",39],
            ["AM",40],
            ["AN",41],
            ["AO",42],
            ["AQ",43],
            ["AR",44],
            ["AS",45],
            ["AT",46],
            ["AU",47],
            ["AV",48],
            ["AW",49],
            ["AX",50],
            ["AY",51],
            ["AZ",52],
            ["BA",53],
        ],
        "types":{
            'int':int,
            'str':str,
            'bool':bool,
            'object':object,
            'float':float,
        }
    }
    excelCells = emc["cells_codes"]
    types = emc["types"]
    def check_file_name(text):
        res = text.split('.')
        date = f'{res[3][-4:]}-{res[3][2:4]}-{res[3][0:2]}' 
        date = datetime.strptime(date, '%Y-%m-%d')
        print(date)
        return {
            "name":res[0],
            "version":res[1],
            "bic4":res[2],
            "date":str(date)
        }
    def selectOne(table,prop,value,get_arguments=None):
                        get_arguments = "id" if get_arguments is None else get_arguments
                        postgres_insert_query = f"select {get_arguments} from sma_stat_dep.{table} WHERE {value}" if prop is None else f"select {get_arguments} from sma_stat_dep.{table} WHERE {prop}='{value}'"
                        cursor.execute(postgres_insert_query)
                        try:
                            if get_arguments=="id":
                                return cursor.fetchone()[0]
                            else:
                                 return cursor.fetchone()
                        except:
                             return False 

    def findeCEll(arg):
            res = arg
            for i in range(len(excelCells)):
                if(arg.find(excelCells[i][0])>=0):
                    res = [excelCells[i][1]-1,int(arg[len(excelCells[i][0]):])-1]  
            return res
    
    def select_datas(file_upload_id):
            try:  
                postgres_insert_query = f"select * from sma_stat_dep.tbl_files WHERE id_file_upload={file_upload_id}"
                cursor.execute(postgres_insert_query) 
                mobile_records = cursor.fetchone() 
                file_id = mobile_records[0]
                logs = {
                            "count":0,
                            "context":'\n',
                            'upload_date':datetime.today().strftime('%Y-%m-%d %H:%M:%S')
                        }
                status = 1
                try:
                    arg = check_file_name(mobile_records[3]) 
                    # selectOne('tbl_report_type','to_date',arg['name'])

                        

                    # print(arg['name'])
                    # print(arg['version'])
                    # print(arg['bic4'])
                    # print(datetime.strptime(arg['date'],"%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%d"))
                    # print(selectOne('tbl_entities','bic4',arg['bic4']))
                    # print(selectOne('tbl_period','to_date',arg['date']))
                    
                    
                    report = selectOne('tbl_report_type','code',arg['name'],'id,report_period_type')
                    # print(selectOne('tbl_period',None,f"type={report[1]} and to_date="+f"'{arg['date']}'"))
                    # print(report[0])
                    # print(report[1])
                    # print(arg)
                    # print(selectOne('tbl_entities','bic4',arg['bic4']))
                    # print(selectOne('tbl_period',None,f"type={report[1]} and to_date="+f"'{arg['date']}'"))
                    # postgres_insert_query = "select * from sma_stat_dep.tbl_schedule WHERE bank_id=1369 AND version_id=1"
                    bank_id = selectOne('tbl_entities','bic4',arg['bic4'])
                    period_id = selectOne('tbl_period',None,f"type={report[1]} and to_date="+f"'{arg['date']}'")
                    postgres_insert_query = f"select * from sma_stat_dep.tbl_schedule WHERE bank_id={bank_id} AND period_id={period_id} AND report_type_id={report[0]}"
                    cursor.execute(postgres_insert_query)
                    schedule_records = cursor.fetchone()
                    to_date = selectOne('tbl_period',None,f"type={report[1]} and to_date="+f"'{arg['date']}'",'to_date')[0]
                    # print(schedule_records)
                    if(datetime.today().date()>to_date+timedelta(days=schedule_records[5])):
                        status = 5
                        logs["context"]="Срок сдачи отчета истек, обратитесь к контактному лицу НБТ по данному отчету"    
                        logs["count"]+=1
                        log_to_text = f"Дата и время получения файла -------------------- {logs['upload_date']}\n\n {logs['context']}\n количество найденных ошибок --------------------{logs['count']}" 
                        postgres_insert_query = f"""UPDATE sma_stat_dep.tbl_files SET logs='{log_to_text}', upload_status='{status}' WHERE id='{file_id}';"""
                        cursor.execute(postgres_insert_query) 
                        connection.commit()
                        postgres_insert_query1 = f"""UPDATE sma_stat_dep.tbl_file_upload SET upload_status='{status}' WHERE id='{file_upload_id}';"""
                        cursor.execute(postgres_insert_query1) 
                        connection.commit()
                    elif(to_date>datetime.today().date()):
                        status = 5
                        logs["context"]+="Предоставление отчета невозможно, отчетный перид еще не закрыт"    
                        logs["count"]+=1
                        log_to_text = f"Дата и время получения файла -------------------- {logs['upload_date']}\n\n {logs['context']}\n количество найденных ошибок --------------------{logs['count']}" 
                        postgres_insert_query = f"""UPDATE sma_stat_dep.tbl_files SET logs='{log_to_text}', upload_status='{status}' WHERE id='{file_id}';"""
                        cursor.execute(postgres_insert_query) 
                        connection.commit()
                        postgres_insert_query1 = f"""UPDATE sma_stat_dep.tbl_file_upload SET upload_status='{status}' WHERE id='{file_upload_id}';"""
                        cursor.execute(postgres_insert_query1) 
                        connection.commit()
                    elif(schedule_records!=None): 
                        workbook_xml = BytesIO(bytes(mobile_records[4])) 
                        workbook_xml.seek(0)
                        print(openpyxl.load_workbook(workbook_xml))
                        wb = openpyxl.load_workbook(workbook_xml)
                        # print(wb['БД'].protection)
                        wb.save(mobile_records[3]) 

                        # print(data[1][20:40],data[7][20:40])
                        postgres_insert_query = f"""UPDATE sma_stat_dep.tbl_files SET upload_status='1' WHERE id='{file_id}';"""
                        cursor.execute(postgres_insert_query) 
                        connection.commit() 
                        try:
                            # print(arg) 
                            
                            # print(data)
                            report_type = f"SELECT validation_config FROM sma_stat_dep.tbl_report_type WHERE code='{arg['name']}'" 
                            cursor.execute(report_type)  
                            cnf = json.loads(cursor.fetchone()[0])['tables']
                            configList = list(map(lambda x: x['sheet_name'], cnf))
                            mass = {"extra-sheets":[],"missing sheets":[]}
                            # print(wb.sheetnames)
                            for el in wb.sheetnames:
                                res = False
                                for e in configList:
                                    if(el==e):
                                        res = True
                                if(res==False):
                                     mass["extra-sheets"].append(el)

                            for el in configList:
                                res = False
                                for e in wb.sheetnames:
                                    if(el==e):
                                        res = True
                                if(res==False):
                                     mass["missing sheets"].append(el)
                            print(wb.sheetnames)
                            print(configList)
                            if collections.Counter(wb.sheetnames) == collections.Counter(configList):
                                for table in cnf:
                                    # if not have nodes
                                    if('nodes' in table):
                                        logs["context"]+=f"Проверка и валидация ------------------------------------- листа {table['sheet_name']} \n"
                                        # data = pd.read_excel(mobile_records[3],index_col=None,header=None,sheet_name=table['sheet_name'], nrows=140, keep_default_na=False)
                                        data = pd.read_excel(mobile_records[3],index_col=None,header=None,sheet_name=table['sheet_name'], keep_default_na=False)
                                        # print ("Списки l1 и l2 одинаковые")
                                        cnfgMass = {}
                                        for i in table['nodes']: 
                                            cell = findeCEll(i['cell_address'])
                                            cnfgMass[f'{cell[0]},{cell[1]}'] = i 
                                            
                                        # print(cnfgMass.keys()) 
                                        for k in data: 
                                            for i in range(len(data[k])):
                                                # print(f'{k},{i}')
                                                if (f'{k},{i}') in cnfgMass.keys():
                                                    excelCell = data[k][i]
                                                    configCell = cnfgMass[f'{k},{i}'] 
                                                    if(findeCEll(configCell['cell_address'])[0]!=k or findeCEll(configCell['cell_address'])[1]!=i):
                                                        print('error')
                                                        # print(configCell['cell_address'])
                                                    # print(excelCell,'---------------',configCell['cell_address'],'---',configCell)
                                                    # print(data[k][i])
                                                    # if(configCell['cell_address']=='F1634'):
                                                    #      print(excelCell)
                                                    #      print(type(excelCell))
                                                    #      print(len(str(excelCell)))

                                                    if((str(excelCell)!='nan' and type(excelCell)==str) and (configCell["is_empty_allowed"]==False and len(excelCell)==0)):  
                                                        logs["context"]+=f"{configCell['cell_address']}--------{configs['errors']['0003']['ru']}\n"
                                                        logs["count"]+=1
                                                        continue
                                                    
                                                    if(types[configCell["data_type"]]!=type(excelCell) and (str(excelCell)=='nan' and type(excelCell)!=str)): 
                                                        logs["context"]+=f"{configCell['cell_address']}--------{configs['errors']['0001']['ru']}\n"
                                                        logs["count"]+=1
                                                        continue

                                                    # print(configCell['cell_address'],'----',types[configCell["data_type"]],type(excelCell),types[configCell["data_type"]]!=type(excelCell),excelCell!='',(str(excelCell)!='nan'),'-----',excelCell)    
                                                    if(types[configCell["data_type"]]!=type(excelCell) and excelCell!='' and (str(excelCell)!='nan')):
                                                        try:
                                                            type(int(excelCell)) 
                                                        except:  
                                                            logs["context"]+=f"{configCell['cell_address']}--------{configs['errors']['0002']['ru']}\n"
                                                            logs["count"]+=1
                                                            continue
                                                    
                                                    if(len(str(excelCell))>configCell["length"]): 
                                                        logs["context"]+=f"{configCell['cell_address']}--------{configs['errors']['0005']['ru']}\n"
                                                        logs["count"]+=1
                                                        print(f"{configCell['cell_address']}--------{configs['errors']['0005']['ru']}\n")
                                                        continue
                                                    if type(excelCell)==int:
                                                        if('is_negative_allowed' in configCell and (excelCell<0)!=configCell['is_negative_allowed']): 
                                                            logs["context"]+=f"{configCell['cell_address']}--------{configs['errors']['0004']['ru']}\n"
                                                            logs["count"]+=1
                                                            continue
                                        logs["context"]+=f"количество найденных ошибок на листе ---------------------------{logs['count']}\n\n"
                                    else:
                                        print('note nodes')
                                    
                                    if(logs['count']>0):
                                        status = 5
                                    else:
                                        status = 3  
                            else:
                                try:
                                    arg2/123
                                except (Exception, psycopg2.Error) as error:     
                                    # print ("Списки l1 и l2 неодинаковые")
                                    print("Error ", error,traceback.print_exc()) 
                                    status = 5
                                    logs["context"]+="Листы и их наименование в данном отчете не соответствует стандарту \n "    
                                    if(len(mass["extra-sheets"])>0):
                                         logs["count"]+=1
                                         text =''
                                         for el in mass["extra-sheets"]:
                                              text+= f'{el}, '
                                         logs["context"]+=f"в файле присуствуют лишние листы {text}\n "
                                    if(len(mass["missing sheets"])>0):
                                        logs["count"]+=1
                                        text =''
                                        for el in mass["missing sheets"]:
                                              text+= f'{el}, '
                                        logs["context"]+=f"в файле не хватает обязательных листов {text}\n"
                                    print(logs)
                                    logs["count"]+=1
                                    log_to_text = f"Дата и время получения файла -------------------- {logs['upload_date']}\n\n {logs['context']}\n количество найденных ошибок ---------------------------{logs['count']}" 
                                    postgres_insert_query = f"""UPDATE sma_stat_dep.tbl_files SET logs='{log_to_text}', upload_status='{status}' WHERE id='{file_id}';"""
                                    cursor.execute(postgres_insert_query) 
                                    connection.commit()
                                    postgres_insert_query1 = f"""UPDATE sma_stat_dep.tbl_file_upload SET upload_status='{status}' WHERE id='{file_upload_id}';"""
                                    cursor.execute(postgres_insert_query1) 
                                    connection.commit()
                                
                        except (Exception, psycopg2.Error) as error:
                            print("Error while fetching data from PostgreSQL1", error,traceback.print_exc()) 
                            status = 5
                            logs["context"]+="Не найденна конфигурация по данному отчету"    
                            logs["count"]+=1
                            log_to_text = f"Дата и время получения файла -------------------- {logs['upload_date']}\n\n {logs['context']}\n количество найденных ошибок ---------------------------{logs['count']}" 
                            postgres_insert_query = f"""UPDATE sma_stat_dep.tbl_files SET logs='{log_to_text}', upload_status='{status}' WHERE id='{file_id}';"""
                            cursor.execute(postgres_insert_query) 
                            connection.commit()
                            postgres_insert_query1 = f"""UPDATE sma_stat_dep.tbl_file_upload SET upload_status='{status}' WHERE id='{file_upload_id}';"""
                            cursor.execute(postgres_insert_query1) 
                            connection.commit()

                        f = open("logs.txt", "w")
                        log_to_text = f"Дата и время получения файла -------------------- {logs['upload_date']}\n\n {logs['context']}\n количеству найденных ошибок ---------------------------{logs['count']}"
                        f.write(log_to_text)
                        f.close() 

                        postgres_insert_query = f"""UPDATE sma_stat_dep.tbl_files SET logs='{log_to_text}', upload_status='{status}' WHERE id='{file_id}';"""
                        cursor.execute(postgres_insert_query) 
                        connection.commit()

                        postgres_insert_query1 = f"""UPDATE sma_stat_dep.tbl_file_upload SET upload_status='{status}' WHERE id='{file_upload_id}';"""
                        cursor.execute(postgres_insert_query1) 
                        connection.commit()
                    else:
                        status = 5
                        logs["context"]+="Для получения данного отчета не существует расписание"    
                        logs["count"]+=1
                        log_to_text = f"Дата и время получения файла -------------------- {logs['upload_date']}\n\n {logs['context']}\n количество найденных ошибок ---------------------------{logs['count']}" 
                        postgres_insert_query = f"""UPDATE sma_stat_dep.tbl_files SET logs='{log_to_text}', upload_status='{status}' WHERE id='{file_id}';"""
                        cursor.execute(postgres_insert_query) 
                        connection.commit()
                        postgres_insert_query1 = f"""UPDATE sma_stat_dep.tbl_file_upload SET upload_status='{status}' WHERE id='{file_upload_id}';"""
                        cursor.execute(postgres_insert_query1) 
                        connection.commit()
                except (Exception, psycopg2.Error) as error:
                    print("Error while fetching data from PostgreSQL", error,traceback.print_exc())
                    status = 5
                    logs["context"]+="Для получения данного отчета не существует расписание"    
                    logs["count"]+=1
                    log_to_text = f"Дата и время получения файла -------------------- {logs['upload_date']}\n\n {logs['context']}\n количество найденных ошибок ---------------------------{logs['count']}" 
                    postgres_insert_query = f"""UPDATE sma_stat_dep.tbl_files SET logs='{log_to_text}', upload_status='{status}' WHERE id='{file_id}';"""
                    cursor.execute(postgres_insert_query) 
                    connection.commit()
                    postgres_insert_query1 = f"""UPDATE sma_stat_dep.tbl_file_upload SET upload_status='{status}' WHERE id='{file_upload_id}';"""
                    cursor.execute(postgres_insert_query1) 
                    connection.commit()

            except (Exception, psycopg2.Error) as error:
                print("Error while fetching data from PostgreSQL", error,traceback.print_exc())
    try:
        postgres_insert_query = f"select * from sma_stat_dep.tbl_files WHERE upload_status=1"
        # postgres_insert_query = f"select * from sma_stat_dep.tbl_file_upload WHERE id=32"
        cursor.execute(postgres_insert_query)
        mobile_records = cursor.fetchone()
        print('prinnnnnnnnnnnnnnnnt: ',mobile_records)
        # print(mobile_records[0])
        select_datas(mobile_records[1]) 
    except (Exception, psycopg2.Error) as error:
            print("Error while fetching data from PostgreSQL", error,traceback.print_exc())
    finally:
            if connection:
                cursor.close()
                connection.close()
                print("PostgreSQL connection is closed")


# def get_plugin():
#     from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator
#     print(f"clickhouseOperator!!!!!!! with version: {ClickHouseOperator.__class__}")

