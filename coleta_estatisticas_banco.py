#!/usr/bin/env python2.7
## -*- coding: utf-8 -*-




# INFO
__name__       = "coleta_estatisticas"
__author__     = "Gabriel Magro dos Santos"
__maintainer__ = "Gabriel Magro dos Santos"
__email__      = "gamadsantos@timbrasil.com.br"


print("## Início da Execucao ##")


# IMPORT SISTEMA
import argparse, cx_Oracle, json, os, sys
from datetime import datetime

# IMPORT BIBLIOTECAS
sys.path.append(os.path.dirname(os.path.realpath(__file__))+'/../..')
from lib.environment import config
from lib import log

# Parâmetros Gerais
DIR_LOG    = config.get("path", "database") + 'DBA/log/'
recolor = {'green':'\033[92m %s \033[0m', 'red':'\033[91m %s \033[0m'}
staleTabs_list = []
queriesList = []
OK = "[OK]"
OK = recolor['green'] % OK
FALHA = "[FALHA]"
FALHA = recolor['red'] % FALHA


#PARSE ARGS
coletor = argparse.ArgumentParser(description='LOAD SCRIPT')
coletor.add_argument('--config', required=True, help='Voce deve fornecer config em json')
args = coletor.parse_args()
v_config_json_string = args.config.upper()
try:
    JSON_DATA = json.loads(v_config_json_string) #v_config_json_string = '{"NUMLOG":$log, "SCHEMA":"DT", "OBJTYPE":"TABLE", "QTD":"TIM", "DATA":"$data", "DEGREE":"12"}' EXEMPLO DE ENTRADA
except:
    outMsg = '[2|0|ABORTOU|CONFIG JSON com Problemas]'
    log.GeraLog(DIR_LOG, 'COLETA_STATS', 9999999, 'Vazio', 0, 'DBA', outMsg)
    log.GeraLog(DIR_LOG, 'COLETA_STATS', 9999999, 'Vazio', 'fim', 'DBA', outMsg)
    print(outMsg)
    sys.exit(1)
##

#NUMLOG
try:
    NUMLOG = JSON_DATA['NUMLOG']
except:
    log.GeraLog(DIR_LOG, 'COLETA_STATS', 9999999, 'Vazio', 'inicio', 'DBA', '')
    outMsg = 'Replicacao iniciada sem número de log'
    log.GeraLog(DIR_LOG, 'COLETA_STATS', 9999999, 'Vazio', 0, 'DBA', outMsg)
    outMsg = '[2|0|ABORTOU|Replicacao iniciada sem número de log]'
    log.GeraLog(DIR_LOG, 'COLETA_STATS', 9999999, 'Vazio', 0, 'DBA', outMsg)
    log.GeraLog(DIR_LOG, 'COLETA_STATS', 9999999, 'Vazio', 'fim', 'DBA', outMsg)
    print(outMsg)
    sys.exit(1)
##

#TABLE
try:
    TABLE = JSON_DATA['TABLE']
except:
    TABLE = False
    pass

#OBJTYPE
try:
    OBJTYPE = JSON_DATA['OBJTYPE']
except:
    outMsg = '[2|0|ABORTOU|Tipo de Objeto nao informado]'
    log.GeraLog(DIR_LOG, 'COLETA_STATS', NUMLOG, 'Vazio', 0, 'DBA', outMsg)
    log.GeraLog(DIR_LOG, 'COLETA_STATS', NUMLOG, 'Vazio', 'fim', 'DBA', outMsg)
    print(outMsg)
    sys.exit(1)

#PARTITION
try:
    PARTITION = JSON_DATA.get('PARTITION', None)
except:
    PARTITION = False 
    pass

#SUBPARTITION
try:
    SUBPARTITION = JSON_DATA.get('SUBPARTITION', None)
except:
    SUBPARTITION = False 
    pass

#SCHEMA
try:
    SCHEMA = JSON_DATA['SCHEMA']
except:
    SCHEMA = False
    pass

#ROWS
try:
    ROWS = JSON_DATA['ROWS']
except:
    ROWS = '10'

#DEGREE
try:
    DEGREE = JSON_DATA.get('DEGREE', None)
except:
    DEGREE = False
    pass


##Criando dicionário com os parâmetros recebidos (params)
params = {}

if SCHEMA:
        params['schema'] = SCHEMA
if OBJTYPE:
        params['objtype'] = OBJTYPE
if TABLE:
        params['table'] = TABLE
if PARTITION:
        params['partition'] = PARTITION
if SUBPARTITION:
        params['subpartition'] = SUBPARTITION
if DEGREE:
        params['degree'] = DEGREE
if ROWS:
        params['rows'] = ROWS


def conectaQDE():
    v_makedsn = {'host':config.get('qde', 'db_host_1'),'port':config.get('qde', 'db_port_1'),'service_name':config.get('qde', 'db_service_name')}
    v_dsn = cx_Oracle.makedsn(**v_makedsn)
    v_connectparameters = {'user':config.get('qde', 'username'),'password':config.get('qde', 'password'),'dsn':v_dsn,'threaded':True}
    try:
        conQDE = cx_Oracle.connect(**v_connectparameters)
        outMsg = 'Sucesso na conexao com QDE HOST 1'
        log.GeraLog(DIR_LOG, 'COLETA_STATS', NUMLOG, 'Vazio', 0, 'DBA', outMsg)
        return conQDE
    except:
        outMsg = 'Falha na conexao com QDE HOST 1'
        log.GeraLog(DIR_LOG, 'COLETA_STATS', NUMLOG, 'Vazio', 0, 'DBA', outMsg)
        v_makedsn = {'host':config.get('qde', 'db_host_2'),'port':config.get('qde', 'db_port_2'),'service_name':config.get('qde', 'db_service_name')}
        v_dsn = cx_Oracle.makedsn(**v_makedsn)
        v_connectparameters = {'user':config.get('qde', 'username'),'password':config.get('qde', 'password'),'dsn':v_dsn,'threaded':True}
        try:
            conQDE = cx_Oracle.connect(**v_connectparameters)
            outMsg = 'Sucesso na conexao com QDE HOST 2'
            log.GeraLog(DIR_LOG, 'COLETA_STATS', NUMLOG, 'Vazio', 0, 'DBA', outMsg)
            return conQDE
        except:
            outMsg = 'Falha Conexao Oracle QDE HOST 1 e HOST 2'
            log.GeraLog(DIR_LOG, 'COLETA_STATS', NUMLOG, 'Vazio', 0, 'DBA', outMsg)
            outMsg = '[2|0|ABORTOU|Falha Conexao Oracle QDE HOST 1 e HOST 2]'
            log.GeraLog(DIR_LOG, 'COLETA_STATS', NUMLOG, 'Vazio', 0, 'DBA', outMsg)
            log.GeraLog(DIR_LOG, 'COLETA_STATS', NUMLOG, 'Vazio', 'fim', 'DBA', outMsg)
            print(outMsg)
            sys.exit(1)

#variaveis de conexao
connection = conectaQDE()
cursor = connection.cursor()

def queryConstructor1(params):

    query = """
    SELECT * FROM (
    SELECT  tab1.OWNER, tab1.TABLE_NAME, tab1.PARTITION_NAME, tab1.SUBPARTITION_NAME, tab1.OBJECT_TYPE, tab1.STALE_STATS, tab1.LAST_ANALYZED, tab1.BLOCKS, tab1.NUM_ROWS, 
    CASE WHEN tab1.BLOCKS < 1000 and tab1.NUM_ROWS < 100000 THEN 1 ELSE 2 END RN
    FROM <schema>.<table> tab1
    inner join ALL_USERS tab2
    on tab1.OWNER = tab2.USERNAME
    where tab2.ORACLE_MAINTAINED = 'N'
    AND tab1.stale_stats = 'YES'
    """

    for key in params.keys():
        if key == 'table':
            tables = ','.join(["'%s'" % t for t in params[key].split(',')])
            query += " AND tab1.TABLE_NAME in upper(%s)" % tables     
        if key == 'schema':
            schemas = ','.join(["'%s'" % t for t in params[key].split(',')])
            query += " AND tab1.OWNER in upper(%s)" % schemas
        if key == 'partition':
            partitions = ','.join(["'%s'" % t for t in params[key].split(',')])
            query += " AND tab1.PARTITION_NAME in upper(%s)" % partitions
        if key == 'subpartition':
            subpartitions = ','.join(["'%s'" % t for t in params[key].split(',')])
            query += " AND tab1.SUBPARTITION_NAME in upper(%s)" % subpartitions
    
    query += """ AND tab1.OBJECT_TYPE = upper('%s')""" % params['objtype']

    query += """
    )
    ORDER BY OBJECT_TYPE DESC, RN, BLOCKS, NUM_ROWS, LAST_ANALYZED
    """
    
    query += """FETCH FIRST %s ROWS ONLY""" %params['rows']
    
    return query


def queryConstructor2(target_list):

    for items in target_list:
        if items['OBJECT_TYPE'] == 'TABLE' and not DEGREE:
            query = """
            BEGIN
            DBMS_STATS.GATHER_TABLE_STATS(
            ownname => '%s', 
            tabname => '%s');
            END;""" % (items['OWNER'], items['TABLE_NAME'])
            queriesList.append(query)

        if items['OBJECT_TYPE'] == 'TABLE' and DEGREE:
            query = """
            BEGIN
            DBMS_STATS.GATHER_TABLE_STATS(
            ownname => '%s', 
            tabname => '%s',
            degree => '%s' 
            );
            END;""" % (items['OWNER'], items['TABLE_NAME'], params['degree'])

            queriesList.append(query)

        if items['OBJECT_TYPE'] != 'TABLE' and not DEGREE:
            if items['OBJECT_TYPE'] == 'PARTITION':
                query = """
                BEGIN
                DBMS_STATS.GATHER_TABLE_STATS(
                ownname => '%s', 
                tabname => '%s',
                partname => '%s');
                END;""" % (items['OWNER'], items['TABLE_NAME'], items['PARTITION_NAME'])
                queriesList.append(query)
            if items['OBJECT_TYPE'] == 'SUBPARTITION':
                query = """
                BEGIN
                DBMS_STATS.GATHER_TABLE_STATS(
                ownname => '%s', 
                tabname => '%s',
                partname => '%s');
                END;""" % (items['OWNER'], items['TABLE_NAME'], items['SUBPARTITION_NAME'])
                queriesList.append(query)

        if items['OBJECT_TYPE'] != 'TABLE' and DEGREE:
            if items['OBJECT_TYPE'] == 'PARTITION':
                query = """
                BEGIN
                DBMS_STATS.GATHER_TABLE_STATS(
                ownname => '%s', 
                tabname => '%s',
                partname => '%s',
                degree => '%s' 
                );
                END;""" % (items['OWNER'], items['TABLE_NAME'], items['PARTITION_NAME'], params['degree'])
                queriesList.append(query)
            if items['OBJECT_TYPE'] == 'SUBPARTITION':
                query = """
                BEGIN
                DBMS_STATS.GATHER_TABLE_STATS(
                ownname => '%s', 
                tabname => '%s',
                partname => '%s',
                degree => '%s' 
                );
                END;""" % (items['OWNER'], items['TABLE_NAME'], items['SUBPARTITION_NAME'], params['degree'])
                queriesList.append(query)


def executeProcess():
    print('Início do processo de coleta das estatísticas do(s) elemento(s) abaixo:')

    cursor.execute(queryConstructor1(params))
    query_output = cursor.fetchall()

    for element in query_output:
        if element[4] == 'PARTITION':
            staleTabs_list.append({'OWNER': element[0], 
                'TABLE_NAME': element[1],
                'PARTITION_NAME': element[2],
                'SUBPARTITION_NAME' : None,
                'OBJECT_TYPE': element[4], 
                'LAST_ANALYZED': element[6], 
                'BLOCKS': element[7], 
                'NUM_ROWS': element[8]})
        if element[4] == 'SUBPARTITION':
            staleTabs_list.append({'OWNER': element[0], 
                'TABLE_NAME': element[1],
                'PARTITION_NAME': element[2],
                'SUBPARTITION_NAME': element[3],
                'OBJECT_TYPE': element[4], 
                'LAST_ANALYZED': element[6], 
                'BLOCKS': element[7], 
                'NUM_ROWS': element[8]})    
        if element[4] == 'TABLE':
            staleTabs_list.append({'OWNER': element[0], 
                'TABLE_NAME': element[1], 
                'PARTITION_NAME': None,
                'SUBPARTITION_NAME': None,
                'OBJECT_TYPE': element[4], 
                'LAST_ANALYZED': element[6], 
                'BLOCKS': element[7], 
                'NUM_ROWS': element[8]})
            

    for indice, item in enumerate(staleTabs_list, start = 1):
        if item['OBJECT_TYPE'] == 'TABLE':
            v_msg = 'Elemento %s: [Owner: %s, Table: %s, Blocks: %s, Num_Rows: %s]' % (indice, item['OWNER'], item['TABLE_NAME'], item['BLOCKS'], item['NUM_ROWS'])
            print(v_msg)
            log.GeraLog(DIR_LOG, 'COLETA_STATS', NUMLOG, 'Vazio', 0, 'DBA', v_msg)
            #time.sleep(0.1)
        if item['OBJECT_TYPE'] == 'PARTITION':
            v_msg = 'Elemento %s: [Owner: %s, Table: %s, Partition: %s, Blocks: %s, Num_Rows: %s]' % (indice, item['OWNER'], item['TABLE_NAME'], item['PARTITION_NAME'], item['BLOCKS'], item['NUM_ROWS'])
            print(v_msg)
            log.GeraLog(DIR_LOG, 'COLETA_STATS', NUMLOG, 'Vazio', 0, 'DBA', v_msg)
            #time.sleep(0.1)    
        if item['OBJECT_TYPE'] == 'SUBPARTITION':
            v_msg = 'Elemento %s: [Owner: %s, Table: %s, Partition: %s, Subpartition: %s, Blocks: %s, Num_Rows: %s]' % (indice, item['OWNER'], item['TABLE_NAME'], item['PARTITION_NAME'], item['SUBPARTITION_NAME'], item['BLOCKS'], item['NUM_ROWS'])
            print(v_msg)
            log.GeraLog(DIR_LOG, 'COLETA_STATS', NUMLOG, 'Vazio', 0, 'DBA', v_msg)
            #time.sleep(0.1)

    queryConstructor2(staleTabs_list)

    for indice, element in enumerate(queriesList):
        owner = staleTabs_list[indice]['OWNER']
        table_name = staleTabs_list[indice]['TABLE_NAME']
        partition_name = staleTabs_list[indice]['PARTITION_NAME'] if staleTabs_list[indice]['PARTITION_NAME'] else """Null"""
        subpartition_name = staleTabs_list[indice]['SUBPARTITION_NAME'] if staleTabs_list[indice]['SUBPARTITION_NAME'] else """Null"""
        last_analyzed = staleTabs_list[indice]['LAST_ANALYZED']
        object_type = staleTabs_list[indice]['OBJECT_TYPE']
        datinicolest = datetime.now().strftime('%d/%m/%Y %H:%M:%S')
        try:
            print('Coleta em andamento...')
            cursor.execute(element)
            datfimcolest = datetime.now().strftime('%d/%m/%Y %H:%M:%S')
            print("""Elemento %s: Status %s || Hora de início: %s, Hora de término: %s""" % (indice + 1, OK, datinicolest, datfimcolest))
        except cx_Oracle.DatabaseError as e:
            print('Elemento %s: Status %s' % (indice + 1, FALHA))
            print('Erro: ' + str(e))
            print('O processo será encerrado agora.')
            sys.exit(1)
            

        insert_query = """INSERT INTO <schema>.<table> (
        OWNER,
        TABLE_NAME,
        PARTITION_NAME, 
        SUBPARTITION_NAME, 
        OBJECT_TYPE, 
        LAST_ANALYZED, 
        DATINICOLEST, 
        DATFIMCOLEST,
        NUMLOG
        ) 
        VALUES ("""

        if object_type == 'TABLE':
            insert_query += """
            '%s', 
            '%s', 
            %s, 
            %s, 
            '%s', 
            TO_DATE('%s', 'YYYY-MM-DD HH24:MI:SS'), 
            TO_DATE('%s', 'DD/MM/YYYY HH24:MI:SS'), 
            TO_DATE('%s', 'DD/MM/YYYY HH24:MI:SS'),
            %s
            )
            """ % (owner, table_name, partition_name, subpartition_name, object_type, last_analyzed, datinicolest, datfimcolest, NUMLOG)
            """print(insert_query)"""

        if object_type == 'PARTITION':
            insert_query += """
            '%s', 
            '%s', 
            '%s', 
            %s, 
            '%s', 
            TO_DATE('%s', 'YYYY-MM-DD HH24:MI:SS'), 
            TO_DATE('%s', 'DD/MM/YYYY HH24:MI:SS'), 
            TO_DATE('%s', 'DD/MM/YYYY HH24:MI:SS'),
            %s
            )
            """ % (owner, table_name, partition_name, subpartition_name, object_type, last_analyzed, datinicolest, datfimcolest, NUMLOG)
            """print(insert_query)"""

        if object_type == 'SUBPARTITION':
            insert_query += """
            '%s', 
            '%s', 
            '%s', 
            '%s', 
            '%s', 
            TO_DATE('%s', 'YYYY-MM-DD HH24:MI:SS'), 
            TO_DATE('%s', 'DD/MM/YYYY HH24:MI:SS'), 
            TO_DATE('%s', 'DD/MM/YYYY HH24:MI:SS'),
            %s
            )
            """ % (owner, table_name, partition_name, subpartition_name, object_type, last_analyzed, datinicolest, datfimcolest, NUMLOG)
        try:            
            cursor.execute(insert_query)
            connection.commit()
        except:
            outMsg = 'Falha na inserção da coleta na tabela de controle.'
            outMsg = recolor['red'] % outMsg
            print(outMsg)
            outMsg = '[1|0|ATENCAO|Falha na inserção da coleta na tabela de controle]'
            log.GeraLog(DIR_LOG, 'COLETA_STATS', NUMLOG, 'Vazio', 0, 'DBA', outMsg)
            outMsg = insert_query
            print(outMsg)
            log.GeraLog(DIR_LOG, 'COLETA_STATS', NUMLOG, 'Vazio', 0, 'DBA', outMsg)
            log.GeraLog(DIR_LOG, 'COLETA_STATS', NUMLOG, 'Vazio', 'fim', 'DBA', outMsg)
            print(outMsg)

executeProcess()

outMsg = '[0|0|SUCESSO|Processo executado com sucesso]'
log.GeraLog(DIR_LOG, 'COLETA_STATS', NUMLOG, 'Vazio', 0, 'DBA', outMsg)
log.GeraLog(DIR_LOG, 'COLETA_STATS', NUMLOG, 'Vazio', 'fim', 'DBA', outMsg)
print(outMsg)
sys.exit(0)
