# coding: utf-8


import pandas as pd
import cx_Oracle
from sqlalchemy import create_engine
import time
import numpy as np
import os
os.environ['NLS_LANG'] = 'AMERICAN_AMERICA.AL32UTF8'

class useOracle():
    
    def __init__(self,user,password,database):
        self.user = user
        self.password = password
        self.database = database

    def createCopyTable(self,comm,account):
        connection = cx_Oracle.connect(account)
        cursor = connection.cursor()
        cursor.execute(comm)
        cursor.close()
        connection.close()
        return
        
    def getData(self,commit,account):
        connection = cx_Oracle.connect(account)
        cursor = connection.cursor()
        cursor.execute(commit)
        x = cursor.description
        columns = [y[0] for y in x]
        cursor01 = cursor.fetchall()
        cursor.close()
        data = pd.DataFrame(cursor01,columns = columns)
        return data

    
    def getDataThrogPand(self,address,targetTable):
        engine = create_engine(address)
        result = pd.read_sql('select * from {}'.format(targetTable),engine)
#         engine.close()
        return result
    
    def copyTableStructure(self,newTable,targetTable):
        connection = cx_Oracle.connect(self.user, self.password, self.database)
        cursor = connection.cursor()
        cursor.execute("""
                       declare  
                        i integer;  
                        begin  
                        select count(*) into i from user_tables where table_name = '{}';  
                        if i > 0 then  
                        dbms_output.put_line('该表已存在!'); 
                        else  
                        dbms_output.put_line('该表不存在');  
                        execute immediate 'create table {} as select * from {} where 1=2'; 
                        end if;  
                        end;                          
                       """.format(newTable,newTable,targetTable))
#                                 execute immediate 'DROP TABLE {}'; 
        data = self.getData(newTable)
        dHigh = data.shape[0]
        if dHigh == 0:
            print("表格结构已存在，但表格内为空")
            print("表格结构形式为：",data.columns)
        else:
            print("表格已存在，且有数据")
            print("表格大小为：",data.shape)
        print("要删除表格，请执行dropTable命令。")
        cursor.close()
        connection.close()
        return

    def copyTable(self,newTable,targetTable):
        connection = cx_Oracle.connect(self.user, self.password, self.database)
        cursor = connection.cursor()
        cursor.execute("""create table {} as select * from {}""".format(newTable,targetTable))
        cursor.close()
        connection.close()
        return
    
    def insertDataToTable(self,data,dataTable,account):
        connection = cx_Oracle.connect(account)
        cursor = connection.cursor()
        query = "INSERT INTO "+ dataTable + " ({}) VALUES ({})"

        if type(data) == pd.core.frame.DataFrame:
            columns = list(data.columns)
            aname = ','.join(columns)
            dtHigh = data.shape[0]
            dtWidth = data.shape[1]
            creatVar = locals()
            for i in range(dtHigh):
                value_list = []
                for j in range(dtWidth):
                    value_list.append("'{}'".format(data.iloc[i,j]))
                values = ','.join(value_list)
                try:
                    commit=query.format(aname,values)
                    cursor.execute(commit)
                    # print(i)
                except Exception as e:
                    print(e)
        elif type(data)== list:
            dtWidth = len(data[0].split('  '))
            dtHigh = len(data)
            creatVar = locals()
            aname = [':'+str(i+1) for i in range(dtWidth)]
            aname = ','.join(aname)
            for i in range(dtHigh):
                value_list = []
                text = data[i].split('  ')
                for j in range(dtWidth):
                    value_list.append("'{}'".format(str(text[j])))
                values = ','.join(value_list)
                cursor.execute(query.format(aname,values))
        connection.commit()
        cursor.close()
        connection.close()
        return

    #单条上传有时间类型的数据
    def insertdateDataToTable(self,data,dataTable,account):
        connection = cx_Oracle.connect(account)
        cursor = connection.cursor()
        query = "INSERT INTO "+ dataTable + " ({}) VALUES ({})"

        if type(data) == pd.core.frame.DataFrame:
            columns = list(data.columns)
            aname = ','.join(columns)
            dtHigh = data.shape[0]
            dtWidth = data.shape[1]
            creatVar = locals()
            for i in range(dtHigh):
                value_list = []
                for j in range(dtWidth):
                    if j==3:
                        data1="to_date('{}','YYYY-MM-DD HH24:MI:SS')".format(data.iloc[i, j])
                        value_list.append(data1)
                    # elif j==9:
                    #     value_list.append(float(data.iloc[i, j]))
                    else:
                        value_list.append("'{}'".format(data.iloc[i, j]))
                #             values = ','.join(value_list)
                values = ','.join(value_list)
                try:
                    commit=query.format(aname,values)
                    cursor.execute(commit)
                    # print(i)
                except Exception as e:
                    print(e)
        elif type(data)== list:
            dtWidth = len(data[0].split('  '))
            dtHigh = len(data)
            creatVar = locals()
            aname = [':'+str(i+1) for i in range(dtWidth)]
            aname = ','.join(aname)
            for i in range(dtHigh):
                value_list = []
                text = data[i].split('  ')
                for j in range(dtWidth):
                    value_list.append("'{}'".format(str(text[j])))
                values = ','.join(value_list)
                cursor.execute(query.format(aname,values))
        connection.commit()
        cursor.close()
        connection.close()
        return
    
    def BatchinsertDataToTable(self,data,dataTable,account):
        connection = cx_Oracle.connect(account)
        cursor = connection.cursor()
        query = "INSERT INTO "+ dataTable + " VALUES ({})"
        creatVar = locals()
        wholeData = []
        if type(data) == pd.core.frame.DataFrame:
            columns = list(data.columns)
            aidx = list(range(1,len(columns)+1))
            aidx = [':'+str(i) for i in aidx]
            aname = ','.join(aidx)
    #         print(aname)
            dtHigh = data.shape[0]
            dtWidth = data.shape[1]
            for i in range(dtHigh):
                value_list = []
                for j in range(dtWidth):
                    value_list.append("{}".format(str(data.iloc[i,j])))
    #             values = ','.join(value_list)
                wholeData.append(value_list)
            for ii in range(1, len(wholeData) // 1000 + 2):
                begin = (ii - 1) * 1000
                end = ii * 1000
                common=query.format(aname)
                batch_data=wholeData[begin:end]
                try:
                    cursor.executemany(common, batch_data)
                except:
                    print(batch_data)
        elif type(data)== list:
            a = data[0]
#             print(a)
            b = a.split('  ')
#             print(b)
            dtWidth = len(b)
            dtHigh = len(data)
            creatVar = locals()
            aname = [':'+str(i+1) for i in range(dtWidth)]
            aname = ','.join(aname)
            for i in range(dtHigh):
                value_list = []
                text = data[i].split('  ')
                for j in range(dtWidth):
                    value_list.append("{}".format(str(text[j])))
                wholeData.append(value_list)
            for ii in range(1, len(wholeData) // 100 + 2):
                begin = (ii - 1) * 100
                end = ii * 100
                cursor.executemany(query.format(aname), wholeData[begin:end])
#         print(aname,wholeData)
#         cursor.executemany(query.format(aname),wholeData)
        connection.commit()
        cursor.close()
        connection.close()
        return

    #批次上传有时间类型的数据
    def BatchdateinsertDataToTable(self, data, dataTable, account):
        connection = cx_Oracle.connect(account)
        cursor = connection.cursor()
        cursor.execute("select * from  {}".format(dataTable))
        x = cursor.description
        columns = [y[0] for y in x]
        for r in columns:
            if r == 'VERIFYFINALDATE':
                columns.remove('VERIFYFINALDATE')
        columns_name = ','.join(columns) + ',{}'.format('VERIFYFINALDATE')
        columns_num = ',:'.join(
            [str(r) for r in range(1, len(columns) + 1)]) + ",to_date(:{},'yyyy/mm/dd hh24:mi:ss')".format(
            str(len(columns) + 1))
        columns.append('VERIFYFINALDATE')
        # data_input_list = []
        wholeData=[]
        if type(data) == pd.core.frame.DataFrame:
            # columns = list(data.columns)
            # aidx = list(range(1, len(columns) + 1))
            # aidx = [':' + str(i) for i in aidx]
            # aname = ','.join(aidx)
            dtHigh = data.shape[0]
            dtWidth = data.shape[1]
            for i in range(dtHigh):
                value_list = []
                for j in range(dtWidth):
                    value_list.append("{}".format(str(data.iloc[i, j])))
                #             values = ','.join(value_list)
                wholeData.append(value_list)
            for ii in range(1, len(wholeData) // 100 + 2):
                begin = (ii - 1) * 100
                end = ii * 100
                batch_data = wholeData[begin:end]
                commit="insert into {} ({}) values(:{})".format(dataTable, columns_name, columns_num)
                cursor.prepare(commit)
                cursor.executemany(None, batch_data)
        connection.commit()
        connection.close()
        return

    #批次传入时间类型数据
    def BatchsysteminsertDataToTable(self, data, dataTable, account):
        connection = cx_Oracle.connect(account)
        cursor = connection.cursor()
        query = "INSERT INTO " + dataTable + " VALUES ({})"
        creatVar = locals()
        wholeData = []
        if type(data) == pd.core.frame.DataFrame:
            columns = list(data.columns)
            aidx = list(range(1, len(columns)))
            aname = ':'+',:'.join([str(r) for r in range(1, len(columns))]) + ",to_date(:{},'yyyy/mm/dd hh24:mi:ss')".format(str(len(columns)))
            dtHigh = data.shape[0]
            dtWidth = data.shape[1]
            for i in range(dtHigh):
                value_list = []
                for j in range(dtWidth):
                        value_list.append("{}".format(data.iloc[i, j]))
                #             values = ','.join(value_list)
                wholeData.append(value_list)
            for ii in range(1, len(wholeData) // 1000 + 2):
                begin = (ii - 1) * 1000
                end = ii * 1000
                common = query.format(aname)
                batch_data = wholeData[begin:end]
                cursor.executemany(common, batch_data)

        elif type(data) == list:
            a = data[0]
            #             print(a)
            b = a.split('  ')
            #             print(b)
            dtWidth = len(b)
            dtHigh = len(data)
            creatVar = locals()
            aname = [':' + str(i + 1) for i in range(dtWidth)]
            aname = ','.join(aname)
            for i in range(dtHigh):
                value_list = []
                text = data[i].split('  ')
                for j in range(dtWidth):
                    value_list.append("{}".format(str(text[j])))
                wholeData.append(value_list)
            for ii in range(1, len(wholeData) // 100 + 2):
                begin = (ii - 1) * 100
                end = ii * 100
                cursor.executemany(query.format(aname), wholeData[begin:end])
        #         print(aname,wholeData)
        #         cursor.executemany(query.format(aname),wholeData)
        connection.commit()
        cursor.close()
        connection.close()
        return

    def Batch_gs_insertDataToTable(self, data, dataTable, account):
        connection = cx_Oracle.connect(account)
        cursor = connection.cursor()
        query = "INSERT INTO " + dataTable + " VALUES ({})"
        creatVar = locals()
        wholeData = []
        if type(data) == pd.core.frame.DataFrame:
            columns = list(data.columns)
            # aidx = list(range(1, len(columns)))
            aname = ':'+',:'.join([str(r) for r in range(1, len(columns)-3)]) + ",to_date(:{},'yyyy/mm/dd hh24:mi:ss')".format(str(len(columns)-3))\
                    +',:'+',:'.join([str(r) for r in range(16, len(columns))])+",to_date(:{},'yyyy/mm/dd hh24:mi:ss')".format(str(len(columns))) \
            # dtHigh = data.shape[0]
            # dtWidth = data.shape[1]
            wholeData=data.values.tolist()
            # for i in range(dtHigh):
            #     value_list = []
            #     for j in range(dtWidth):
            #             value_list.append("{}".format(data.iloc[i, j]))
            #     wholeData.append(value_list)
            for ii in range(1, len(wholeData) // 10000 + 2):
                begin = (ii - 1) * 10000
                end = ii * 10000
                common = query.format(aname)
                batch_data = wholeData[begin:end]
                cursor.executemany(common, batch_data)

        elif type(data) == list:
            a = data[0]
            #             print(a)
            b = a.split('  ')
            #             print(b)
            dtWidth = len(b)
            dtHigh = len(data)
            creatVar = locals()
            aname = [':' + str(i + 1) for i in range(dtWidth)]
            aname = ','.join(aname)
            for i in range(dtHigh):
                value_list = []
                text = data[i].split('  ')
                for j in range(dtWidth):
                    value_list.append("{}".format(str(text[j])))
                wholeData.append(value_list)
            for ii in range(1, len(wholeData) // 100 + 2):
                begin = (ii - 1) * 100
                end = ii * 100
                cursor.executemany(query.format(aname), wholeData[begin:end])
        #         print(aname,wholeData)
        #         cursor.executemany(query.format(aname),wholeData)
        connection.commit()
        cursor.close()
        connection.close()
        return

    def dropTable(self,dropTable):
        connection = cx_Oracle.connect(self.user, self.password, self.database)
        cursor = connection.cursor()
        cursor.execute("drop table {}".format(dropTable))
        cursor.close()
        connection.close()
        return

    def truncateTable(self,truncateTable,account):
        connection = cx_Oracle.connect(account)
        cursor = connection.cursor()
        cursor.execute("truncate table {}".format(truncateTable))
        cursor.close()
        connection.close()
        return
      
    def fbPreView(self,table,time):
        connection = cx_Oracle.connect(self.user, self.password, self.database)
        cursor = connection.cursor()
#         print("select * from {} as of timestamp to_timestamp('{}', 'yyyy-mm-dd hh24:mi:ss')".format(table,time))
        cursor.execute("select * from {} as of timestamp to_timestamp('{}', 'yyyy-mm-dd hh24:mi:ss')".format(table,time))
        x = cursor.description
        columns = [y[0] for y in x]
        cursor01 = cursor.fetchall()
        data = pd.DataFrame(cursor01,columns = columns)
        cursor.close()
        connection.close()
        return data
    
    def flashBack(self,table,time):
        connection = cx_Oracle.connect(self.user, self.password, self.database)
        cursor = connection.cursor()
        cursor.execute("alter table {} enable row movement".format(table))
        cursor.execute("flashback table {} to timestamp to_timestamp('{}','yyyy-mm-dd hh24:mi:ss')".format(table,time))
        cursor.close()
        return
    
    def creatDataFrame(self,df,tableName,account):
        connection = cx_Oracle.connect(account)
        cursor = connection.cursor()
        columns = df.columns
        character_types = ['NVARCHAR2(400)'] * len(columns)
        jihe = ','.join([k + ' ' + v for k, v in (list(zip(columns, character_types)))])
        commit="create table {}({})".format(tableName,jihe)
        cursor.execute(commit)
        cursor.close()
        connection.close()
        return

    def  executeCommit(self,commit,account):
        connection = cx_Oracle.connect(account)
        cursor = connection.cursor()
        cursor.execute(commit)
        cursor.close()
        connection.close()
        return
    def  executeCommitSubmit(self,commit,account):
        connection = cx_Oracle.connect(account)
        cursor = connection.cursor()
        cursor.execute(commit)
        connection.commit()
        cursor.close()
        connection.close()
        return

    def changeUTF(self,codenum):
        connection = cx_Oracle.connect(self.user, self.password, self.database)
        cursor = connection.cursor()
        print("""shutdown immediate;
        startup mount;
        alter system enable restricted session;
        alter system set job_queue_processes=0;
        alter system set aq_tm_processes=0;
        alter database open;
        alter database character set internal_use {};
        shutdown immediate;
        startup;
        """.format(codenum))
        
        cursor.execute("""shutdown immediate;
        startup mount;
        alter system enable restricted session;
        alter system set job_queue_processes=0;
        alter system set aq_tm_processes=0;
        alter database open;
        alter database character set internal_use {};
        shutdown immediate;
        startup;
        """.format(codenum))
        cursor.close()
        connection.close()
        return
    
if __name__ == "__main__":
    oracle = useOracle("VDATA", "xdf123", "LBORA170")
    tableName='nlpccQA'
    path="./DB_Data/clean_triple.csv"
    df=pd.read_csv(path,encoding='utf-8')
    oracle.creatDataFrame(df,tableName,"vdata/xdf123@10.9.1.170/lbora")
    oracle.insertDataToTable(df,tableName,"vdata/xdf123@10.9.1.170/lbora")