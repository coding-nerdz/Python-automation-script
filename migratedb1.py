import time
import mysql.connector
import pandas as pd
import pymongo

mysql_host="127.0.0.1"
mysql_database="tpch"
mysql_user="root"
mysql_password="root"

mongodb_host = "mongodb://localhost:27017/"
mongodb_dbname = "test"

client = pymongo.MongoClient(mongodb_host)
mongodb = client[mongodb_dbname]

 mysqlCon = mysql.connector.connect(
        host=mysql_host,
        database=mysql_database,
        user=mysql_user,
        password=mysql_password,
        auth_plugin='mysql_native_password',
    )

mycol = mongodb["lineitem"]

mycursor = mysqlCon.cursor(buffered=True)
mycursor.execute("SELECT * FROM LINEITEM")
myresult = mycursor.fetchall()

field_names = [i[0] for i in mycursor.description]

lineitems = pd.DataFrame(columns=field_names, data=myresult)
lineitem = lineitems.to_dict(orient='records')

convertDeci = ['L_QUANTITY', 'L_EXTENDEDPRICE', 'L_DISCOUNT', 'L_TAX']
convertDate = ['L_SHIPDATE', 'L_COMMITDATE', 'L_RECEIPTDATE']

def get_OrderInfo(mycursor, orderKey):
    mycursor.execute("SELECT * FROM ORDERS WHERE O_ORDERKEY = "+str(orderKey)+"")
    myresult = mycursor.fetchone()
    field_names = [i[0] for i in mycursor.description]
    order = pd.DataFrame(columns=field_names, data=[myresult])
    order['O_TOTALPRICE'] = order['O_TOTALPRICE'].astype(float)
    order['O_ORDERDATE'] = order['O_ORDERDATE'].astype(str)
    order = order.to_dict(orient='records')
    return order[0]

def get_CustInfo(mycursor, custKey):
    mycursor.execute("SELECT * FROM CUSTOMER WHERE C_CUSTKEY ="+str(custKey)+"")
    myresult = mycursor.fetchone()
    field_names = [i[0] for i in mycursor.description]
    customer = pd.DataFrame(columns=field_names, data=[myresult])
    customer['C_ACCTBAL'] = customer['C_ACCTBAL'].astype(float)
    customer = customer.to_dict(orient='records')
    return customer[0]


def get_SuppNation(mycursor, nationKey):
    mycursor.execute("SELECT * FROM NATION where N_NATIONKEY ="+str(nationKey)+"")
    myresult = mycursor.fetchone()
    field_names = [i[0] for i in mycursor.description]
    nation = pd.DataFrame(columns=field_names, data=[myresult])
    nation = nation.to_dict(orient='records')
    return nation[0]

def get_SuppInfo(mycursor, suppKey):
    mycursor.execute("SELECT * FROM SUPPLIER s where S_SUPPKEY = "+str(suppKey)+"")
    myresult = list(mycursor.fetchone())
    field_names = [i[0] for i in mycursor.description]
    supplier = pd.DataFrame(columns=field_names, data=[myresult])
    supplier['S_ACCTBAL'] = supplier['S_ACCTBAL'].astype(float)
    supplier = supplier.to_dict(orient='records')
    return supplier[0]

def get_PartsuppInfo(mycursor, suppKey, partKey):
    mycursor.execute("SELECT * FROM PARTSUPP where PS_SUPPKEY = "+str(suppKey)+" and PS_PARTKEY="+str(partKey)+"")
    myresult = mycursor.fetchone()
    field_names = [i[0] for i in mycursor.description]
    supplier = pd.DataFrame(columns=field_names, data=[myresult])
    supplier['PS_SUPPLYCOST'] = supplier['PS_SUPPLYCOST'].astype(float)
    supplier = supplier.to_dict(orient='records')
    return supplier[0]

def get_PartInfo(mycursor, partKey): 
    mycursor.execute("SELECT * FROM PART where P_PARTKEY = "+str(partKey)+"")
    myresult = mycursor.fetchone()
    field_names = [i[0] for i in mycursor.description]
    part = pd.DataFrame(columns=field_names, data=[myresult])
    part['P_RETAILPRICE'] = part['P_RETAILPRICE'].astype(float)
    part = part.to_dict(orient='records') 
    return part[0]

def get_SuppRegion(mycursor, regionKey):
    mycursor.execute("SELECT * FROM REGION where R_REGIONKEY = "+str(regionKey)+"")
    myresult = mycursor.fetchone()
    field_names = [i[0] for i in mycursor.description]
    region = pd.DataFrame(columns=field_names, data=[myresult])
    region = region.to_dict(orient='records')
    return region[0]


for item in lineitem:
    OrderInfo = get_OrderInfo(mycursor, item['L_ORDERKEY'])
    OrderInfo['O_CUSTKEY'] = get_CustInfo(mycursor, OrderInfo['O_CUSTKEY'])
    OrderInfo['O_CUSTKEY']['C_NATIONKEY'] = get_SuppNation(mycursor, OrderInfo['O_CUSTKEY']['C_NATIONKEY'])
    OrderInfo['O_CUSTKEY']['C_NATIONKEY']['SUPPLIERS'] =  get_SuppInfo(mycursor, item['L_SUPPKEY'])
    OrderInfo['O_CUSTKEY']['C_NATIONKEY']['SUPPLIERS']['PARTSUPP'] = get_PartsuppInfo(mycursor, item['L_SUPPKEY'], item['L_PARTKEY'])
    OrderInfo['O_CUSTKEY']['C_NATIONKEY']['SUPPLIERS']['PARTSUPP']['PART'] = get_PartInfo(mycursor, OrderInfo['O_CUSTKEY']['C_NATIONKEY']['SUPPLIERS']['PARTSUPP']['PS_PARTKEY'])
    OrderInfo['O_CUSTKEY']['C_NATIONKEY']['SUPPLIERS']['PARTSUPP']['PART']['REGION'] =  get_SuppRegion(mycursor, OrderInfo['O_CUSTKEY']['C_NATIONKEY']['N_REGIONKEY'])
    item['ORDERS'] = OrderInfo
    for name in field_names:
        if name in convertDeci:
            item[name] = float(item[name])
        elif name in convertDate:
            item[name] = item[name].strftime('%Y-%m-%d')

    mycol.insert_one(item)