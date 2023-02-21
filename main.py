# create python script to load data a defined data to table in snowflake also create create table if not exist.

# db = CICD_TEST
# schema =  SCH
# table = TABLE_CICD (id, status, current_timestamp)
# data like when script runs id auto increament , status 'DATA_LOADED', current_timestamp


import snowflake.connector

# Create a connection object
ctx = snowflake.connector.connect(
            user='VKUMA50',
            password='Varun@123',
            account='ho10872',
            warehouse='COMPUTE_WH',
            database='CICD_POC',
            schema='TEST',
            region='central-india.azure'
            )

# Create a cursor object
cur = ctx.cursor()

# Create sequence for id
cur.execute("create sequence if not exists SEQ_CICD start 1 increment 1")

# Create table if not exist with id as primary key and auto increment

cur.execute("create table if not exists TABLE_CICD (id int primary key autoincrement, status string, current_ts timestamp)")

# insert data into table with id as auto increment

cur.execute("insert into TABLE_CICD (id, status, current_ts) values ( SEQ_CICD.NEXTVAL, 'DATA_LOADED', current_timestamp)")

# Fetch the last inserted row
res = cur.execute("select * from TABLE_CICD order by id desc limit 1").fetchall()
print(res)