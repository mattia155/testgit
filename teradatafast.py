import pandas as pd
import teradatasql
import os

################################################################################
## FUNCTIONS 

config = {}

def read_config():
    for key in os.environ:
        if "teradata" in key:
            value=os.environ[str(key)]
            config[key.strip()] = value.strip()
    return config

def connect(cfg):
    host, port, user, pwd = cfg['teradata_host'], cfg['teradata_port'], cfg['teradata_user'], cfg['teradata_pwd']
    return teradatasql.connect(host=host, dbs_port=port, user=user, password=pwd, cop=True)
    
def fastload_df(input_df, teradata_db, output_table):
    # Read credentials from environment
    cfg = read_config();
    # With connection open execute query
    with connect(cfg) as con:
        with con.cursor() as cur:
            try:
                req = '{fn teradata_nativesql}{fn teradata_autocommit_off}'
                print(req)
                cur.execute(req)
                # Find columns and values from dataset
                columns = ','.join(input_df.columns.values.tolist())
                values_ph = ','.join(['?'] * len(input_df.columns))
                values = input_df.values.tolist()
                # Compose and execute fastload insert query
                req = '''
                {{fn teradata_error_table_database({e})}}
                {{fn teradata_try_fastload}}
                INSERT INTO {d}.{o} ({c})
                VALUES ({v})'''.format(d=teradata_db, o=output_table, c=columns, v=values_ph, e=cfg['teradata_error_db'])
                print(req)
                cur.executemany(req, values)
                print('Committing ...')
                con.commit()
                # Restore autocommit
                req = "{fn teradata_nativesql}{fn teradata_autocommit_on}"
                print(req)
                cur.execute(req)
            except Exception as ex:
                print(ex)

def delete_table_rows(teradata_db, teradata_table):
    # Read credentials from environment
    cfg = read_config();
    # With connection open execute query
    with connect(cfg) as con:
        with con.cursor () as cur:
            try:
                # Delete all the records in output_table
                req = '''
                DELETE FROM {d}.{t}
                '''.format(d=teradata_db, t=teradata_table)
                print(req)
                cur.execute(req)
            except Exception as ex:
                print(ex) 
                
def delete_and_insert_df(input_df, teradata_db, output_table):
    # Delete
    delete_table_rows(teradata_db=teradata_db, teradata_table=output_table)
    # Insert
    fastload_df(input_df=input_df, teradata_db=teradata_db, output_table=output_table)
    
def fastexport_data(teradata_db, teradata_table):
    # Read credentials from environment
    cfg = read_config();
    # With connection open execute query
    with connect(cfg) as con:
        try:
            # Compose and execute get data query
            req='''
            {{fn teradata_try_fastexport}}
            SELECT * FROM {d}.{o};
            '''.format(d=teradata_db, o=teradata_table)
            print(req)
            df = pd.read_sql(req, con)
            return df
        except Exception as ex:
            print(ex)
            
################################################################################


################################################################################
## MAIN

## FastExport
print("Testing fastexport ...")
pandas_df_data = fastexport_data('V3_DATALAB_DM', "TEST_TRAGHETTO_TERADATA_INPUT")
print("Fastexport done.")

## Delete all records and then FastLoad
#print("Testing delete + fastload...")
#delete_and_insert_df(pandas_df_data, "V3_DATALAB_DM", "TEST_TRAGHETTO_TERADATA_INPUT")
#print("Fastload done.")

## Only FastLoad
#fastload_df(pandas_df_data, "V3_DATALAB_DM", "TEST_TRAGHETTO_TERADATA_INPUT")

################################################################################
