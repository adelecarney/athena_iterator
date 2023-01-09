import boto3
import pandas as pd
import io
import re
import time
from string import Template

params = {
    'region': 'ap-southeast-2',
    'bucket': 'tempgippsland',
    'path': 'temp/athena/output',
    'query': ''
}

session = boto3.Session()
s3_client = boto3.client('s3')

def athena_query(client, params):
    
    response = client.start_query_execution(
        QueryString=params["query"],
        ResultConfiguration={
            'OutputLocation': 's3://' + params['bucket'] + '/' + params['path']
        }
    )
    return response

def athena_to_s3(session, params, max_execution = 500):
    client = session.client('athena', region_name=params["region"])
    execution = athena_query(client, params)
    execution_id = execution['QueryExecutionId']
    state = 'RUNNING'

    while (max_execution > 0 and state in ['RUNNING', 'QUEUED']):
        max_execution = max_execution - 1
        response = client.get_query_execution(QueryExecutionId = execution_id)

        if 'QueryExecution' in response and \
                'Status' in response['QueryExecution'] and \
                'State' in response['QueryExecution']['Status']:
            state = response['QueryExecution']['Status']['State']
            if state == 'FAILED':
                return False
            elif state == 'SUCCEEDED':
                s3_path = response['QueryExecution']['ResultConfiguration']['OutputLocation']
                filename = re.findall('.*\/(.*)', s3_path)[0]
                return filename
        time.sleep(1)
    
    return False

# Deletes all files in your path so use carefully!
def cleanup(session, params):
    s3 = session.resource('s3')
    my_bucket = s3.Bucket(params['bucket'])
    for item in my_bucket.objects.filter(Prefix=params['path']):
        item.delete()


# MAIN BELOW

# load a list of athena tables to iterate through
athena_tables = pd.read_csv('D:/python/athena_tables_firesize.csv')
athena_tables = athena_tables.reset_index()

first_table = True
first_output = True

for index, row in athena_tables.iterrows():

    query_string = Template('''with phx_data as (
                                select 
                                    cell.*,
                                    grid.firefmz as firefmz,
                                    grid.delwp_district
                                from 
                                    $table.cell cell,
                                    temp_gippsland.grid_cell_180m grid
                                where
                                    cell.intensity > 0 
                                and
                                    grid.cellid = cell.cellid
                                )
                            select '$percentage' as treat_perc,
                                '$weather' as wx_rank,
                                '$replicate' as replicate,
                                ignitionid,
                                firefmz,
                                count(cellid) as cells,
                                count(cellid) * 3.24 / 10 as burntarea
                            from 
                                phx_data
                            group by 
                                ignitionid, firefmz
                            ''')
    
    params['query'] = query_string.safe_substitute({'percentage': row['perc'],
                                                    'weather': row['wx'],
                                                    'replicate': row['rep'],
                                                    'table': row['database_name'],
                                                    })

    s3_filename = athena_to_s3(session, params)
    print('s3_filename=' + str(s3_filename))
    s3_bucket = str(params['bucket'])
    s3_key = str(params['path'] + '/' + s3_filename)
    s3_data = s3_client.get_object(Bucket=s3_bucket, Key=s3_key)
    results_data = pd.read_csv(io.BytesIO(s3_data['Body'].read())) #, index_col=0)
    
    print(results_data.info())
    print(results_data)

    # copy the first result to output_table, otherwise append results to it
    if first_table == True:
        output_data = results_data
        first_table = False
    else:
        output_data = pd.concat([output_data, results_data], ignore_index=True)

    # save output_data table to csv
    if first_output == True:
        output_data.to_csv('D:/python/output_data.csv')
    else:
        first_output = False
        output_data.to_csv('D:/python/output_data.csv', mode='a', index=False, header=False)

# Removes all files from the s3 folder you specified, so be careful
cleanup(session, params)
