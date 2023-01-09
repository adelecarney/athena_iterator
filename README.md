# athena_iterator
Demonstration of method to iterate SQL query through Athena tables

## Required packages
* [boto3](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html) - allows python to talk to Amazon AWS services, in this case Athena and S3
* [pandas](https://pandas.pydata.org/) - provides efficient storage and manipulation of data in python
* [string.Template](https://docs.python.org/3/library/string.html?highlight=safe_substitute#string.Template.safe_substitute) - allows us to easily substitute different information into the SQL we send to Athena 

## Explanation
The athena_query function sends an SQL query string (just regular old SQL) to Athena, and specifies which bucket and path to deposit the results into:
```python

def athena_query(client, params):

    response = client.start_query_execution(
        QueryString=params["query"],
        ResultConfiguration={
            'OutputLocation': 's3://' + params['bucket'] + '/' + params['path']
        }
    )
    return response
 ```   
The athena_to_s3 function sets up the Athena session, executes the query using athena_query, and monitors the query execution status each second until it completes or falls over. 
!! If it returns False, this most likely means there is an error in your SQL or you've set the max_execution value (time in seconds) too low !!
```python

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
```

This is where the panda's package is useful. We use it to read the CSV file into a dataframe...
```python
athena_tables = pd.read_csv('D:/python/athena_tables_firesize.csv')
athena_tables = athena_tables.reset_index()
```

Then iterate through it, row by row:
```python
for index, row in athena_tables.iterrows():
```
