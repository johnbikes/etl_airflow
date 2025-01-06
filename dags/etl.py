import json

from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.decorators import task
# for putting the data into postgres
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago


## define the DAG
with DAG(
    dag_id = 'nasa_data_apod_postgres',
    start_date = days_ago(1), # TODO: interesting - see how this works at runtime
    schedule_interval = '@daily',
    catchup=False,
) as dag:

    ## Step 1: Create the etl pipeline - create the table if it does not exist

    @task
    def create_table():
        ## initialize the postgres hook
        postgres_hook = PostgresHook(postgres_conn_id="my_postgres_connection")

        ## sql query to create the table
        create_table_query="""
        CREATE TABLE IF NOT EXISTS apod_data (
            id SERIAL PRIMARY KEY,
            title VARCHAR(255),
            explanation TEXT,
            url TEXT,
            date DATE,
            media_type VARCHAR(50)
        );
        """

        ## exceute the table creation query
        postgres_hook.run(create_table_query)


    ## Step 2: Extract the NASA api data(APOD)- Astronomy Picture of the Day - creating an extract pipeline

    extract_apod = SimpleHttpOperator(
        task_id='extract_apod',
        http_conn_id='nasa_api', ## Connection ID Defined in Airflow for NASA API
        endpoint='planetary/apod', ## NASA API endpoint for APOD
        method='GET',
        # WHAT??
        data={"api_key": "{{ conn.nasa_api.extra_dejson.api_key }}"}, ## Use the API Key from the connection ...
        response_filter=lambda response: response.json(), ## Convert response to json
    )

    ## Step 3: Tansform the data (pick the information that I need to save)
    @task
    def transform_apod_data(response):
        apod_data = {
            'title': response.get('title', ''),
            'explanation': response.get('explanation', ''),
            'url': response.get('url', ''),
            'date': response.get('date', ''),
            'media_type': response.get('media_type', '')
        }
        return apod_data


    ## Step 4: Load the data into postgres
    @task
    def load_data_to_postgres(apod_data):
        ## Initialize the postgres hook
        postgres_hook = PostgresHook(postgres_conn_id='my_postgres_connection')

        ## Define the SQL insert query
        insert_query = """
            INSERT INTO apod_data (title, explanation, url, date, media_type)
            VALUES (%s, %s, %s, %s, %s);
        """

        ## Exceute the sql query
        postgres_hook.run(insert_query, parameters=(
            apod_data['title'],
            apod_data['explanation'],
            apod_data['url'],
            apod_data['date'],
            apod_data['media_type']
        ))

    ## Step 5: Verify the data with DBViewer

    ## Step 6: Define the task dependencies
    ## EXTRACT
    create_table() >> extract_apod ## Ensure the table is created before extraction
    api_response = extract_apod.output
    ## TRANSFORM
    transformed_data = transform_apod_data(api_response)
    ## LOAD - ETL
    load_data_to_postgres(transformed_data)

