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
        postgres_hook = PostgresHook(postgres_con_id="my_postgres_connection")

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

    ## Step 3: Tansform the data (pick the information that I need to save)

    ## Step 4: Load the data into postgres

    ## Step 5: Verify the data with DBViewer

    ## Step 6: Define the task dependencies


