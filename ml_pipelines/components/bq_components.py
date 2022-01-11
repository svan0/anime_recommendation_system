from kfp.v2.dsl import component
from kfp.v2.dsl import (
    Input,
    Output,
    Dataset
)

@component(
    packages_to_install=['google-cloud-bigquery==2.31.0', 'pyarrow==6.0.1', 'pandas==1.3.4']
)
def load_big_query_data(query:str, output_csv:Output[Dataset]):
    from google.cloud import bigquery
    client = bigquery.Client(project="anime-rec-dev")
    dataset_ref = client.dataset("prod_area_us")
    job_config = bigquery.QueryJobConfig()
    query_job = client.query(query, job_config=job_config)
    data = query_job.to_dataframe()
    data.to_csv(output_csv.path, index = False)

@component(
    packages_to_install=['google-cloud-bigquery==2.31.0', 'pyarrow==6.0.1', 'pandas==1.3.4']
)
def load_big_query_external_data(external_table_uri:Input[Dataset],
                                 external_table_schema:list,
                                 external_table_id:str,
                                 query:str, 
                                 output_csv:Output[Dataset]):
    from google.cloud import bigquery
    client = bigquery.Client(project="anime-rec-dev")
    
    external_config = bigquery.ExternalConfig("CSV")
    external_config.source_uris = [
        external_table_uri.path.replace('/gcs/', 'gs://')
    ]
    external_config.schema = [bigquery.SchemaField(x[0], x[1]) for x in external_table_schema]
    external_config.options.skip_leading_rows = 1
    
    job_config = bigquery.QueryJobConfig(table_definitions={external_table_id: external_config})
    query_job = client.query(query, job_config=job_config)
    data = query_job.to_dataframe()
    data.to_csv(output_csv.path, index = False)