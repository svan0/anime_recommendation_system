from kfp.v2.dsl import component
from kfp.v2.dsl import (
    Input,
    Output,
    Dataset
)

@component(
    packages_to_install=['google-cloud-bigquery==2.31.0']
)
def gcs_to_bq_table(
    gcs_input_data:Input[Dataset],
    gcs_input_data_format: str,
    gcs_input_data_schema:list,
    project_id:str, 
    destination_dataset_id: str, 
    destination_table_id: str
):
    
    import logging
    from google.cloud import bigquery

    logging.basicConfig(
        format='%(levelname)s: %(asctime)s: %(message)s',
        level=logging.INFO
    )

    client = bigquery.Client(project=project_id)

    table_ref = client.dataset(destination_dataset_id).table(destination_table_id)

    if gcs_input_data_format == 'csv':
        job_config = bigquery.LoadJobConfig(
            schema=[bigquery.SchemaField(x[0], x[1]) for x in gcs_input_data_schema],
            skip_leading_rows=1,
            field_delimiter=',',
            source_format=bigquery.SourceFormat.CSV
        )
    elif gcs_input_data_format == 'avro':
        job_config = bigquery.LoadJobConfig(
            schema=[bigquery.SchemaField(x[0], x[1]) for x in gcs_input_data_schema],
            source_format=bigquery.SourceFormat.AVRO,
            use_avro_logical_types=True
        )
    else:
        raise(f"{gcs_input_data_format} format is not supported. Specify either 'CSV' or 'AVRO'")

    gcs_input_data = gcs_input_data.path.replace('/gcs/', 'gs://') + "/*"

    load_job = client.load_table_from_uri(
        source_uris = gcs_input_data, 
        destination = table_ref, 
        job_config = job_config
    )
    logging.info(f"Started exporting {gcs_input_data} to BQ table {table_ref.path}")
    load_job.result()
    logging.info(f"Started exporting {gcs_input_data} to BQ table {table_ref.path}")

    client.close()

@component(
    packages_to_install=['google-cloud-bigquery==2.31.0']
)
def run_query_save_to_bq_table_and_gcs(
    query: str,
    project_id: str, 
    destination_dataset_id: str, 
    destination_table_id: str,
    gcs_output_format: str,
    gcs_output_data: Output[Dataset]
):
    
    import logging
    from google.cloud import bigquery
    
    logging.basicConfig(
        format='%(levelname)s: %(asctime)s: %(message)s',
        level=logging.INFO
    )

    client = bigquery.Client(project=project_id)
    destination_table_ref = client.dataset(destination_dataset_id).table(destination_table_id)

    job_config = bigquery.QueryJobConfig()
    job_config.destination = destination_table_ref

    query_job = client.query(
        query=query,
        location='us-central1',
        job_config=job_config
    )
    logging.info(f"Started running query and saving results to BQ table : {destination_table_ref.path}")
    
    query_job.result()
    logging.info(f"Finished running query and saved results to BQ table : {destination_table_ref.path}")
    logging.info(f"{destination_table_ref.path} has {client.get_table(destination_table_ref).num_rows} rows")

    if gcs_output_format == 'csv':
        job_config = bigquery.job.ExtractJobConfig()
        job_config.field_delimiter = ','
        job_config.destination_format = bigquery.job.DestinationFormat.CSV

    elif gcs_output_format == 'avro':
        job_config = bigquery.job.ExtractJobConfig()
        job_config.destination_format = bigquery.job.DestinationFormat.AVRO
        job_config.use_avro_logical_types = True
        job_config.compression = bigquery.Compression.SNAPPY
    
    else:
        raise(f"{gcs_output_format} format is not supported. Specify either 'CSV' or 'AVRO'")
    
    gcs_output_path = gcs_output_data.path.replace('/gcs/', 'gs://') + "/*"
    extract_job = client.extract_table(
        source=destination_table_ref,
        destination_uris=gcs_output_path,
        location="us-central1",
        job_config=job_config
    )
    logging.info(f"Started exporting BQ table {destination_table_ref.path} to {gcs_output_path}")
    
    extract_job.result()
    logging.info(f"Finished exporting BQ table {destination_table_ref.path} to {gcs_output_path}")

    client.close()
