from h11 import Data
from kfp.v2.dsl import component
from kfp.v2.dsl import (
    Input,
    Output,
    Model,
    Metrics
)

@component(
    packages_to_install=['pandas==1.3.4', 'fsspec==2022.2.0', 'gcsfs==2022.2.0', 'google-cloud-aiplatform==1.11.0']
)
def get_model_training_details(
    project_id:str,
    input_model: Input[Model],
    model_name:str,
    labels:dict,
    input_metrics: Input[Metrics],
    output_model: Output[Model],
    output_metrics: Output[Metrics]
):  
    import pandas as pd
    from google.cloud import aiplatform

    output_model.metadata["model_name"] = model_name
    for k, v in labels.items():
        output_model.metadata[k] = v
    output_model.path = f"{input_model.path}/"

    metrics_df = pd.read_json(f"{input_metrics.path}/metrics.json")
    for metric in metrics_df.iterrows():
        metric_name = metric[1]['metrics']['name']
        metric_value = metric[1]['metrics']['number_value']
        output_metrics.log_metric(metric_name, metric_value)
        output_model.metadata[metric_name] = metric_value
    
    labels = {k: str(v).replace(".", "") for k, v in labels.items()}
    aiplatform.init(
        project=project_id, 
        location='us-central1',
        staging_bucket='gs://anime-rec-dev-ml-pipelines'
    )
    aiplatform.Model.upload(
        display_name = model_name,
        artifact_uri = input_model.path,
        serving_container_image_uri="gcr.io/anime-rec-dev/user_anime_infer_ranking:latest",
        labels = labels
    )
