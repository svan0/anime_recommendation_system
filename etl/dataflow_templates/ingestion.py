import argparse
import json
import logging
import time
from typing import Any, Dict, List

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, SetupOptions
import apache_beam.transforms.window as window

class IngestionPipelineOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument(
            "--input_topic",
            type  = str,
            help="Input PubSub topic of the form "
            '"projects/<PROJECT>/topics/<TOPIC>."',
        )
        parser.add_value_provider_argument(
            "--window_interval_sec",
            default=60,
            type=int,
            help="Window interval in seconds for grouping incoming messages.",
        )

def run(argv=None):
    """Build and run the pipeline."""

    pipeline_options = PipelineOptions()
    cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    ingestion_options = pipeline_options.view_as(IngestionPipelineOptions)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline(options=pipeline_options) as pipeline:
        
        messages = (
            pipeline
            | "Read from Pub/Sub" >> beam.io.ReadFromPubSub(topic = ingestion_options.input_topic.get()).with_output_types(bytes)
            | "UTF-8 bytes to string" >> beam.Map(lambda msg: msg.decode("utf-8"))
            | "Parse JSON messages" >> beam.Map(lambda msg: json.loads(msg))
        )
        
        anime_item = (
            messages
            | "(anime_item) Filter relevant messages" >> beam.Filter(lambda msg: msg['bq_table'] == "anime_item")
            | "(anime_item) Get the data" >> beam.Map(lambda msg: msg['data'])
            | "(anime_item) Fixed-size windows" >> beam.WindowInto(window.FixedWindows(ingestion_options.window_interval_sec.get(), 0))
            | "(anime_item) Write to Big Query" >> beam.io.WriteToBigQuery(table = "landing_area.anime_item")
        )

        profile_item = (
            messages
            | "(profile_item) Filter relevant messages" >> beam.Filter(lambda msg: msg['bq_table'] == "profile_item")
            | "(profile_item) Get the data" >> beam.Map(lambda msg: msg['data'])
            | "(profile_item) Fixed-size windows" >> beam.WindowInto(window.FixedWindows(ingestion_options.window_interval_sec.get(), 0))
            | "(profile_item) Write to Big Query" >> beam.io.WriteToBigQuery(table = "landing_area.profile_item")
        )

        review_item = (
            messages
            | "(review_item) Filter relevant messages" >> beam.Filter(lambda msg: msg['bq_table'] == "review_item")
            | "(review_item) Get the data" >> beam.Map(lambda msg: msg['data'])
            | "(review_item) Fixed-size windows" >> beam.WindowInto(window.FixedWindows(ingestion_options.window_interval_sec.get(), 0))
            | "(review_item) Write to Big Query" >> beam.io.WriteToBigQuery(table = "landing_area.review_item")
        )

        watch_status_item = (
            messages
            | "(watch_status_item) Filter relevant messages" >> beam.Filter(lambda msg: msg['bq_table'] == "watch_status_item")
            | "(watch_status_item) Get the data" >> beam.Map(lambda msg: msg['data'])
            | "(watch_status_item) Fixed-size windows" >> beam.WindowInto(window.FixedWindows(ingestion_options.window_interval_sec.get(), 0))
            | "(watch_status_item) Write to Big Query" >> beam.io.WriteToBigQuery(table = "landing_area.watch_status_item")
        )

        favorite_item = (
            messages
            | "(favorite_item) Filter relevant messages" >> beam.Filter(lambda msg: msg['bq_table'] == "favorite_item")
            | "(favorite_item) Get the data" >> beam.Map(lambda msg: msg['data'])
            | "(favorite_item) Fixed-size windows" >> beam.WindowInto(window.FixedWindows(ingestion_options.window_interval_sec.get(), 0))
            | "(favorite_item) Write to Big Query" >> beam.io.WriteToBigQuery(table = "landing_area.favorite_item")
        )

        activity_item = (
            messages
            | "(activity_item) Filter relevant messages" >> beam.Filter(lambda msg: msg['bq_table'] == "activity_item")
            | "(activity_item) Get the data" >> beam.Map(lambda msg: msg['data'])
            | "(activity_item) Fixed-size windows" >> beam.WindowInto(window.FixedWindows(ingestion_options.window_interval_sec.get(), 0))
            | "(activity_item) Write to Big Query" >> beam.io.WriteToBigQuery(table = "landing_area.activity_item")
        )

        related_anime_item = (
            messages
            | "(related_anime_item) Filter relevant messages" >> beam.Filter(lambda msg: msg['bq_table'] == "related_anime_item")
            | "(related_anime_item) Get the data" >> beam.Map(lambda msg: msg['data'])
            | "(related_anime_item) Fixed-size windows" >> beam.WindowInto(window.FixedWindows(ingestion_options.window_interval_sec.get(), 0))
            | "(related_anime_item) Write to Big Query" >> beam.io.WriteToBigQuery(table = "landing_area.related_anime_item")
        )

        recommendation_item = (
            messages
            | "(recommendation_item) Filter relevant messages" >> beam.Filter(lambda msg: msg['bq_table'] == "recommendation_item")
            | "(recommendation_item) Get the data" >> beam.Map(lambda msg: msg['data'])
            | "(recommendation_item) Fixed-size windows" >> beam.WindowInto(window.FixedWindows(ingestion_options.window_interval_sec.get(), 0))
            | "(recommendation_item) Write to Big Query" >> beam.io.WriteToBigQuery(table = "landing_area.recommendation_item")
        )

        friends_item = (
            messages
            | "(friends_item) Filter relevant messages" >> beam.Filter(lambda msg: msg['bq_table'] == "friends_item")
            | "(friends_item) Get the data" >> beam.Map(lambda msg: msg['data'])
            | "(friends_item) Fixed-size windows" >> beam.WindowInto(window.FixedWindows(ingestion_options.window_interval_sec.get(), 0))
            | "(friends_item) Write to Big Query" >> beam.io.WriteToBigQuery(table = "landing_area.friends_item")
        )

        result = pipeline.run()
        result.wait_until_finish()



if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()