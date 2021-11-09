import argparse
import json
import logging
import time
from typing import Any, Dict, List

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, SetupOptions
import apache_beam.transforms.window as window

class CustomPipelineOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument(
            "--input_topic",
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
    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    custom_options = pipeline_options.view_as(CustomPipelineOptions)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline(options=pipeline_options) as pipeline:
        
        messages = (
            pipeline
            | "Read from Pub/Sub" >> beam.io.ReadFromPubSub(topic = custom_options.input_topic).with_output_types(bytes)
            | "UTF-8 bytes to string" >> beam.Map(lambda msg: msg.decode("utf-8"))
            | "Parse JSON messages" >> beam.Map(lambda msg: json.loads(msg))
        )
        
        anime_item = (
            messages
            | "Filter relevant messages" >> beam.Filter(lambda msg: msg['bq_table'] == "anime_item")
            | "Get the data" >> beam.Map(lambda msg: msg['data'])
            | "Fixed-size windows" >> beam.WindowInto(window.FixedWindows(custom_options.window_interval_sec, 0))
            | "Write to Big Query" >> beam.io.WriteToBigQuery(table = "landing_area.anime_item")
        )

        profile_item = (
            messages
            | "Filter relevant messages" >> beam.Filter(lambda msg: msg['bq_table'] == "profile_item")
            | "Get the data" >> beam.Map(lambda msg: msg['data'])
            | "Fixed-size windows" >> beam.WindowInto(window.FixedWindows(custom_options.window_interval_sec, 0))
            | "Write to Big Query" >> beam.io.WriteToBigQuery(table = "landing_area.profile_item")
        )

        review_item = (
            messages
            | "Filter relevant messages" >> beam.Filter(lambda msg: msg['bq_table'] == "review_item")
            | "Get the data" >> beam.Map(lambda msg: msg['data'])
            | "Fixed-size windows" >> beam.WindowInto(window.FixedWindows(custom_options.window_interval_sec, 0))
            | "Write to Big Query" >> beam.io.WriteToBigQuery(table = "landing_area.review_item")
        )

        watch_status_item = (
            messages
            | "Filter relevant messages" >> beam.Filter(lambda msg: msg['bq_table'] == "watch_status_item")
            | "Get the data" >> beam.Map(lambda msg: msg['data'])
            | "Fixed-size windows" >> beam.WindowInto(window.FixedWindows(custom_options.window_interval_sec, 0))
            | "Write to Big Query" >> beam.io.WriteToBigQuery(table = "landing_area.watch_status_item")
        )

        favorite_item = (
            messages
            | "Filter relevant messages" >> beam.Filter(lambda msg: msg['bq_table'] == "favorite_item")
            | "Get the data" >> beam.Map(lambda msg: msg['data'])
            | "Fixed-size windows" >> beam.WindowInto(window.FixedWindows(custom_options.window_interval_sec, 0))
            | "Write to Big Query" >> beam.io.WriteToBigQuery(table = "landing_area.favorite_item")
        )

        activity_item = (
            messages
            | "Filter relevant messages" >> beam.Filter(lambda msg: msg['bq_table'] == "activity_item")
            | "Get the data" >> beam.Map(lambda msg: msg['data'])
            | "Fixed-size windows" >> beam.WindowInto(window.FixedWindows(custom_options.window_interval_sec, 0))
            | "Write to Big Query" >> beam.io.WriteToBigQuery(table = "landing_area.activity_item")
        )

        related_anime_item = (
            messages
            | "Filter relevant messages" >> beam.Filter(lambda msg: msg['bq_table'] == "related_anime_item")
            | "Get the data" >> beam.Map(lambda msg: msg['data'])
            | "Fixed-size windows" >> beam.WindowInto(window.FixedWindows(custom_options.window_interval_sec, 0))
            | "Write to Big Query" >> beam.io.WriteToBigQuery(table = "landing_area.related_anime_item")
        )

        recommendation_item = (
            messages
            | "Filter relevant messages" >> beam.Filter(lambda msg: msg['bq_table'] == "recommendation_item")
            | "Get the data" >> beam.Map(lambda msg: msg['data'])
            | "Fixed-size windows" >> beam.WindowInto(window.FixedWindows(custom_options.window_interval_sec, 0))
            | "Write to Big Query" >> beam.io.WriteToBigQuery(table = "landing_area.recommendation_item")
        )

        friends_item = (
            messages
            | "Filter relevant messages" >> beam.Filter(lambda msg: msg['bq_table'] == "friends_item")
            | "Get the data" >> beam.Map(lambda msg: msg['data'])
            | "Fixed-size windows" >> beam.WindowInto(window.FixedWindows(custom_options.window_interval_sec, 0))
            | "Write to Big Query" >> beam.io.WriteToBigQuery(table = "landing_area.friends_item")
        )

        result = pipeline.run()
        result.wait_until_finish()



if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()