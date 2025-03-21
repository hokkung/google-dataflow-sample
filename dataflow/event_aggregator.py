import json
import pytz
from datetime import datetime

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam import WindowInto
from apache_beam.transforms.window import FixedWindows


class ParseJson(beam.DoFn):
    """Parses Pub/Sub messages into JSON."""
    def process(self, element):
        try:
            event = json.loads(element.decode("utf-8"))
            yield event
        except Exception as e:
            print(f"Error parsing JSON: {e}")


class FormatForBigQuery(beam.DoFn):
    """Formats aggregated data for BigQuery."""
    def process(self, element, window=beam.DoFn.WindowParam):
        yield {
            "event_type": element["event_type"],
            "user_id": element["user_id"],
            "timestamp": element["timestamp"],
            "payload": element["payload"],
        }

def run():
    pipeline_options = PipelineOptions(
        streaming=True,
        project="analytic-demo-454105",
        region="us-central1",
        job_name="event-aggregator",
    )

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | "ReadFromPubSub" >> beam.io.ReadFromPubSub(subscription="projects/analytic-demo-454105/subscriptions/event-sub")
            | "ParseJson" >> beam.ParDo(ParseJson())
            | "WindowInto60Sec" >> WindowInto(FixedWindows(30))
            | "FormatForBigQuery" >> beam.ParDo(FormatForBigQuery())
            | "WriteToBigQuery" >> WriteToBigQuery(
                table="analytic-demo-454105.analytic_dev.events",
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )
        )

if __name__ == "__main__":
    run()