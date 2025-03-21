import json
import pytz
from datetime import datetime
import requests
import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam import WindowInto
from apache_beam.transforms.window import FixedWindows

logging.basicConfig(level=logging.INFO)


class ParseJson(beam.DoFn):
    def process(self, element):
        try:
            yield json.loads(element.decode("utf-8"))
        except Exception as e:
            logging.error(f"Error parsing JSON: {e}")


class FormatForBigQuery(beam.DoFn):
    def process(self, element, window=beam.DoFn.WindowParam):
        event_value, count = element
        yield {
            "item_id": event_value,
            "click": count,
            "window_start": window.start.to_utc_datetime().isoformat(),
            "window_end": window.end.to_utc_datetime().isoformat()
        }
      

class EnrichWithDataDetails(beam.DoFn):
    def setup(self):
        self.session = requests.Session()

    def process(self, element):
        try:
            response = self.session.get("https://jsonplaceholder.typicode.com/todos/1", timeout=5)
            if response.status_code == 200:
                user_data = response.json()
                logging.info(user_data)
                element.update(user_data)
                yield element
            else:
                logging.info(f"API call failed status: {response.status_code}")
        except Exception as e:
            logging.error(f"Error fetching user details: {e}")

def run():
    pipeline_options = PipelineOptions(
        streaming=True,
        project="analytic-demo-454105",
        region="us-central1",
        job_name="ad-click-aggregator",
    )

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | "ReadFromPubSub" >> beam.io.ReadFromPubSub(subscription="projects/analytic-demo-454105/subscriptions/click-events-sub")
            | "ParseJson" >> beam.ParDo(ParseJson())
            | "WindowInto60Sec" >> WindowInto(FixedWindows(30))
            | "GetMoreDetail" >> beam.ParDo(EnrichWithDataDetails())
            | "ExtractEventType" >> beam.Map(lambda e: (e["event_type"], 1))
            | "CountClicks" >> beam.CombinePerKey(sum)
            | "FormatForBigQuery" >> beam.ParDo(FormatForBigQuery())
            | "WriteToBigQuery" >> WriteToBigQuery(
                table="analytic-demo-454105.analytics_dev.click_events",
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )
        )

if __name__ == "__main__":
    run()
