import json
import pytz
import requests
import logging

from typing import Any, Tuple
from datetime import datetime

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.io import PubsubMessage, WriteToPubSub
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
            "timestamp": datetime.now(pytz.UTC),
            "event_value": event_value,
            "click": count,
            "window_start": window.start.to_utc_datetime().isoformat(),
            "window_end": window.end.to_utc_datetime().isoformat()
        }
      
class FormatToPubSubMessage(beam.DoFn):
    def process(self, item: Tuple[str, Any]):
        try:
            from apache_beam.io import PubsubMessage
            message = {
                "event_value": item[0],
                "click_count": item[1],
            }

            attributes = {"event_value": item[0]}
            data = bytes(json.dumps(message), "utf-8")

            yield PubsubMessage(data=data, attributes=attributes)
        except Exception as e:
            logging.error('Unable to converted pub/sub message', e)

class FraudAPI(beam.DoFn):
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
        grouped_events = (
            p
            | "ReadFromPubSub" >> beam.io.ReadFromPubSub(subscription="projects/analytic-demo-454105/subscriptions/click-events-sub")
            | "ParseJson" >> beam.ParDo(ParseJson())
            | "WindowInto10Sec" >> WindowInto(FixedWindows(30))
            | "CheckClickFraudAPI" >> beam.ParDo(FraudAPI())
            | "ExtractEventValue" >> beam.Map(lambda e: (e["event_value"], 1))
            | "CountClicks" >> beam.CombinePerKey(sum)
        )

        formatted_to_bq = (
            grouped_events
            | "FormatForBigQuery" >> beam.ParDo(FormatForBigQuery())
        )

        formatted_to_bq | "WriteToBigQuery" >> WriteToBigQuery(
                table="analytic-demo-454105.analytics_dev.click_events",
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            )

        grouped_events | "FormatPubSubMessage" >> beam.ParDo(FormatToPubSubMessage()) \
            | "PublishToPubSub" >> WriteToPubSub(
                topic="projects/analytic-demo-454105/topics/ad-clicked-events-topic",
                with_attributes=True
                )


if __name__ == "__main__":
    run()
