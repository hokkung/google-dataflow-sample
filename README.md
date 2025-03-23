# google-dataflow-sample

## Getting Started
1. vir venv
2. source venv/bin/activate
3. pip install -r requirements.txt


## Script
1. gcloud auth application-default login
2. cmd 
ad_click_aggregator
```
python dataflow/ad_click_aggregator.py \
--runner=DataflowRunner  \
--max_num_workers=1 \
--temp_location=gs://analytic-demo-454105-analytics-bucket-1/temp \
--service_account_email 289185882711-compute@developer.gserviceaccount.com \
--save_main_session \
--update \
--dataflow_service_options=graph_validate_only
```
event
```
python dataflow/event_aggregator.py \
--runner=DataflowRunner  \
--max_num_workers=1 \
--temp_location=gs://analytic-demo-454105-analytics-bucket-1/temp \
--service_account_email 289185882711-compute@developer.gserviceaccount.com \
--save_main_session \
--update \
--dataflow_service_options=graph_validate_only
````

3. payload
```
{
  "timestamp": "2024-03-18T12:54:09.66644+07:00",
  "event_category": "click",
  "user_id": "1",
  "payload":  null
}
```
