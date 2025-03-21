1. gcloud auth application-default login
2. cmd 
ad_click_aggregator
```
python ad_click_aggregator.py \
--runner=DataflowRunner  \
--max_num_workers=1 \
--temp_location=gs://analytic-bucket/temp \
--service_account_email 289185882711-compute@developer.gserviceaccount.com \
--save_main_session \
--update \
--dataflow_service_options=graph_validate_only
```
event
```
python event_aggregator.py \
--runner=DataflowRunner  \
--max_num_workers=1 \
--temp_location=gs://analytic-bucket/temp \
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

