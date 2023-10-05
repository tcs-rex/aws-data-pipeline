/opt/airflow/start-services.sh
/opt/airflow/start.sh

airflow users create --email student@example.com --firstname student --lastname student --password admin --role Admin --username admin

# user to input there --conn-uri's below
airflow connections add aws_credentials --conn-uri 'aws://.........................'
airflow connections add redshift --conn-uri 'redshift://..............................'

# reviewer to replace xxxxxxx with their s3 bucket name
airflow variables set s3_bucket xxxxxxx

# student used smaller subsets for testing and development - reviewer to change at their discretion
airflow variables set s3_log_data_prefix log-data/2018/11
airflow variables set s3_song_data_prefix song-data/A/A/A