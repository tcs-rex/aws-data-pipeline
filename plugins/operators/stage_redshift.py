from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.models import Variable
import logging


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
    copy_sql = """
        COPY {} 
        FROM '{}' 
        ACCESS_KEY_ID '{}' 
        SECRET_ACCESS_KEY '{}' 
        {};"""

    @apply_defaults
    def __init__(self,
                 aws_credentials_id="",
                 redshift_conn_id="",
                 table="",
                 task_id="",
                 s3_key="",
                 delimiter=",",
                 ignore_headers=1,
                 create_table_query="",
                 prefix="",
                 json_format="",
                 *args,
                 **kwargs):
        super(StageToRedshiftOperator, self).__init__(task_id=task_id, *args, **kwargs)
        self.aws_credentials_id = aws_credentials_id
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.bucket = Variable.get('s3_bucket')
        self.s3_key = s3_key
        self.delimiter = delimiter
        self.ignore_headers = ignore_headers
        self.create_table_query = create_table_query
        self.prefix = prefix
        self.json_format = json_format
        

    def execute(self, context):
        s3_hook = S3Hook(aws_conn_id=self.aws_credentials_id)
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        # drop table 
        self.log.info("Dropping {} Redshift table".format(self.table))
        redshift.run("DROP TABLE IF EXISTS {}".format(self.table))
        
        # create table
        self.log.info("Creating {} Redshift table".format(self.table))
        redshift.run("CREATE TABLE IF NOT EXISTS public.{} ".format(self.table) + self.create_table_query)
      
        # copy data
        self.log.info("Staging data - copying data from S3 to Redshift")
        s3_path = "s3://{}/{}/".format(self.bucket, self.prefix)
        self.log.info("source path: " + s3_path)
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            s3_hook.get_credentials().access_key,
            s3_hook.get_credentials().secret_key,
            self.json_format
        )
        redshift.run(formatted_sql)
