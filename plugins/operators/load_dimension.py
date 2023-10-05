from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from udacity.common.final_project_sql_statements import SqlQueries


class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 table_queries={},
                 insert_mode="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.table_queries = table_queries
        self.insert_mode = insert_mode

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info('Loading {} dim table'.format(self.table))

        create_query = self.table_queries["create"].format(self.table)
        redshift.run(create_query)

        if self.insert_mode == 'append':
            insert_query = self.table_queries["insert"].format(self.table)
        
        # defaults to 'truncate' if append not specified (i.e., any input provided)
        else:
            truncate_query = SqlQueries.dim_table_truncate.format(self.table)
            redshift.run(truncate_query)
            insert_query = self.table_queries["insert"].format(self.table)

        redshift.run(insert_query)