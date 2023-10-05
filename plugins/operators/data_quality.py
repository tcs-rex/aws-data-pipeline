from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 table_queries={},
                 column="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.table_queries = table_queries
        self.column = column

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        for query_name, query in self.table_queries.items():
            if query_name == 'null_count':
                self.log.info("Checking table '{}' column {} nulls...".format(self.table, self.column))
                formatted_query = query.format(self.table, self.column)
                num_nulls = redshift.get_first(formatted_query)[0]
                if num_nulls == None:
                    self.log.error('Table is empty...')
                    raise Exception(f'Table {self.table} is empty.')
                elif num_nulls > 0:
                    self.log.error(f'NULL values in {self.table}.{self.column}: {num_nulls}')
                    raise Exception(f'NULL values in {self.table}.{self.column}: {num_nulls}')
                else:
                    self.log.info(f'No NULL values in {self.table}.{self.column}')
            elif query_name == 'row_count':
                self.log.info("Checking table '{}' row count...".format(self.table))
                formatted_query = query.format(self.table)
                row_count = redshift.get_first(formatted_query)[0]
                if row_count == None:
                    self.log.error('Table is empty...')
                    raise Exception(f'Table {self.table} is empty.')
                else:
                    self.log.info(f'Number of rows in {self.table}: {row_count}')
            else:
                self.log.error('Query does not exist...')
                raise Exception(f'Query does not exist...')