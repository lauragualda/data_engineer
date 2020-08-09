from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    """
    Load data from staging DBs on Redshift into fact tables.
    
    Operator parameters:
        redshift_conn_id (str) : conn_id of the connection to Redshift as configured in the Airflow UI
        load_sql_query (str) : SELECT query to be used for loading data
        table (str) : name of the table on Redshift to load data
    """

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 load_sql_query,
                 table,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.load_sql = load_sql_query
        self.table = table

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Loading data into {} fact table".format(self.table_name))
        formatted_sql = 'INSERT INTO %s %s' % (self.table_name, self.load_sql)
        redshift.run(formatted_sql)
