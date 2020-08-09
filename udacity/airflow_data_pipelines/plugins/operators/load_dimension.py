from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    """
    Load data from staging DBs on Redshift into dimension tables.
    
    Operator parameters:
        redshift_conn_id (str) : conn_id of the connection to Redshift as configured in the Airflow UI
        load_sql_query (str) : SELECT query to be used for loading data
        table (str) : name of the table on Redshift to load data
        insert_append (bool) : boolean variable to allow switching to "append" insert methodd, default True
                               if False, "truncate" method is applied, ie. target table is emptied before the load    
    """
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 load_sql_query,
                 table,
                 insert_append=True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.load_sql = load_sql_query
        self.table = table

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        # in case truncate insert method should be applied
        if not insert_append:
            self.log.info("Deleting data from destination Redshift table.")
            redshift.run("DELETE FROM {}".format(self.table))

        self.log.info("Loading data into {} fact table".format(self.table_name))
        formatted_sql = 'INSERT INTO %s %s' % (self.table_name, self.load_sql)
        redshift.run(formatted_sql)
            
