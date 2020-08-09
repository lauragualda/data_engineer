from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """
    Run a data quality check by querying the number of records in each DWH table listed on list_tables.
    
    Operator parameters:
        redshift_conn_id (str) : conn_id of the connection to Redshift as configured in the Airflow UI
        list_tables (list) : list of tables to run data quality check 
    """

    ui_color = '#89DA59'
    
    assert_null_checks = [
            {'table': 'users',
             'check_sql': "SELECT COUNT(*) FROM users WHERE user_id is null",
             'expected': 0},
            {'table': 'songs',
             'check_sql': "SELECT COUNT(*) FROM songs WHERE song_id is null",
             'expected': 0},
            {'table': 'artists',
             'check_sql': "SELECT COUNT(*) FROM artists WHERE artist_id is null",
             'expected': 0}
        ]

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 list_tables,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.list_tables = list_tables
       
    def execute(self, context):
        redshift= PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        # 1. Checking that all tables contain records
        for table in self.list_tables:
            records = redshift.get_records(f"SELECT COUNT(*) FROM {table}")
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed: {table} returned no results.")
            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(f"Data quality check failed: {table} contained 0 rows.")
            logging.info(f"Found {records[0][0]} records on table {table}. Data quality check passed.")
            
        # 2. Checking that all id columns have no null values
        for check in DataQualityOperator.assert_null_checks:
            records = redshift.get_records(check['check_sql'])
            if records[0][0] != check['expected_result']:
                raise ValueError(f"Data quality check failed: id column on {check['table']} has null values.")
            else:
                logging.info(f"No null values on id column on {check['table']}.  Data quality check passed.")
       
    
            