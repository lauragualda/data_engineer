from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    """
    Load a json file from a s3 bucket to Amazon Redshift.
    
    Operator parameters:
        redshift_conn_id (str) : conn_id of the connection to Redshift as configured in the Airflow UI
        aws_credentials_id (str) : conn_id of the AWS credentials as configured in the Airflow UI
        s3_bucket (str) : s3 bucket where the json file is 
        s3_key (str) : s3 key to locate json file to be loaded
        table (str) : name of the table on Redshift to load data
    """
    ui_color = '#358140'

    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        FORMAT AS JSON 'auto'
        compupdate off region 'us-west-2'
        TIMEFORMAT AS 'epochmillisecs'
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 s3_bucket="",
                 s3_key="",
                 table="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.table = table

        
    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info("Deleting data from destination Redshift table.")
        redshift.run("DELETE FROM {}".format(self.table))
    
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        self.log.info(f"Copying data from {s3_path} to table {self.table} on Redshift")
        formatted_sql = StageToRedshiftOperator.copy_sql.format(self.table, s3_path, credentials.access_key, credentials.secret_key)
        redshift.run(formatted_sql)





