from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    """
    - Json_arg accepts json path, auto, auto ignore case and no shred. All as strings
    """
    ui_color = '#358140'
    
    # make s3_key templatable 
    template_fields = ("s3_key",)
    
    # Skeleton S3 to Redshift
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        json '{}'
    """
    
    
    # Apply default params specified in dag.py, params pass to function as **kwargs
    @apply_defaults
    # Define operator parameters
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 json_arg="",
                 append="",
                 *args, **kwargs):
        
        # map init arguments to attributes
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.aws_credentials_id = aws_credentials_id
        self.json_arg = json_arg
        self.append = append 

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        self.log.info("Getting AWS Credentials")
        credentials = aws_hook.get_credentials()
        
        #connect to redshift
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))
        rendered_key = self.s3_key.format(**context)
        
        # fill s3 path
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        
        # fill 'copy_sql' statement defined above
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.json_arg
        )
        
        self.log.info("Copy staging data to Redshift table : {self.table}, starting.")
        redshift.run(formatted_insert)        
        self.log.info("Copying data to Redshift table : {self.table}, complete!")
        


        




