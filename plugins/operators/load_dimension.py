from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    
    # Insert Statement
    copy_sql = """
        INSERT INTO {}
        {}
    """
    # Apply default params specified in dag.py, params pass to function as **kwargs
    @apply_defaults
    # Define operator parameters
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 insert_statements="",
                 append=""
                 *args, **kwargs):
        
        # map init arguments to attributes
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.insert_statements = insert_statements
        self.append = append

    def execute(self, context):
        #connect to redshift
        self.log.info("Connecting to Redshift")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
            
        # formating 'copy_sql' statement defined above
        formatted_insert = LoadDimensionOperator.copy_sql.format(
                self.table,
                self.insert_statements
            )
        
        ## Perform truncate operation if append is false
        if self.append == False:
            self.log.info("Performing Truncate then Delete Operation") 
            
            self.log.info("Clearing data from destination Redshift table")
            redshift.run("DELETE FROM {}".format(self.table))


            self.log.info("Inserting data to Redshift table : {self.table}, starting.")
            redshift.run(formatted_insert)        
            self.log.info("Inserting data to Redshift table : {self.table}, finished.")
         
        if self.append == True:
            self.log.info("Performing Append Operation") 
           
            self.log.info("Inserting data to Redshift table : {self.table}, starting.")
            redshift.run(formatted_insert)        
            self.log.info("Inserting data to Redshift table : {self.table}, finished!")            
       
        
