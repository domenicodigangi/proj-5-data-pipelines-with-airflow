from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.secrets.metastore import MetastoreBackend


class StageToRedshiftOperator(BaseOperator):
    ui_color = "#358140"

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id: str = "",
        s3_path: str = "",
        table_name: str = "",
        *args,
        **kwargs,
    ):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.conn_id = redshift_conn_id
        self.s3_path = s3_path
        self.table_name = table_name

        self.copy_sql = f"""
            COPY {self.table_name}
            FROM '{self.s3_path}'
            ACCESS_KEY_ID '{{}}'
            SECRET_ACCESS_KEY '{{}}'
            IGNOREHEADER 1
            DELIMITER ','
            """

    def execute(self, context):
        metastoreBackend = MetastoreBackend()
        aws_connection = metastoreBackend.get_connection("aws_credentials")
        redshift = PostgresHook(postgres_conn_id=self.conn_id)

        self.log.info("Clearing data from destination Redshift table")
        redshift.run(f"DELETE FROM {self.table_name}")

        self.log.info("Copying data from S3 to Redshift")
        formatted_sql = self.copy_sql.format(
            aws_connection.login, aws_connection.password
        )
        redshift.run(formatted_sql)
