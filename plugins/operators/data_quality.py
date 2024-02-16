from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):

    ui_color = "#89DA59"

    @apply_defaults
    def __init__(
        self, conn_id="", sql_test_case="", expected_result=None, *args, **kwargs
    ):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.sql_test_case = sql_test_case
        self.expected_result = expected_result

    def execute(self, context):
        redshift_hook = PostgresHook("redshift")
        records = redshift_hook.get_records(self.sql_test_case)
        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError(
                f"Data quality check failed as {self.sql_test_case} returned no results"
            )
        if records == self.expected_result:
            self.log.info(f"Data quality on {self.sql_test_case} check passed")
        else:
            self.log.info(f"Data quality on {self.sql_test_case} check failed")
            raise ValueError(f"Data quality on {self.sql_test_case} check failed")
