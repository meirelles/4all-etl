import logging

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.postgres_hook import PostgresHook
from operators.utils import (IntermediateRead, config, batch_iter)

log = logging.getLogger(__name__)


class LoadToDatabase(BaseOperator):
    """
    Load the results into Postgre
    """

    template_fields = ('input_ns', 'output_table', 'output_ds')

    @apply_defaults
    def __init__(self, input_ns, output_table, output_ds, *args, **kwargs):
        """
        Create a new LoadToDatabase instance.

        :param input_ns: IntermediateRead's namespace string
        :param output_table: output table name
        :param output_ds: formatted execution date and/or time to identify the source
        """

        super(LoadToDatabase, self).__init__(*args, **kwargs)
        self.input_ns = input_ns
        self.output_table = output_table
        self.output_ds = output_ds

    def execute(self, context):
        pg_hook = PostgresHook(postgres_conn_id=config()['postgres_main_conn'])

        # Clean past runs to guarantee idempotence
        sql = "DELETE FROM " + self.output_table + " WHERE ds = %s"
        pg_hook.run(sql, parameters=[self.output_ds])

        # The "%s" in this string is safe and processed as "?"
        sql = "INSERT INTO " + self.output_table + """
            (ds, latitude, longitude, rua, numero, bairro, cidade, cep, estado, pais)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        iread = IntermediateRead(self.input_ns)

        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        try:
            # For best performance, get the records in batch and push them at once.
            for records in batch_iter(iread):
                batch = list()

                for record in records:
                    parameters = [
                        self.output_ds,
                        record["lat"],
                        record["lng"],
                        record["address"]["street"],
                        record["address"]["number"],
                        record["address"]["neighborhood"],
                        record["address"]["city"],
                        record["address"]["zipcode"],
                        record["address"]["state"],
                        record["address"]["country"]
                    ]
                    batch.append(parameters)

                cursor.executemany(sql, batch)
                conn.commit()
        except Exception as e:
            log.info(e)
        finally:
            cursor.close()
            conn.close()
