import importlib
from abc import ABC, abstractmethod

from google.cloud import bigquery

BQ_CLIENT = bigquery.Client()
DATASET = "Palma"


class Taboola(ABC):
    @property
    @abstractmethod
    def endpoint(self):
        pass

    @property
    @abstractmethod
    def keys(self):
        pass

    @property
    @abstractmethod
    def schema(self):
        pass

    @property
    @abstractmethod
    def getter(self):
        pass

    @property
    @abstractmethod
    def transformer(self):
        pass

    @staticmethod
    def factory(table, start, end):
        try:
            module = importlib.import_module(f"models.{table}")
            model = getattr(module, table)
            return model(start, end)
        except (ImportError, AttributeError):
            raise ValueError(table)

    def __init__(self, start, end):
        self.table = self.__class__.__name__
        self._getter = self.getter(start, end, self.endpoint)
        self._transformer = self.transformer(self.schema)

    def _load(self, rows):
        output_rows = (
            BQ_CLIENT.load_table_from_json(
                rows,
                f"{DATASET}._stage_{self.table}",
                job_config=bigquery.LoadJobConfig(
                    create_disposition="CREATE_IF_NEEDED",
                    write_disposition="WRITE_APPEND",
                    schema=self.schema,
                ),
            )
            .result()
            .output_rows
        )
        self._update()
        return output_rows

    def _update(self):
        query = f"""
        CREATE OR REPLACE TABLE `{DATASET}`.`{self.table}` AS
        SELECT * EXCEPT (`row_num`) FROM
        (
            SELECT
                *,
                ROW_NUMBER() over (
                    PARTITION BY {','.join(self.keys['p_key'])}
                    ORDER BY {self.keys['incre_key']} DESC) AS `row_num`
            FROM `{DATASET}`.`_stage_{self.table}`
            )
        WHERE
            `row_num` = 1
        """
        BQ_CLIENT.query(query).result()

    def run(self):
        results = self._getter.get()
        response = {
            "table": self.table,
            "start": self._getter.start.isoformat(timespec='seconds'),
            "end": self._getter.end.isoformat(timespec='seconds'),
        }
        if results:
            rows = self._transformer.transform(results)
            response = {
                **response,
                "num_processed": len(rows),
                "output_rows": self._load(rows),
            }
        return response
