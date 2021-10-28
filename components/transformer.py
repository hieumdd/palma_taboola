from abc import ABCMeta, abstractmethod


class Transformer(metaclass=ABCMeta):
    def __init__(self, schema):
        self.schema = schema

    @abstractmethod
    def transform(self):
        pass


class SingleDayTransformer(Transformer):
    def transform(self, rows):
        rows = [self._transform_one(i) for i in rows]
        rows = [item for sublist in rows for item in sublist]
        return rows

    def _transform_one(self, result):
        rows = result["results"]
        return [
            {
                **{
                    i: row[i]
                    for i in [
                        i["name"]
                        for i in self.schema
                        if i["name"]
                        not in [
                            "start_date",
                            "end_date",
                            "last_used_rawdata_update_time",
                        ]
                    ]
                },
                "start_date": result["metadata"]["start_date"],
                "end_date": result["metadata"]["end_date"],
                "last_used_rawdata_update_time": result[
                    "last-used-rawdata-update-time"
                ],
            }
            for row in rows
        ]


class MultiDayTransformer(Transformer):
    def transform(self, results):
        rows = results["results"]
        return [
            {
                **{
                    i: row[i]
                    for i in [
                        i["name"]
                        for i in self.schema
                        if i["name"]
                        not in [
                            "last_used_rawdata_update_time",
                        ]
                    ]
                },
                "last_used_rawdata_update_time": results[
                    "last-used-rawdata-update-time"
                ],
            }
            for row in rows
        ]

class CampaignFilterTransformer(SingleDayTransformer):
    def _transform_one(self, result):
        rows = result["results"]
        return [
            {
                **{
                    i: row[i]
                    for i in [
                        i["name"]
                        for i in self.schema
                        if i["name"]
                        not in [
                            "start_date",
                            "end_date",
                            "campaign",
                            "last_used_rawdata_update_time",
                        ]
                    ]
                },
                "campaign": result["campaign"],
                "start_date": result["metadata"]["start_date"],
                "end_date": result["metadata"]["end_date"],
                "last_used_rawdata_update_time": result[
                    "last-used-rawdata-update-time"
                ],
            } if row else {}
            for row in rows
        ]
