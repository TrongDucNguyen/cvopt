# ----------------------------------------------------
# OpenAQ - SASG (Single Aggregation Single Group-By)
# ----------------------------------------------------


class sasg:

    def __init__(self, aggr, col, grp):
        self.aggr = aggr
        self.col = col
        self.grp = grp

    # language=HQL - create result table
    def create_result_table_sql(self, sample_table):
        fields: str = ""
        for g in self.grp.keys():
            fields += g + " " + self.grp.get(g) + ", "

        sql = """
            CREATE TABLE IF NOT EXISTS {0} (
                {1}
                aggr DOUBLE
            )
            PARTITIONED BY (ds STRING)
        """.format(sample_table, fields)
        return sql

    # language=HQL - perform aggregation over input_table, and write to result_table
    def sasg_sql(self, ds, input_table_name, result_table_name):
        fields = ""
        for g in self.grp.keys():
            fields += g + ", "

        sql = """
            INSERT OVERWRITE TABLE {result_table} PARTITION (ds = '{ds}')
            SELECT
                {field1}
                {aggr}({col}) as aggr
            FROM {input_table}
            WHERE ds = '{ds}'
            GROUP BY {field2}
            
        """.format(
                ds=ds,
                input_table=input_table_name,
                result_table=result_table_name,
                field1=fields,
                field2=fields[:-2],
                aggr=self.aggr,
                col=self.col
            )

        return sql
