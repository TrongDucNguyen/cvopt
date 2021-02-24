INPUT_TABLE = 'openaq_clean'
RESULT_TABLE = 'openaq_mamg'

DROP = True

AGG = ['value', 'latitude']
GROUP_BY = ['country', 'parameter']

# language=HQL - create result table
create_result_table = """
    CREATE TABLE IF NOT EXISTS {0} (
        country STRING,
        parameter STRING,
        agg1 DOUBLE,
        agg2 DOUBLE
    )
"""

# language=HQL - average by parameter, unit
mamg = """
    INSERT OVERWRITE TABLE {result_table}
    SELECT
        (case when country is not null then country else 'null' end),
        (case when parameter is not null then parameter else 'null' end),
        SUM(value) agg1,
        SUM(latitude+180) agg2
    FROM {input_table}
    GROUP BY country, parameter WITH CUBE
"""

mamg_s = """
    INSERT OVERWRITE TABLE {result_table}
    SELECT
        (case when country is not null then country else 'null' end),
        (case when parameter is not null then parameter else 'null' end),
        SUM(value/sample_rate) agg1,
        SUM((latitude+180)/sample_rate) agg2
    FROM {input_table}
    GROUP BY country, parameter WITH CUBE
"""

