INPUT_TABLE = 'openaq_clean'
RESULT_TABLE = 'openaq_sasg_multiple'

DROP = False

# language=HQL - create result table
create_result_table = """
    CREATE TABLE IF NOT EXISTS {0} (
        country STRING,
        parameter STRING,
        unit STRING,
        agg DOUBLE
    )
"""


# language=HQL - average by country parameter, unit
query1 = """
    INSERT OVERWRITE TABLE {result_table}
    SELECT
        country,
        parameter, 
        unit, 
        AVG(value) average
    FROM {input_table}
    GROUP BY country, parameter, unit
"""
