INPUT_TABLE = 'openaq_10x'
RESULT_TABLE = 'openaq_sasg'

DROP = True

# language=HQL - create result table
create_result_table = """
    CREATE TABLE IF NOT EXISTS {0} (
        country STRING,
        parameter STRING,
        unit STRING,
        average DOUBLE
    )
"""


# language=HQL - average by country parameter, unit
sasg = """
    INSERT OVERWRITE TABLE {result_table}
    SELECT
        country,
        parameter, 
        unit, 
        AVG(value) average
    FROM {input_table}
    GROUP BY country, parameter, unit
"""

sasg_ss = """
    INSERT OVERWRITE TABLE {result_table}
    SELECT
        country,
        parameter, 
        unit, 
        SUM(value/sample_rate) / SUM(1/sample_rate) average
    FROM {input_table}
    GROUP BY country, parameter, unit
"""