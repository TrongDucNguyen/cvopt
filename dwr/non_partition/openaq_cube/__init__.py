import itertools

INPUT_TABLE = 'openaq_clean'
RESULT_TABLE = 'openaq_cube'

GROUP_BY = ['country', 'parameter']

# language=HQL - create result table
create_result_table = """
    CREATE TABLE IF NOT EXISTS {0} (
        country STRING,
        parameter STRING,
        sum_value DOUBLE
    )
"""

# language=HQL - average by parameter, unit
cube_sql = """
    INSERT OVERWRITE TABLE {result_table}
    SELECT
        (case when country is not null then country else 'null' end),
        (case when parameter is not null then parameter else 'null' end),
        SUM(value)
    FROM {input_table}
    GROUP BY country, parameter WITH CUBE
"""

cube_sample = """
    INSERT OVERWRITE TABLE {result_table}
    SELECT
        (case when country is not null then country else 'null' end),
        (case when parameter is not null then parameter else 'null' end),
        SUM(value/sample_rate)
    FROM {input_table}
    GROUP BY country, parameter WITH CUBE
"""

