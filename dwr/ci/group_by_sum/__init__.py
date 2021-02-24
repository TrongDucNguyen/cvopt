INPUT_TABLE = 'openaq_clean'
RESULT_TABLE = 'openaq_sum_by_parameter'

# language=HQL - create result table
create_result_table = """
    CREATE TABLE IF NOT EXISTS {0} (
        country STRING,
        parameter STRING,
        unit STRING,
        sum BIGINT
    )
"""

# language=HQL - sum by country, parameter, unit
sum_by_parameter_sql = """
    INSERT OVERWRITE TABLE {result_table}
    SELECT
        country,
        parameter, 
        unit, 
        SUM(value) average
    FROM {input_table}
    GROUP BY country, parameter, unit
"""

# language=HQL
sum_by_parameter_sample = """
    INSERT OVERWRITE TABLE {result_table}
    SELECT
        country,
        parameter, 
        unit,
        SUM(value / sample_rate) average
    FROM {input_table}
    GROUP BY country, parameter, unit
"""