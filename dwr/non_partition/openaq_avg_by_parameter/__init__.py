INPUT_TABLE = 'openaq_clean'
RESULT_TABLE = 'openaq_avg_by_parameter'

# language=HQL - create result table
create_result_table = """
    CREATE TABLE IF NOT EXISTS {0} (
        parameter STRING,
        unit STRING,
        average DOUBLE
    )
"""

# language=HQL - average by parameter, unit
average_by_parameter_sql = """
    INSERT OVERWRITE TABLE {result_table}
    SELECT
        parameter, 
        unit, 
        AVG(value) average
    FROM {input_table}
    GROUP BY parameter, unit
"""
