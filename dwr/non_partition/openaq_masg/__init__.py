INPUT_TABLE = 'openaq_clean'
RESULT_TABLE = 'openaq_masg'

# language=HQL - create result table
create_result_table = """
    CREATE TABLE IF NOT EXISTS {0} (
        country STRING,
        parameter STRING,
        unit STRING,
        count BIGINT,
        sum BIGINT,
        avg DOUBLE,
        stddev DOUBLE
    )
"""

# language=HQL - average by parameter, unit
masg = """
    INSERT OVERWRITE TABLE {result_table}
    SELECT
        country,
        parameter,
        unit,
        COUNT(value),
        SUM(value),
        AVG(value),        
        STDDEV(value)
    FROM {input_table}
    GROUP BY country, parameter, unit
"""

# language=HQL - average by parameter, unit
masg_sampled = """
    INSERT OVERWRITE TABLE {result_table}
    SELECT
        country,
        parameter,
        unit,
        SUM(1 / sample_rate),
        SUM(value / sample_rate),
        AVG(value),
        STDDEV(value)
    FROM {input_table}
    GROUP BY country, parameter, unit
"""
