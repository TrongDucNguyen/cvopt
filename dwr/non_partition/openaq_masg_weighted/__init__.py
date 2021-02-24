INPUT_TABLE = 'openaq_clean'
RESULT_TABLE = 'openaq_masg_2'

# language=HQL - create result table
create_result_table = """
    CREATE TABLE IF NOT EXISTS {0} (
        country STRING,
        parameter STRING,
        unit STRING,        
        agg1 DOUBLE,
        agg2 DOUBLE
    )
"""

# language=HQL - average by parameter, unit
masg_sampled = """
    INSERT OVERWRITE TABLE {result_table}
    SELECT
        country,
        parameter,
        unit,        
        AVG(value),
        AVG(latitude+180)
    FROM {input_table}
    GROUP BY country, parameter, unit
"""

# language=HQL - average by parameter, unit
masg = """
    INSERT OVERWRITE TABLE {result_table}
    SELECT
        country,
        parameter,
        unit,        
        AVG(value),        
        AVG(latitude+180)
    FROM {input_table}
    GROUP BY country, parameter, unit
"""