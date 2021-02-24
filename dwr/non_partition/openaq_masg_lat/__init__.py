INPUT_TABLE = 'openaq_clean'
RESULT_TABLE = 'openaq_masg_lat'

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

# language=HQL - average by attribute 1 & 2
masg = """
    INSERT OVERWRITE TABLE {result_table}
    SELECT
        country,
        parameter,
        unit,
        AVG(value),        
        AVG(latitude + 180) 
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
        AVG(value),        
        AVG(latitude + 180) 
    FROM {input_table}
    GROUP BY country, parameter, unit
"""

masg_ss = """
    INSERT OVERWRITE TABLE {result_table}
    SELECT
        country,
        parameter,
        unit,
        SUM(value/sample_rate)/SUM(1/sample_rate),        
        SUM((latitude+180)/sample_rate)/SUM(1/sample_rate) 
    FROM {input_table}
    GROUP BY country, parameter, unit
"""