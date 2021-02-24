PREDICATE = "18"

INPUT_TABLE = 'openaq_clean'
RESULT_TABLE = 'openaq_masg_sel_'+PREDICATE

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
    WHERE HOUR(regexp_replace(local_time,'T',' ')) < {predicate}
    GROUP BY country, parameter, unit
"""

