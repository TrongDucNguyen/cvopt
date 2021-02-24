PREDICATE = "18"

INPUT_TABLE = 'openaq_clean'
RESULT_TABLE = 'openaq_sasg_sel_'+PREDICATE



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
    WHERE HOUR(regexp_replace(local_time,'T',' ')) < {predicate}
    GROUP BY country, parameter, unit
"""
