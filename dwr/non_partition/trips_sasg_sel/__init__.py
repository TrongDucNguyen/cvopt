PREDICATE = "34"
# PREDICATE = 29 (25%), 34 (50%), 43 (75%)

INPUT_TABLE = 'trips_3_year_subscribers'
RESULT_TABLE = 'trips_sasg_sel_'+PREDICATE

DROP = True

# language=HQL - create result table
create_result_table = """
    CREATE TABLE IF NOT EXISTS {0} (
        from_station_id INT,
        average DOUBLE
    )
"""

# language=HQL - average by parameter, unit
sasg = """
    INSERT OVERWRITE TABLE {result_table}
    SELECT
        from_station_id,
        AVG(tripduration) average
    FROM {input_table}
    WHERE age < {predicate}
    GROUP BY from_station_id
"""
