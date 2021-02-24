INPUT_TABLE = 'trips_3_year_subscribers'
RESULT_TABLE = 'trips_masg'

DROP = True

# language=HQL - create result table
create_result_table = """
    CREATE TABLE IF NOT EXISTS {0} (
        from_station_id INT,
        agg1 DOUBLE,
        agg2 DOUBLE
    )
"""

# language=HQL - average by attribute 1 & 2
masg = """
    INSERT OVERWRITE TABLE {result_table}
    SELECT
        from_station_id,
        AVG(age) agg1,
        AVG(tripduration) agg2
    FROM {input_table}
    GROUP BY from_station_id
"""

masg_sampled = """
    INSERT OVERWRITE TABLE {result_table}
    SELECT
        from_station_id,
        AVG(age) agg1,
        AVG(tripduration) agg2
    FROM {input_table}
    GROUP BY from_station_id
"""

masg_ss = """
    INSERT OVERWRITE TABLE {result_table}
    SELECT
        from_station_id,
        SUM(age/sample_rate)/SUM(1/sample_rate) agg1,
        SUM(tripduration/sample_rate)/SUM(1/sample_rate) agg2
    FROM {input_table}
    GROUP BY from_station_id
"""