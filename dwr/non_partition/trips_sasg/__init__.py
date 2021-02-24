INPUT_TABLE = 'trips_3_year_subscribers'
RESULT_TABLE = 'trips_sasg'

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
    GROUP BY from_station_id
"""

sasg_ss = """
    INSERT OVERWRITE TABLE {result_table}
    SELECT
        from_station_id,
        SUM(tripduration/sample_rate) / SUM(1/sample_rate) average
    FROM {input_table}
    GROUP BY from_station_id
"""