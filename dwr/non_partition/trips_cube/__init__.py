INPUT_TABLE = 'trips_3_year_subscribers'
RESULT_TABLE = 'trips_cube'

DROP = True

AGG = "tripduration"
GROUP_BY = ['from_station_id', 'year']

# language=HQL - create result table
create_result_table = """
    CREATE TABLE IF NOT EXISTS {0} (
        from_station_id INT,
        year INT,
        sum_value DOUBLE
    )
"""

# language=HQL - average by parameter, unit
cube_sql = """
    INSERT OVERWRITE TABLE {result_table}
    SELECT
        (case when from_station_id is not null then from_station_id else 0 end),
        (case when year is not null then year else 0 end),
        SUM(tripduration)
    FROM {input_table}
    GROUP BY from_station_id, year WITH CUBE
"""

cube_sample = """
    INSERT OVERWRITE TABLE {result_table}
    SELECT
        (case when from_station_id is not null then from_station_id else 0 end),
        (case when year is not null then year else 0 end),
        SUM(tripduration/sample_rate)
    FROM {input_table}
    GROUP BY from_station_id, year WITH CUBE
"""

