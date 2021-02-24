INPUT_TABLE = 'trips_3_year_subscribers'
RESULT_TABLE = 'trips_mamg'

DROP = True

AGG = ['age', 'tripduration']
GROUP_BY = ['from_station_id', 'year']

# language=HQL - create result table
create_result_table = """
    CREATE TABLE IF NOT EXISTS {0} (
        from_station_id INT,
        year INT,
        agg1 DOUBLE,
        agg2 DOUBLE
    )
"""

# language=HQL - average by parameter, unit
mamg = """
    INSERT OVERWRITE TABLE {result_table}
    SELECT
        (case when from_station_id is not null then from_station_id else 0 end),
        (case when year is not null then year else 0 end),
        SUM(age) agg1,
        SUM(tripduration) agg2
    FROM {input_table}
    GROUP BY from_station_id, year WITH CUBE
"""

mamg_s = """
    INSERT OVERWRITE TABLE {result_table}
    SELECT
        (case when from_station_id is not null then from_station_id else 0 end),
        (case when year is not null then year else 0 end),
        SUM(age/sample_rate) agg1,
        SUM(tripduration/sample_rate) agg2
    FROM {input_table}
    GROUP BY from_station_id, year WITH CUBE
"""

