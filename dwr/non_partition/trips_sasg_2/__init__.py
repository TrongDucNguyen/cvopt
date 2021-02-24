INPUT_TABLE = 'trips_clear'
RESULT_TABLE = 'trips_sasg_2'

# language=HQL - create result table
create_result_table = """
    CREATE TABLE IF NOT EXISTS {0} (
        from_station_id INT,
        usertype STRING,
        year int,
        average DOUBLE
    )
"""

# language=HQL - average by parameter, unit
sasg = """
    INSERT OVERWRITE TABLE {result_table}
    SELECT
        from_station_id,
        usertype,
        year,
        AVG(tripduration) average
    FROM {input_table}
    GROUP BY from_station_id,usertype,year
"""
