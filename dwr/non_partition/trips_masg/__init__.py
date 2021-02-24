INPUT_TABLE = 'trips_clean'
RESULT_TABLE = 'trips_masg'

# language=HQL - create result table
create_result_table = """
    CREATE TABLE IF NOT EXISTS {0} (
        from_station_id INT,
        usertype STRING,
        cnt int,
        average DOUBLE
    )
"""

# language=HQL - average by attribute 1 & 2
masg = """
     INSERT OVERWRITE TABLE {result_table}
    SELECT
        from_station_id,
        usertype,
        COUNT(tripduration) cnt,
        AVG(tripduration) average
    FROM {input_table}
    GROUP BY from_station_id,usertype
"""

# language=HQL - average by parameter, unit
masg_sampled = """
    INSERT OVERWRITE TABLE {result_table}
     SELECT 
      from_station_id,
        usertype,
        SUM(1 / sample_rate) cnt,
        AVG(tripduration) average
    FROM {input_table}
    GROUP BY from_station_id,usertype
"""
