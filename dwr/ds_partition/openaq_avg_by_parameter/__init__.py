INPUT_TABLE = 'openaq_clean'
RESULT_TABLE = 'openaq_clean_ds_avg_by_parameter'

# ds list and sample rate parameters
# start_date = datetime.strptime('2017-01-01', '%Y-%m-%d').date()
# end_date = datetime.strptime('2017-12-31', '%Y-%m-%d').date()
# day_count = (end_date - start_date).days + 1
# ds_list = [d.strftime('%Y-%m-%d') for d in (start_date + timedelta(n) for n in range(day_count))]

# language=HQL - create result table
create_result_table = """
    CREATE TABLE IF NOT EXISTS {0} (
        parameter STRING,
        unit STRING,
        average DOUBLE
    )
    PARTITIONED BY (ds STRING)
"""

# language=HQL - average by parameter, unit
average_by_parameter_sql = """
    INSERT OVERWRITE TABLE {result_table} PARTITION (ds = '{ds}')
    SELECT
        parameter, 
        unit, 
        AVG(value) average
    FROM {input_table} 
    WHERE ds = '{ds}'
    GROUP BY parameter, unit
"""

# language=HQL - average by country, parameter, unit
average_by_country_sql = """
  SELECT
    country,
    parameter, 
    unit, 
    ds, 
    AVG(value) average
  FROM openaq 
  WHERE ds = '{ds}'
  GROUP BY country, parameter, unit
"""
