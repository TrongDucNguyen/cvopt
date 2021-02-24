from dwr import *

INPUT_TABLE = 'openaq_clean'
RESULT_TABLE = 'openaq_clean_ds_avg_by_parameter'

# ds list and sample rate parameters
start_date = datetime.strptime('2017-01-01', '%Y-%m-%d').date()
end_date = datetime.strptime('2017-12-31', '%Y-%m-%d').date()
day_count = (end_date - start_date).days + 1
ds_list = [d.strftime('%Y-%m-%d') for d in (start_date + timedelta(n) for n in range(day_count))]

# language=HQL - create result table
create_result_table = """
    CREATE TABLE IF NOT EXISTS {0} (
        parameter STRING,
        unit STRING,
        average DOUBLE
    )
    PARTITIONED BY (ds STRING)
"""

# language=HQL - create result table with CI
create_result_table_with_ci = """
    CREATE TABLE IF NOT EXISTS {0} (
        parameter STRING,
        unit STRING,
        average DOUBLE,
        ci DOUBLE
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

# language=HQL - average by parameter, unit, with CI
average_by_parameter_sql_with_ci = """
    INSERT OVERWRITE TABLE {result_table} PARTITION (ds = '{ds}')
    SELECT
        parameter, 
        unit, 
        AVG(value) average,
        1.645 * STDDEV(value) / SQRT(COUNT(*)) ci
    FROM {input_table} 
    WHERE ds = '{ds}'
    GROUP BY parameter, unit
"""

logging.info('Create dynamic alloc table')
# language=HQL
hiveql.execute("""
    CREATE TABLE IF NOT EXISTS dynamic_allocation (                    
        sample_rate double
    )
    PARTITIONED BY (
        ds string,
        table_name string,
        sample_type string
    )
""")


def get_dynamic_allocation(ds, table_name, sample_type):
    """
    Get dynamic allocation.
    :param ds: ds
    :param table_name: table name
    :param sample_type: sample type
    :return: dynamic sample rate. None if the sample rate is not available
    """
    logging.info('Get dynamic {0} sample rate for table {1} on {2}'.format(sample_type, table_name, ds))
    # language=HQL
    hiveql.execute("""    
        SELECT sample_rate
        FROM dynamic_allocation
        WHERE ds = '{ds}' 
        AND table_name = '{table_name}'
        AND sample_type = '{sample_type}' 
    """.format(
        ds=ds,
        table_name=table_name,
        sample_type=sample_type,
    ))
    r = hiveql.fetchall()
    return r[len(r) - 1][0] if len(r) > 0 else None


def set_dynamic_allocation(ds, table_name, sample_type, sample_rate):
    """
    Set dynamic allocation.
    :param ds: ds
    :param table_name: table name
    :param sample_type: sample type
    :param sample_rate: sample rate
    :return: dynamic sample rate. None if the sample rate is not available
    """

    logging.info('Set {0} sample rate for table {1} on {2} as {3}'.format(sample_type, table_name, ds, sample_rate))
    # language=HQL - add new allocation
    hiveql.execute("""
        INSERT OVERWRITE TABLE dynamic_allocation PARTITION(
            ds = '{ds}',
            table_name =  '{table_name}',
            sample_type = '{sample_type}'
        )
        VALUES ('{sample_rate}')
    """.format(
        ds=ds,
        table_name=table_name,
        sample_type=sample_type,
        sample_rate=sample_rate,
    ))


# constant names
WEB_SALES = 'web_sales'
CATALOG_SALES = 'catalog_sales'

# language=HQL
query2_create_table = """
    CREATE TABLE IF NOT EXISTS {0} (
        d_week_seq1 int,
        sun double,
        mon double,
        tue double,
        wed double,
        thu double,
        fri double,
        sat double
    )
    PARTITIONED BY (
        ds string
    )
"""

# language=HQL
query2 = """  
  WITH wscs AS (
    SELECT sold_date_sk,
      sales_price
    FROM (
      SELECT ws_sold_date_sk sold_date_sk,
        ws_ext_sales_price sales_price
      FROM web_sales
      WHERE ds = '{ds}'
    ) x

    UNION ALL

    (
      SELECT cs_sold_date_sk sold_date_sk,
        cs_ext_sales_price sales_price
      FROM catalog_sales
      WHERE ds = '{ds}'
    )
  ),
  wswscs AS (
    SELECT 
      d_week_seq,
      SUM(CASE WHEN (d_day_name = 'Sunday')     THEN sales_price ELSE NULL END) sun_sales,
      SUM(CASE WHEN (d_day_name = 'Monday')     THEN sales_price ELSE NULL END) mon_sales,
      SUM(CASE WHEN (d_day_name = 'Tuesday')    THEN sales_price ELSE NULL END) tue_sales,
      SUM(CASE WHEN (d_day_name = 'Wednesday')  THEN sales_price ELSE NULL END) wed_sales,
      SUM(CASE WHEN (d_day_name = 'Thursday')   THEN sales_price ELSE NULL END) thu_sales,
      SUM(CASE WHEN (d_day_name = 'Friday')     THEN sales_price ELSE NULL END) fri_sales,
      SUM(CASE WHEN (d_day_name = 'Saturday')   THEN sales_price ELSE NULL END) sat_sales
    FROM wscs, DATE_DIM
    WHERE d_date_sk = sold_date_sk
    GROUP BY d_week_seq
  )
  INSERT OVERWRITE TABLE {query2_table} PARTITION (ds = '{ds}')
  SELECT d_week_seq1,
    ROUND(sun_sales1 / sun_sales2, 2) sun,
    ROUND(mon_sales1 / mon_sales2, 2) mon,
    ROUND(tue_sales1 / tue_sales2, 2) tue,
    ROUND(wed_sales1 / wed_sales2, 2) wed,
    ROUND(thu_sales1 / thu_sales2, 2) thu,
    ROUND(fri_sales1 / fri_sales2, 2) fri,
    ROUND(sat_sales1 / sat_sales2, 2) sat
  FROM 
    (
      SELECT wswscs.d_week_seq d_week_seq1,
        sun_sales sun_sales1,
        mon_sales mon_sales1,
        tue_sales tue_sales1,
        wed_sales wed_sales1,
        thu_sales thu_sales1,
        fri_sales fri_sales1,
        sat_sales sat_sales1
      FROM wswscs
      JOIN (
        SELECT DISTINCT
          d_week_seq,
          d_year
        FROM date_dim
        WHERE d_year = 2001
      ) date_dim
      ON date_dim.d_week_seq = wswscs.d_week_seq			
    ) y,
    (
      SELECT wswscs.d_week_seq d_week_seq2,
        sun_sales sun_sales2,
        mon_sales mon_sales2,
        tue_sales tue_sales2,
        wed_sales wed_sales2,
        thu_sales thu_sales2,
        fri_sales fri_sales2,
        sat_sales sat_sales2
      FROM wswscs
      JOIN (
        SELECT DISTINCT
          d_week_seq,
          d_year
        FROM date_dim				
        WHERE d_year = 2001 + 1
      ) date_dim
      ON date_dim.d_week_seq = wswscs.d_week_seq			
    ) z
  WHERE d_week_seq1 = d_week_seq2 - 53
  ORDER BY d_week_seq1
"""

# language=HQL
query2_sampled = """  
  WITH wscs AS (
    SELECT 
      sold_date_sk,
      sales_price,
      sample_rate
    FROM (
      SELECT 
        ws_sold_date_sk AS sold_date_sk,
        ws_ext_sales_price AS sales_price,
        sample_rate
      FROM {web_sales_table}
      WHERE ds = '{ds}'
    ) x

    UNION ALL

    (
      SELECT 
        cs_sold_date_sk AS sold_date_sk,
        cs_ext_sales_price AS sales_price,
        sample_rate
      FROM {catalog_sales_table}
      WHERE ds = '{ds}'
    )
  ),
  wswscs AS (
    SELECT 
      d_week_seq,
      SUM(CASE WHEN (d_day_name = 'Sunday')    THEN sales_price / sample_rate ELSE NULL END) sun_sales,
      SUM(CASE WHEN (d_day_name = 'Monday')    THEN sales_price / sample_rate ELSE NULL END) mon_sales,
      SUM(CASE WHEN (d_day_name = 'Tuesday')   THEN sales_price / sample_rate ELSE NULL END) tue_sales,
      SUM(CASE WHEN (d_day_name = 'Wednesday') THEN sales_price / sample_rate ELSE NULL END) wed_sales,
      SUM(CASE WHEN (d_day_name = 'Thursday')  THEN sales_price / sample_rate ELSE NULL END) thu_sales,
      SUM(CASE WHEN (d_day_name = 'Friday')    THEN sales_price / sample_rate ELSE NULL END) fri_sales,
      SUM(CASE WHEN (d_day_name = 'Saturday')  THEN sales_price / sample_rate ELSE NULL END) sat_sales
    FROM wscs, DATE_DIM
    WHERE d_date_sk = sold_date_sk
    GROUP BY d_week_seq
  )
  INSERT OVERWRITE TABLE {query2_table} PARTITION (ds = '{ds}')
  SELECT d_week_seq1,
    ROUND(sun_sales1 / sun_sales2, 2) sun,
    ROUND(mon_sales1 / mon_sales2, 2) mon,
    ROUND(tue_sales1 / tue_sales2, 2) tue,
    ROUND(wed_sales1 / wed_sales2, 2) wed,
    ROUND(thu_sales1 / thu_sales2, 2) thu,
    ROUND(fri_sales1 / fri_sales2, 2) fri,
    ROUND(sat_sales1 / sat_sales2, 2) sat
  FROM 
    (
      SELECT wswscs.d_week_seq d_week_seq1,
        sun_sales sun_sales1,
        mon_sales mon_sales1,
        tue_sales tue_sales1,
        wed_sales wed_sales1,
        thu_sales thu_sales1,
        fri_sales fri_sales1,
        sat_sales sat_sales1
      FROM wswscs
      JOIN (
        SELECT DISTINCT
          d_week_seq,
          d_year
        FROM date_dim
        WHERE d_year = 2001
      ) date_dim
      ON date_dim.d_week_seq = wswscs.d_week_seq			
    ) y,
    (
      SELECT wswscs.d_week_seq d_week_seq2,
        sun_sales sun_sales2,
        mon_sales mon_sales2,
        tue_sales tue_sales2,
        wed_sales wed_sales2,
        thu_sales thu_sales2,
        fri_sales fri_sales2,
        sat_sales sat_sales2
      FROM wswscs
      JOIN (
        SELECT DISTINCT
          d_week_seq,
          d_year
        FROM date_dim				
        WHERE d_year = 2001 + 1
      ) date_dim
      ON date_dim.d_week_seq = wswscs.d_week_seq			
    ) z
  WHERE d_week_seq1 = d_week_seq2 - 53
  ORDER BY d_week_seq1
"""