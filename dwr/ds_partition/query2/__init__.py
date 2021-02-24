# ds list and sample rate parameters
ds_list = ['2018-08-01']

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
