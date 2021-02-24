from dwr import *
from dwr.ds_partition.query3 import *

SAMPLE_TYPE = VOILA

for DS in ds_list:
    logging.info("DS: {0} - Get statistic".format(DS))

    # language=HQL
    hiveql.execute("""
        SELECT
            CONCAT(COALESCE(dt.d_year,'~'),'_',COALESCE(item.i_brand_id,'~'),'_',COALESCE(item.i_brand,'~')),
            COUNT(*),
            STDDEV(ss_ext_discount_amt) / AVG(ss_ext_discount_amt)
         FROM store_sales,item,date_dim dt
                WHERE store_sales.ds = '{0}' 
                 AND dt.d_date_sk = store_sales.ss_sold_date_sk 
                 AND store_sales.ss_item_sk = item.i_item_sk 
                 AND ss_ext_discount_amt<>0
                GROUP BY d_year,i_brand_id,i_brand
    """.format(DS))
    result = hiveql.fetchall()
    store_sales_count = {r[0]: r[1] for r in result}
    store_sales_coeff = {r[0]: r[2] for r in result}

    for sample_rate in sample_rates:
        logging.info("DS: {0}        sample rate: {1}".format(DS, sample_rate))
        # check if result is available
        if exists(
                table_name='sample_error',
                partition={'ds': DS, 'table_name': 'query3', 'sample_type': SAMPLE_TYPE, 'sample_rate': sample_rate},
        ):
            continue

        # voila sample for store sale
        store_sales_sampled = voila_sample(
            sample_rate=sample_rate,
            table_name=STORE_SALES,
            group_by=['d_year','i_brand_id','i_brand'],
            partition={'ds': DS},
            frequency=store_sales_count,
            coefficient=store_sales_coeff,
            # language=HQL
            select="""
                SELECT *
                FROM {table_name} full_table,item,date_dim dt,voila_allocation
                    WHERE full_table.ds = '{ds}' 
                     AND dt.d_date_sk = full_table.ss_sold_date_sk 
                     AND full_table.ss_item_sk = item.i_item_sk 
                     AND voila_allocation.ds = '{ds}'
                     AND voila_allocation.table_name = '{table_name}'
                     AND voila_allocation.default_rate = '{sample_rate}'
                     AND  CONCAT(COALESCE(dt.d_year,'~'),'_',COALESCE(item.i_brand_id,'~'),'_',COALESCE(item.i_brand,'~')) = voila_allocation.stratum  
                     AND RAND() <= COALESCE(sample_rate, '{sample_rate}')              
            """.format(
                ds=DS,
                table_name=STORE_SALES,
                sample_rate=sample_rate,
            ),
        )

        if not exists(sample_table_name('query3', SAMPLE_TYPE, sample_rate), {'ds': DS}):
            # create result table
            hiveql.execute(query3_create_table.format(sample_table_name('query3', SAMPLE_TYPE, sample_rate, 'ds')))

            # run query over sample table
            hiveql.execute(query3_sampled.format(
                ds=DS,
                query3_table=sample_table_name('query3', SAMPLE_TYPE, sample_rate, 'ds'),
                store_sales_table=store_sales_sampled,
            ))

        sample_evaluate(
            table_name='query3',
            sample_type=SAMPLE_TYPE,
            sample_rate=sample_rate,
            group_by_columns=['d_year', 'i_brand_id', 'i_brand'],
            aggregation_columns=['sum_agg'],
            partition={'ds': DS},
        )
