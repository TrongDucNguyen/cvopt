from dwr import *
from dwr.ds_partition.query3 import *

SAMPLE_TYPE = SENATE
OVERWRITE = False

for DS in ds_list:
    store_sales_count = None
    for sample_rate in sample_rates:
        logging.info("DS: {0}        sample rate: {1}".format(DS, sample_rate))

        if store_sales_count is None:
            # language=HQL
            hiveql.execute("""
                SELECT
                     CONCAT(COALESCE(dt.d_year,'~'),'_',COALESCE(item.i_brand_id,'~'),'_',COALESCE(item.i_brand,'~')),
                     COUNT(*)
                FROM store_sales,item,date_dim dt
                WHERE store_sales.ds = '{0}' 
                 AND dt.d_date_sk = store_sales.ss_sold_date_sk 
                 AND store_sales.ss_item_sk = item.i_item_sk
                 AND dt.d_year IS NOT NULL AND item.i_brand_id IS  NOT NULL AND item.i_brand IS NOT NULL
                GROUP BY d_year,i_brand_id,i_brand
            """.format(DS))
            store_sales_count = {r[0]: r[1] for r in hiveql.fetchall()}

        # senate sample for store sale
        store_sales_sampled = senate_sample(
            sample_rate=sample_rate,
            table_name=STORE_SALES,
            group_by=['d_year','i_brand_id','i_brand'],
            partition={'ds': DS},
            frequency=store_sales_count,
            # language=HQL
            select="""
                SELECT *
                FROM {table_name} full_table,item,date_dim dt,senate_allocation
                    WHERE full_table.ds = '{ds}' 
                     AND dt.d_date_sk = full_table.ss_sold_date_sk 
                     AND full_table.ss_item_sk = item.i_item_sk 
                     AND senate_allocation.ds = '{ds}'
                     AND senate_allocation.table_name = '{table_name}'
                     AND senate_allocation.default_rate = {sample_rate}
                     AND RAND() <= COALESCE(sample_rate, '{sample_rate}')                 
            """.format(
                ds=DS,
                table_name=STORE_SALES,
                sample_rate=sample_rate,
            ),
            overwrite=OVERWRITE,
        )


    if OVERWRITE or not exists(sample_table_name('query3', SAMPLE_TYPE, sample_rate), {'ds': DS}):
            # create result table
            hiveql.execute(query3_create_table.format(sample_table_name('query3', SAMPLE_TYPE, sample_rate,'ds')))

            # run query over sample table
            hiveql.execute(query3_sampled.format(
                ds=DS,
                query3_table=sample_table_name('query3', SAMPLE_TYPE, sample_rate,'ds'),
                store_sales_table=store_sales_sampled,
            ))

            sample_evaluate(
                table_name='query3',
                sample_type=SAMPLE_TYPE,
                sample_rate=sample_rate,
                group_by_columns=['d_year','i_brand_id','i_brand'],
                aggregation_columns=['sum_agg'],
                partition={'ds': DS},
            )
