from dwr import *
from dwr.ds_partition.query26 import *

SAMPLE_TYPE = VOILA

for DS in ds_list:
    logging.info("DS: {0} - Get statistic".format(DS))
    # language=HQL
    hiveql.execute("""
            SELECT
               i_item_id,
               COUNT(*),
                ABS(STDDEV(cs_quantity) / AVG(cs_quantity)),
                ABS(STDDEV(cs_list_price) / AVG(cs_list_price)),
                ABS(STDDEV(cs_coupon_amt) / AVG(cs_coupon_amt)),
                ABS(STDDEV(cs_sales_price) / AVG(cs_sales_price)) 

            FROM catalog_sales
            JOIN item
            ON catalog_sales.cs_item_sk = i_item_sk
            WHERE catalog_sales.ds = '{0}'
            AND item.ds = '{0}'
            GROUP BY i_item_id
        """.format(DS))
    result = hiveql.fetchall()
    catalog_sales_count = {r[0]: r[1] for r in result}
    catalog_sales_coeff = {r[0]: r[2:] for r in result}


    for sample_rate in sample_rates:
        logging.info("DS: {0}        sample rate: {1}".format(DS, sample_rate))
        # senate sample for catalog sale
        catalog_sales_sampled = masg_sample(
            sample_rate=sample_rate,
            table_name=CATALOG_SALES,
            group_by=['i_item_id'],
            aggregate_column=['agg1', 'agg2', 'agg3', 'agg4'],
            partition={'ds': DS},
            frequency=catalog_sales_count,
            coefficient=catalog_sales_coeff,
            # language=HQL
            select="""
                SELECT *                
                FROM {table_name} full_table
                JOIN item
                    ON full_table.cs_item_sk = i_item_sk 
                    AND item.ds='{ds}'
                LEFT JOIN voila_allocation
                    ON voila_allocation.ds = '{ds}'
                    AND voila_allocation.table_name = '{table_name}'
                    AND voila_allocation.default_rate = {sample_rate}
                    AND item.i_item_id = voila_allocation.stratum                          
                WHERE full_table.ds = '{ds}'                               
                AND rand() <= COALESCE(sample_rate, '{sample_rate}')                 
            """.format(
                ds=DS,
                table_name=CATALOG_SALES,
                sample_rate=sample_rate,
            ),
            overwrite= True
        )


        # create result table
        hiveql.execute(query26_create_table.format(sample_table_name('query26', SAMPLE_TYPE, sample_rate, 'ds')))

        # run query over sample table
        hiveql.execute(query26_sampled.format(
            ds=DS,
            query26_table=sample_table_name('query26', SAMPLE_TYPE, sample_rate, 'ds'),
            catalog_sales_table=catalog_sales_sampled,
        ))

        sample_evaluate(
            table_name='query26',
            sample_type=SAMPLE_TYPE,
            sample_rate=sample_rate,
            group_by_columns=['i_item_id'],
            aggregation_columns=['agg1','agg2','agg3','agg4'],
            partition={'ds': DS},
        )
