from dwr import *
from dwr.non_partition.query21 import *

SAMPLE_TYPE = SENATE

# language=HQL - get statistic from data
hiveql.execute("""
    SELECT
        w_warehouse_name || '_' || i_item_id,
        COUNT(*)
    FROM inventory,
        warehouse,
        item    
    WHERE i_item_sk = inv_item_sk
        AND inv_warehouse_sk = w_warehouse_sk
    GROUP BY w_warehouse_name,
        i_item_id
""")
frequency = {r[0]: r[1] for r in hiveql.fetchall()}

for sample_rate in sample_rates:
    logging.info("Sample rate: {0}".format(sample_rate))

    # senate sample
    sample_input_table = senate_sample(
        sample_rate=sample_rate,
        table_name=INVENTORY,
        frequency=frequency,
        # language=HQL
        select="""
            SELECT *
            FROM inventory,
                warehouse,
                item                
            LEFT JOIN senate_allocation
                On  senate_allocation.table_name = 'inventory'
                AND senate_allocation.ds = '~'                
                AND senate_allocation.default_rate = {sample_rate}
                AND senate_allocation.stratum = w_warehouse_name || '_' || i_item_id
            WHERE i_item_sk = inv_item_sk
                AND inv_warehouse_sk = w_warehouse_sk                                
                AND Rand() <= COALESCE(sample_rate, '{sample_rate}')                 
        """.format(
            sample_rate=sample_rate,
        ),
    )

    logging.info('Create result table')
    sample_result_table = sample_table_name(RESULT_TABLE, SAMPLE_TYPE, sample_rate)
    hiveql.execute(create_result_table.format(sample_result_table))

    logging.info('Run query over sampled tables')
    hiveql.execute(query21.format(
        table_name=sample_result_table,
        inventory=sample_input_table,
    ))

    sample_evaluate(
        table_name=RESULT_TABLE,
        sample_type=SAMPLE_TYPE,
        sample_rate=sample_rate,
        group_by_columns=['w_warehouse_name', 'i_item_id'],
        aggregation_columns=['inv_before', 'inv_after'],
    )
