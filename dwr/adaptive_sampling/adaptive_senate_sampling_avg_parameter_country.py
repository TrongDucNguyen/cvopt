from dwr import *
from dwr.ds_partition.openaq_avg_by_parameter import *

# Expect to have average of the error less than 10% error
EPSILON = 0.05

SAMPLE_TYPE = 'adaptive'
sample_table = sample_table_name(INPUT_TABLE, SAMPLE_TYPE)
sample_result_table = sample_table_name(RESULT_TABLE, SAMPLE_TYPE)

# language=HQL - Create voila adaptive table
hiveql.execute("""
    CREATE TABLE IF NOT EXISTS adaptive_allocation (
        stratum string,
        sample_rate double 
    )        
    PARTITIONED BY (
        ds string,
        table_name string
    )
""")

# create result table
logging.info('create result table')
hiveql.execute(create_result_table.format(sample_result_table))

# get full table's schema
schema_column, partition_column = schema(INPUT_TABLE)

logging.info('Create sampled table')
# language=HQL - Create sampled table
hiveql.execute("""
    CREATE TABLE IF NOT EXISTS {table_name} (
        {schema},
        sample_rate double
    )
    PARTITIONED BY (ds string)
""".format(
    table_name=sample_table,
    schema=','.join('{0} {1}'.format(i, schema_column[i]) for i in schema_column if i not in partition_column),
))

for ds in ds_list:
    logging.info("DS: {0}".format(ds))

    logging.info('get old sample rate')
    # language=HQL
    hiveql.execute("""
        SELECT stratum,
               sample_rate
        FROM adaptive_allocation
        WHERE ds = '{ds}'
        AND table_name = '{table_name}' 
    """.format(
        ds=yesterday(ds),
        table_name=RESULT_TABLE,
    ))
    alloc = hiveql.fetchall()
    if len(alloc) > 1:
        alloc = {r[0]: r[1] for r in alloc}
    else:
        alloc = {}
    logging.info("Previous alloc: {0}".format(alloc))

    logging.info('Get error from sample_error')
    # language=HQL
    hiveql.execute("""
        SELECT attribute,
               error
        FROM sample_error
        WHERE ds = '{ds}'
        AND table_name = '{table_name}'
        AND sample_type = '{sample_type}'
        AND sample_rate = '{sample_rate}'             
    """.format(
        ds=yesterday(ds),
        table_name=RESULT_TABLE,
        sample_type=SAMPLE_TYPE,
        sample_rate=0.1,
    ))
    error = hiveql.fetchall()
    if len(error) > 1:
        error = {r[0]: r[1] for r in error}
    else:
        error = {}
    logging.info("Previous error: {0}".format(error))

    # adaptive allocation
    # alloc = {i: max(0.01, min(1.0, alloc.get(i, 0.1) * (1.0 + error[i] - EPSILON))) for i in error}
    alloc = {i: max(0.01, min(1.0, alloc.get(i, 0.1) + error[i] - EPSILON)) for i in error}

    # language=HQL - insert new allocation
    if len(alloc) > 0:
        hiveql.execute("""
            INSERT OVERWRITE TABLE adaptive_allocation 
            PARTITION(ds='{ds}', table_name='{table_name}')
            VALUES {values} 
        """.format(
            ds=ds,
            table_name=RESULT_TABLE,
            values=', '.join(["('{0}', {1})".format(k, alloc[k]) for k in alloc]),
        ))

    logging.info('Draw sample')
    # language=HQL
    hiveql.execute("""
        INSERT OVERWRITE TABLE {table_name} PARTITION (ds = '{ds}')
        SELECT 
            {schema},
            COALESCE(sample_rate, '{default_sample_rate}') AS sample_rate
        FROM {full_table} full_table        
        LEFT JOIN adaptive_allocation alloc
            ON alloc.ds = '{ds}'
            AND alloc.table_name = '{table_name}'
            AND alloc.stratum = full_table.parameter || '_' || full_table.unit            
        WHERE full_table.ds = '{ds}'
        AND RAND() <= COALESCE(sample_rate, '{default_sample_rate}')
    """.format(
        table_name=sample_table,
        full_table=INPUT_TABLE,
        ds=ds,
        schema=', '.join(i for i in schema_column if i not in partition_column),
        default_sample_rate=0.1,
    ))

    # run query over sampled table
    logging.info('run query over sampled table')
    hiveql.execute(average_by_parameter_sql.format(
        ds=ds,
        input_table=sample_table,
        result_table=sample_result_table,
    ))

    # evaluate sample error
    sample_evaluate(
        table_name=RESULT_TABLE,
        sample_type=SAMPLE_TYPE,
        sample_rate=0.1,
        group_by_columns=['parameter', 'unit'],
        aggregation_columns=['average'],
        partition={'ds': ds},
        sample_table=sample_result_table,
    )
