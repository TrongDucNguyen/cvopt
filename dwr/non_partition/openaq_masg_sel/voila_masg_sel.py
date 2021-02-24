from dwr.CSRLSS import *
from dwr.non_partition.openaq_masg_sel import *

SAMPLE_TYPE = VOILA

logging.info('get statistic')
# language=HQL
hiveql.execute("""
    SELECT     
        country || '_' || parameter || '_' || unit,
        COUNT(*),
        ABS(STDDEV(value) / AVG(value)),
        ABS(STDDEV(latitude+180) / AVG(latitude+180))      
    FROM openaq_clean 
    GROUP BY country, parameter, unit
""")
result = hiveql.fetchall()
frequency = {r[0]: r[1] for r in result}
coefficient = {r[0]: r[2:] for r in result}

for sample_rate in sample_rates:
    logging.info("Sample rate: {0}".format(sample_rate))

    # create sampled table
    sample_input_table = masg_sample(
        sample_rate=sample_rate,
        table_name=INPUT_TABLE,
        group_by=['country', 'parameter', 'unit'],
        aggregate_column=['agg1', 'agg2'],
        frequency=frequency,
        coefficient=coefficient,
        overwrite=True,
    )

    # ----------------------------------------------------------
    # The following section applies to all sampling methods

    # create result table
    logging.info('create result table')
    sample_result_table = sample_table_name(RESULT_TABLE, SAMPLE_TYPE, sample_rate)
    hiveql.execute(create_result_table.format(sample_result_table))

    logging.info('run query over sampled table')
    hiveql.execute(masg.format(
        input_table=sample_input_table,
        result_table=sample_result_table,
        predicate=PREDICATE,
    ))

    # evaluate sample error
    sample_evaluate(
        table_name=RESULT_TABLE,
        sample_type=SAMPLE_TYPE,
        sample_rate=sample_rate,
        group_by_columns=['country', 'parameter', 'unit'],
        aggregation_columns=['agg1', 'agg2'],
    )
