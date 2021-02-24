from dwr.CSRLSS import *
from dwr.non_partition.openaq_masg_sel import *

SAMPLE_TYPE = RL

logging.info('get statistic')
# language=HQL
hiveql.execute("""
    SELECT     
        country || '_' || parameter || '_' || unit,
        COUNT(*),
        ABS(AVG(value)),
        STDDEV(value),
        ABS(AVG(latitude+180)),
        STDDEV(latitude+180)
    FROM openaq_clean 
    GROUP BY country, parameter, unit
""")
result = hiveql.fetchall()
frequency = {r[0]: r[1] for r in result}
rsd1 = {r[0]: r[3]/max(r[2], 1.0) for r in result}
lat_mean = {r[0]: float(180 if r[4] is None else r[4]) for r in result}
lat_std = {r[0]: float(0 if r[5] is None else r[5]) for r in result}
rsd2 = {i: lat_std[i]/max(lat_mean[i], 1) for i in frequency.keys()}
strata = list(frequency.keys())
coefficient = {i: (rsd1[i]+rsd2[i])*sqrt(abs(1/frequency[i] - 1/len(strata))) for i in strata}


for sample_rate in sample_rates:
    logging.info("Sample rate: {0}".format(sample_rate))

    # create sampled table
    sample_input_table = rl_sample(
        sample_rate=sample_rate,
        table_name=INPUT_TABLE,
        aggregate_column='value',
        group_by=['country', 'parameter', 'unit'],
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
