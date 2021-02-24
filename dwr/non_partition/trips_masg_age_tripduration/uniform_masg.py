from dwr.CSRLSS import *
from dwr.non_partition.trips_masg_age_tripduration import *

SAMPLE_TYPE = UNIFORM

for sample_rate in sample_rates:
    logging.info("Sample rate: {0}".format(sample_rate))

    # create sampled table
    sample_input_table = uniform_sample(sample_rate, INPUT_TABLE, overwrite=True,)

    # ----------------------------------------------------------
    # The following section applies to all sampling methods

    # create result table
    logging.info('create result table')
    sample_result_table = sample_table_name(RESULT_TABLE, SAMPLE_TYPE, sample_rate)
    drop_table(sample_result_table, DROP)
    hiveql.execute(create_result_table.format(sample_result_table))

    logging.info('run query over sampled table')
    hiveql.execute(masg_sampled.format(
        input_table=sample_input_table,
        result_table=sample_result_table,
    ))

    # evaluate sample error
    sample_evaluate(
        table_name=RESULT_TABLE,
        sample_type=SAMPLE_TYPE,
        sample_rate=sample_rate,
        group_by_columns=['from_station_id'],
        aggregation_columns=['agg1', 'agg2'],
        # set weighted = true to use sampled_aggregate_error table, which separates agg1 & agg2
        weighted=True
    )
