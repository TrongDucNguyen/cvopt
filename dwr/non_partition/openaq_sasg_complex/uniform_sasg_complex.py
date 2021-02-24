from dwr import *
from dwr.non_partition.openaq_sasg_complex import *

SAMPLE_TYPE = UNIFORM


def run():

    for sample_rate in sample_rates:
        logging.info("Sample rate: {0}".format(sample_rate))

        t1 = time.time()  # ---------------------------------------

        # create sampled table
        sample_input_table = uniform_sample(sample_rate, INPUT_TABLE, overwrite=True)

        t2 = time.time()  # ---------------------------------------
        time_export(sample_rate, SAMPLE_TYPE, "Draw Sample", t2-t1)

        # ----------------------------------------------------------
        # The following section applies to all sampling methods

        # create result table
        logging.info('create result table')
        sample_result_table = sample_table_name(RESULT_TABLE, SAMPLE_TYPE, sample_rate)

        hiveql.execute(create_result_table.format(sample_result_table))

        t1 = time.time()  # ---------------------------------------

        # run query over sampled table
        logging.info('run query over sampled table')
        hiveql.execute(sasg.format(
            input_table=sample_input_table,
            result_table=sample_result_table,
        ))

        t2 = time.time()  # ---------------------------------------
        time_export(sample_rate, SAMPLE_TYPE, "Query", t2 - t1)

        # evaluate sample error
        sample_evaluate(
            table_name=RESULT_TABLE,
            sample_type=SAMPLE_TYPE,
            sample_rate=sample_rate,
            group_by_columns=['country', 'month', 'year'],
            aggregation_columns=['average'],
        )


run()
