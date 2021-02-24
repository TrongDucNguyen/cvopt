from dwr import *
from dwr.non_partition.openaq_masg_complex_time import *


def run():

    t1 = time.time()

    # create result table
    drop_table(RESULT_TABLE, DROP)
    hiveql.execute(create_result_table.format(RESULT_TABLE))

    logging.info("Full query")
    hiveql.execute(masg.format(
        result_table=RESULT_TABLE,
        input_table=INPUT_TABLE,
    ))

    t2 = time.time()
    time_export("X", "Original", "Query", t2-t1)