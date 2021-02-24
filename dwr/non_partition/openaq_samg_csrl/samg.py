from dwr import *
from dwr.non_partition.openaq_samg_csrl import *

# create result table
hiveql.execute('DROP TABLE {0}'.format(RESULT_TABLE1))
hiveql.execute('DROP TABLE {0}'.format(RESULT_TABLE2))

hiveql.execute(create_result_table.format(RESULT_TABLE1, "city"))
hiveql.execute(create_result_table.format(RESULT_TABLE2, "attribution"))

logging.info("Full query Q1")
hiveql.execute(samg_q1.format(
    result_table=RESULT_TABLE1,
    input_table=INPUT_TABLE,
))

logging.info("Full query Q2")
hiveql.execute(samg_q2.format(
    result_table=RESULT_TABLE2,
    input_table=INPUT_TABLE,
))



