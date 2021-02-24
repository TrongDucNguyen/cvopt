from dwr import *
from dwr.ds_partition.openaq_avg_by_parameter import *

# create result table
hiveql.execute(create_result_table.format(RESULT_TABLE))

for DS in ds_list:
    logging.info("DS: {0} - Full query".format(DS))
    hiveql.execute(average_by_parameter_sql.format(
        ds=DS,
        result_table=RESULT_TABLE,
        input_table=INPUT_TABLE,
    ))
