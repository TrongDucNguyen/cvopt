from dwr import *
from dwr.ds_partition.openaq_sasg import *
from dwr.ds_partition.openaq_sasg.sasg import *

# Experiment parameters
aggr = "avg"    # single aggregation function (sum, avg, count)
col = "value"   # aggregated column name
grp = {'country': 'STRING', 'parameter': 'STRING', 'unit': 'STRING'}  # group by

# INPUT_TABLE & RESULT_TABLE
INPUT_TABLE = "openaq"
RESULT_TABLE = "openaq_ds_{0}_by_{1}".format(aggr, list(grp)[0])

# SASG
x = sasg(aggr, col, grp)

# Create RESULT_TABLE
hiveql.execute(x.create_result_table_sql(RESULT_TABLE))


for DS in ds_list:
    logging.info("DS: {0} - Full query".format(DS))
    hiveql.execute(
        x.sasg_sql(DS, INPUT_TABLE, RESULT_TABLE)
    )

