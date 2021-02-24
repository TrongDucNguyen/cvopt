from dwr import *
from dwr.ci.group_by_sum import *

SAMPLE_TYPE = 'STRATIFIED'

EPSILON = 0.1

# create sampled table
sample_input_table = stratified_sample(
    epsilon=EPSILON,
    table_name=INPUT_TABLE,
    group_by=['country', 'parameter', 'unit'],
)

# create result table
logging.info('create result table')
sample_result_table = sample_table_name(RESULT_TABLE, SAMPLE_TYPE, EPSILON)
hiveql.execute(create_result_table.format(sample_result_table))

logging.info('run query over sampled table')
hiveql.execute(sum_by_parameter_sample.format(
    input_table=sample_input_table,
    result_table=sample_result_table,
))

# evaluate sample error
sample_evaluate(
    table_name=RESULT_TABLE,
    sample_type=SAMPLE_TYPE,
    sample_rate=EPSILON,
    group_by_columns=['country', 'parameter', 'unit'],
    aggregation_columns=['sum'],
)
