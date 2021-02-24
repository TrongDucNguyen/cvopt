from dwr import *
from dwr.ci.group_by_sum import *
import numbers

SAMPLE_TYPE = UNIFORM

logging.info("Get CVs")

# language=HQL
hiveql.execute("""
    SELECT 
        count(*),
        stddev(value) / avg(value)
    FROM {input_table}
    GROUP BY country, parameter, unit
""".format(input_table=INPUT_TABLE))
result = hiveql.fetchall()
sample_rate = min(1, max((r[1]**2 + 1) / r[0] for r in result if isinstance(r[1], numbers.Number)) * ((1.96 / 0.1) ** 2))

logging.info("Sample rate: {0}".format(sample_rate))

if sample_rate >= 1:
    exit

# create sampled table
sample_table = uniform_sample(sample_rate, INPUT_TABLE)

# create result table
logging.info('create result table')
hiveql.execute(create_result_table.format(sample_table_name(RESULT_TABLE, SAMPLE_TYPE, sample_rate)))

# run query over sampled table
logging.info('run query over sampled table')
hiveql.execute(sum_by_parameter_sample.format(
    input_table=sample_table,
    result_table=sample_table_name(RESULT_TABLE, SAMPLE_TYPE, sample_rate),
))

# evaluate sample error
sample_evaluate(
    table_name=RESULT_TABLE,
    sample_type=SAMPLE_TYPE,
    sample_rate=sample_rate,
    group_by_columns=['country', 'parameter', 'unit'],
    aggregation_columns=['sum'],
)
