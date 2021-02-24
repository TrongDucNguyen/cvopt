from dwr import *
from dwr.ci.group_by_sum import *
import numbers

SAMPLE_TYPE = SENATE

logging.info("get frequency of strata")

# language=HQL
hiveql.execute("""
    SELECT 
        country || '_' || parameter || '_' || unit,
        count(*),
        stddev(value) / avg(value)
    FROM {input_table}
    GROUP BY country, parameter, unit
""".format(input_table=INPUT_TABLE))
result = hiveql.fetchall()
frequency = {r[0]: r[1] for r in result}
sample_rate = int((max(r[2] for r in result if isinstance(r[2], numbers.Number)) ** 2 + 1) * len(result)
                  * ((1.96 / 0.1) ** 2) / sum(frequency.values()))

logging.info("Sample rate: {0}".format(sample_rate))

if sample_rate >= 1:
    exit

# create sampled table
sample_input_table = senate_sample(
    sample_rate=sample_rate,
    table_name=INPUT_TABLE,
    group_by=['country', 'parameter', 'unit'],
    frequency=frequency,
)

# create result table
logging.info('create result table')
sample_result_table = sample_table_name(RESULT_TABLE, SAMPLE_TYPE, sample_rate)
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
    sample_rate=sample_rate,
    group_by_columns=['country', 'parameter', 'unit'],
    aggregation_columns=['sum'],
)
