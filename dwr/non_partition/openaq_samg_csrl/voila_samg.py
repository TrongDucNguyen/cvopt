from dwr import *
from dwr.non_partition.openaq_samg_csrl import *

SAMPLE_TYPE = VOILA

group_by = [{'city'}, {'attribution'}]

# language=HQL
frequency = """
   SELECT
       {stratum},
       f,
       f*f * ij*ij * (1.0 / (i*i) + 1.0 / (j*j))
   FROM (
       SELECT 
           {group_by_ij}, 
           count({aggregate}) f,
           stddev({aggregate}) ij
       FROM {table_name}
       GROUP BY {group_by_ij}
   ) c,
   (
       SELECT
           {group_by_i},
           count({aggregate}) * avg({aggregate}) i
       FROM {table_name}
       GROUP BY {group_by_i}
   ) a,
   (
       SELECT
           {group_by_j},
           count({aggregate}) * avg({aggregate}) j
       FROM {table_name}
       GROUP BY {group_by_j}
   ) b
   WHERE {where_i}
   AND {where_j}                
""".format(
    table_name=INPUT_TABLE,
    aggregate='value',
    stratum=" || '_' || ".join(['c.{0}'.format(i) for i in set.union(*group_by)]),
    group_by_ij=', '.join(i for i in set.union(*group_by)),
    group_by_i=', '.join(i for i in group_by[0]),
    group_by_j=', '.join(i for i in group_by[1]),
    where_i=' AND '.join(['a.{0} = c.{0}'.format(i) for i in group_by[0]]),
    where_j=' AND '.join(['b.{0} = c.{0}'.format(i) for i in group_by[1]]),
)

hiveql.execute(frequency)
result = hiveql.fetchall()
frequency = {r[0]: r[1] for r in result}
coefficient = {r[0]: r[2] for r in result}

for sample_rate in sample_rates:
    logging.info("Sample rate: {0}".format(sample_rate))

    # create sampled table
    sample_input_table = samg_sample(
        sample_rate=sample_rate,
        table_name=INPUT_TABLE,
        group_by=group_by,
        frequency=frequency,
        coefficient=coefficient,
        aggregate_column='value',
        overwrite=True,
    )

    # ----------------------------------------------------------
    # The following section applies to all three sampling methods

    # create result table
    sample_result_table1 = sample_table_name(RESULT_TABLE1, SAMPLE_TYPE, sample_rate)
    sample_result_table2 = sample_table_name(RESULT_TABLE2, SAMPLE_TYPE, sample_rate)

    hiveql.execute('DROP TABLE {0}'.format(sample_result_table1))
    hiveql.execute('DROP TABLE {0}'.format(sample_result_table2))

    hiveql.execute(create_result_table.format(sample_result_table1, "city"))
    hiveql.execute(create_result_table.format(sample_result_table2, "attribution"))

    # run query over sampled table
    logging.info('run query over sampled table')
    hiveql.execute(samg_q1_sampled.format(
        input_table=sample_input_table,
        result_table=sample_result_table1,
    ))
    hiveql.execute(samg_q2_sampled.format(
        input_table=sample_input_table,
        result_table=sample_result_table2,
    ))

    # evaluate sample error
    sample_evaluate(
        table_name=RESULT_TABLE1,
        sample_type=SAMPLE_TYPE,
        sample_rate=sample_rate,
        group_by_columns=['city'],
        aggregation_columns=['agg_value'],
    )
    sample_evaluate(
        table_name=RESULT_TABLE2,
        sample_type=SAMPLE_TYPE,
        sample_rate=sample_rate,
        group_by_columns=['attribution'],
        aggregation_columns=['agg_value'],
    )
