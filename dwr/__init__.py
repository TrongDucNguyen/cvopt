import getpass
import logging
import sys
from datetime import datetime, timedelta
from numbers import Number
from typing import Union

from pyhive import hive

# Constants
SENATE = 'senate'
UNIFORM = 'uniform'

# set up logging
logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format='%(asctime)s %(levelname)-8s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# set up hive connection
if getpass.getuser() == 'mshih':
    hiveql = hive.connect('fastdw2.ece.iastate.edu', auth='NOSASL', database='openaq_10x').cursor()
    # hiveql = hive.connect('fastdw2.ece.iastate.edu', auth='NOSASL', database='trips').cursor()
    # hiveql = hive.connect('fastdw2.ece.iastate.edu', auth='NOSASL', database='default').cursor()
    # openaq exp
    # sample_rates = [0.0001, 0.001, 0.01, 0.1]
    # trips exp
    sample_rates = [0.001, 0.01, 0.05, 0.1]

elif getpass.getuser() == 'trong':
    hiveql = hive.connect('fastdw2.ece.iastate.edu', auth='NOSASL').cursor()
else:
    hiveql = hive.connect('fastdw2.ece.iastate.edu', auth='NOSASL', database='trips').cursor()

logging.info('Setup Hive connection: {0}'.format(hiveql))


# ----------------------
# Utility functions
# ----------------------


def yesterday(ds):
    return (datetime.strptime(ds, '%Y-%m-%d').date() - timedelta(1)).strftime('%Y-%m-%d')


def sample_table_name(base_name, sample_type, sample_rate=None, partition=None, group_by=None, aggregate=None):
    """
    Generate sampled table's name from full table name.
    :param base_name: name of full table
    :param sample_type: sample algorithm
    :param sample_rate: sample rate
    :param partition: [Optional] partition columns
    :param group_by: group by columns
    :param aggregate: aggregate columns
    :return: <base_name>_sampled_<sample_type>_<sample_rate>pct
    """
    elements = [base_name, sample_type]
    if partition:
        if isinstance(partition, str):
            elements.append(partition)
        else:
            elements.extend(partition)
    if sample_rate:
        try:
            float(sample_rate)
            if sample_rate >= 0.01:
                elements.append(str(int(round(sample_rate * 100))) + 'pct')
            else:
                elements.append('{0}'.format(sample_rate).replace('-', '_').replace('.', '_'))
        except ValueError:
            elements.append(sample_rate)
    if group_by:
        elements.append('G')
        if isinstance(group_by, str):
            elements.append(group_by)
            elements.append('G')
        elif isinstance(group_by, list) and (isinstance(group_by[0], list) or isinstance(group_by[0], set)):
            for g in group_by:
                for i in g:
                    elements.append(i)
                elements.append('G')
        else:
            elements.extend(group_by)
            elements.append('G')
    if aggregate:
        elements.append('A')
        if isinstance(aggregate, str):
            elements.append(aggregate)
        else:
            elements.extend(aggregate)
        elements.append('A')
    return '_'.join(elements)


# ---------------------
# Allocation algorithms
# ---------------------


def senate_allocation(count, sample_rate):
    """
    Allocate the sample rate for each stratum using Senate strategy.
    Reallocate unused space from underpopulation strata equal to the rest.
    :param count: # of records for each stratum
    :param sample_rate: sample rate
    :return: sample rate for each stratum
    """
    if sample_rate <= 0:
        return {}
    budget = min(sample_rate, 1) * sum(count.values())
    c = list(count.keys())
    o_len = 0
    while len(c) > 0 and len(c) != o_len:
        o_len = len(c)
        s = budget / o_len
        for i in list(c):
            if count[i] <= s:
                budget -= count[i]
                c.remove(i)
                s = budget / o_len
    return {i: min(s / count[i], 1) for i in count}


def senate_allocation_fast(count, sample_rate):
    """
    Allocate the sample rate for each stratum using Senate strategy.
    Reallocate unused space from underpopulation strata equal to the rest.
    Simple implementation costs O(n^2), n is number of strata.
    This implementation costs O(x log x), x is the allocation for unbounded strata.
    :param count: # of records for each stratum
    :param sample_rate: sample rate
    :return: sample rate for each stratum
    """
    logging.info('senate allocation')
    if sample_rate <= 0:
        logging.error('Invalid sample rate')
        return {}
    budget = min(sample_rate, 1) * sum(count.values())
    lo = hi = budget / len(count)
    while sum([min(count[i], hi) for i in count]) < budget:
        hi *= 2
    while lo < hi:
        mid = (lo + hi) / 2
        mem = sum([min(count[i], mid) for i in count])
        lo = mid if budget + 0.1 >= mem else lo
        hi = mid if budget - 0.1 <= mem else hi
    return {i: min(count[i], lo) / count[i] for i in count}


def cvopt_allocation(sample_rate, count, coeff):
    """
    Group-by Voila allocation for stratified sampling.
    :param count: # of records for each stratum
    :param sample_rate: sample rate
    :param coeff: coefficient variance
    :return: sample rate for each stratum
    """
    logging.info('CVOPT allocation')
    if sample_rate <= 0:
        logging.error('Invalid sample rate')
        return {}

    # give the strata with 0 coeff the min coeff value
    min_coeff = min(i for i in coeff.values() if isinstance(i, Number) and i > 0)
    coeff = {i: coeff[i] if isinstance(coeff[i], Number) and coeff[i] > 0 else min_coeff for i in coeff}

    budget = min(sample_rate, 1) * sum(count.values())
    alloc = {}
    c = list(count.keys())
    sum_coeff = sum(i for i in coeff.values())
    old_len = 0
    while old_len != len(c):
        old_len = len(c)
        for i in list(c):
            if budget * coeff[i] / sum_coeff > count[i]:
                alloc[i] = count[i]
                budget -= count[i]
                sum_coeff -= coeff[i]
                c.remove(i)
    for i in c:
        alloc[i] = budget * coeff[i] / sum_coeff
    return {i: min(1.0, max(alloc[i], 5) / count[i]) for i in alloc}


def cvopt2_allocation(sample_rate, count, coeff):
    """
    Group-by Voila allocation for stratified sampling.
    :param count: # of records for each stratum
    :param sample_rate: sample rate
    :param coeff: coefficient variance
    :return: sample rate for each stratum
    """
    logging.info('CVOPT INF allocation')
    if sample_rate <= 0:
        logging.error('Invalid sample rate')
        return {}

    # give the strata with 0 coeff the min coeff value
    min_coeff = min(i for i in coeff.values() if isinstance(i, Number) and i > 0)
    coeff = {i: coeff[i] if isinstance(coeff[i], Number) and coeff[i] > 0 else min_coeff for i in coeff}

    budget = min(sample_rate, 1) * sum(count.values())
    alloc = {}
    c = list(count.keys())

    inf_coeff = {}
    for i in count.keys():
        inf_coeff[i] = count[i] * pow(coeff[i], 2) / (count[i] + pow(coeff[i], 2))

    sum_coeff = sum(i for i in inf_coeff.values())

    old_len = 0
    while old_len != len(c):
        old_len = len(c)
        for i in list(c):
            if budget * inf_coeff[i] / sum_coeff > count[i]:
                alloc[i] = count[i]
                budget -= count[i]
                sum_coeff -= inf_coeff[i]
                c.remove(i)
    for i in c:
        alloc[i] = budget * inf_coeff[i] / sum_coeff
    return {i: min(1.0, max(alloc[i], 5) / count[i]) for i in alloc}


def cvopt_infinity_allocation(sample_rate, frequency, coeff):
    min_coeff = min(i for i in coeff.values() if isinstance(i, Number) and i > 0)
    alpha = {s: pow(coeff[s] if isinstance(coeff[s], Number) and coeff[s] > 0 else min_coeff, 2) for s in coeff}
    beta = {s: alpha[s] / frequency[s] for s in alpha}

    budget = min(sample_rate, 1) * sum(frequency.values())
    T = 1
    while sum(alpha[i] / (T + beta[i]) for i in alpha) < budget:
        T /= 2
    while sum(alpha[i] / (T + beta[i]) for i in alpha) > budget:
        T *= 2
    l, r = 0, T
    while abs(l - r) / r > 0.01:
        T = (l + r) / 2
        if sum(alpha[i] / (T + beta[i]) for i in alpha) > budget:
            l = T
        else:
            r = T

    return {i: min(1.0, max(alpha[i] / (T + beta[i]), 5) / frequency[i]) for i in alpha}


# ---------------------
# Hive Operators
# ---------------------


def drop_table(table_name, flag=True):
    if flag is True:
        hiveql.execute("DROP TABLE IF EXISTS {0}".format(table_name))
    else:
        pass


def drop_partition(table_name, partition):
    hiveql.execute("ALTER TABLE {table_name} DROP IF EXISTS PARTITION ({partition})".format(
        table_name=table_name,
        partition=",".join("{0}={1}".format(i, partition[i]) for i in partition),
    ))


def exists(table_name, partition=None):
    logging.info('Check if table {0} exists'.format(table_name))
    exist = False
    try:
        # language=HQL - Check if sample table exits
        hiveql.execute("DESC {table_name} PARTITION ({partition})".format(
            table_name=table_name,
            partition=','.join("{0} = '{1}'".format(i, partition[i]) for i in partition),
        ) if partition is not None and len(partition) > 0 else "SHOW TABLES LIKE '{0}'".format(table_name))
        exist = len(hiveql.fetchall()) > 0
    except:
        pass

    logging.info('Table {0} exists!'.format(table_name) if exist else "Table {0} doesn't exists!".format(table_name))
    return exist


def schema(table_name):
    """
    Get table schema
    :param table_name: table name
    :return: schema of table
    """
    logging.info('Get schema of table {0}'.format(table_name))

    # language=HQL - get table schema
    hiveql.execute("DESC {0}".format(table_name))
    schema_column = {}
    partition_column = {}
    is_partitioned = False
    result = iter(hiveql.fetchall())
    for r in result:
        if is_partitioned:
            partition_column[r[0]] = r[1]
        else:
            if r[0] == '' and next(result)[0] == '# Partition Information' \
                    and next(result)[0] == '# col_name            ' and next(result)[0] == '':
                is_partitioned = True
            else:
                schema_column[r[0]] = r[1]
    return schema_column, partition_column


def uniform_sample(sample_rate, table_name, partition={}, sample_table=None, overwrite=False):
    """
    Create uniform sampled table
    :param sample_rate: sample rate
    :param table_name: base table
    :param partition: partition columns of sampled table. Optional: empty to use sample partition as base table.
    :param sample_table: name of sampled table. Optional: empty to use default sample table name.
    :param overwrite: overwrite if sampled table exist. Optional: default is NOT OVERWRITE
    :return: name of sampled table
    """
    logging.info('Create {0} uniform sample table from {1}'.format(sample_rate, table_name))
    if not sample_table:
        sample_table = sample_table_name(table_name, UNIFORM, sample_rate, partition)

    # check if sampled table exists
    if not overwrite and exists(sample_table, partition):
        return sample_table
    else:
        # language=HQL
        hiveql.execute("DROP TABLE {0}".format(sample_table))

    # get table's schema
    schema_column, partition_column = schema(table_name)

    # check the partitions
    if len(set(partition.keys()) ^ set(partition_column.keys())) > 0:
        logging.warning("Sampled table and original table have different partition!")

    logging.info('Create sampled table')

    # language=HQL - Create sampled table
    hiveql.execute("""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {schema},
            sample_rate double
        )
        {partition}
    """.format(
        table_name=sample_table,
        schema=','.join('{0} {1}'.format(i, schema_column[i]) for i in schema_column if i not in partition),
        partition='PARTITIONED BY ({0})'.format(
            ','.join('{0} {1}'.format(i, partition_column[i]) for i in partition)
        ) if len(partition) > 0 else ''
    ))

    logging.info('Draw sample')
    # language=HQL - draw sample
    hiveql.execute("""
        INSERT OVERWRITE TABLE {sampled_table} {sampled_partition}
        SELECT 
            {schema},
            '{sample_rate}' sample_rate
        FROM {full_table}
        WHERE rand() <= '{sample_rate}'
        {full_partition}
    """.format(
        full_table=table_name,
        sampled_table=sample_table,
        sample_rate=sample_rate,
        schema=','.join(i for i in schema_column if i not in partition),
        sampled_partition='PARTITION ({0})'.format(
            ','.join("{0}='{1}'".format(i, partition[i]) for i in partition)
        ) if len(partition) > 0 else '',
        full_partition='\n'.join(
            "AND {0} = '{1}'".format(i, partition[i]) for i in partition
        ) if len(partition) > 0 else '',
    ))
    return sample_table


def senate_sample(sample_rate: float, table_name: str, group_by: Union[str, dict, list], partition: dict = {},
                  frequency: Union[str, dict] = None, select: str = None, sample_table: str = None,
                  overwrite: bool = False) -> str:
    """
    Create senate stratifying sampled table
    :param sample_rate: sample rate
    :param table_name: base table
    :param group_by: group by column(s). Stratification is based on group by columns.
    :param partition: partition column(s) of sampled table. Optional: empty to use same partition as base table.
    :param frequency: [Optional] frequency of each group. Dict: precalculated frequencies / String: query / None: default
    :param select: [Optional] selected query to draw sample. Empty to use default select query to draw sample.
    :param sample_table: [Optional] name of sampled table. Empty to use default name.
    :param overwrite: [Optional] overwrite sampled table if it exists. Default is NOT OVERWRITE.
    :return: name of sampled table.
    """
    logging.info('Create {0} senate sample table from {1}'.format(sample_rate, table_name))
    if isinstance(group_by, str):
        group_by = [group_by]

    if not sample_table:
        sample_table = sample_table_name(
            base_name=table_name,
            sample_type=SENATE,
            sample_rate=sample_rate,
            partition=partition,
            group_by=group_by,
        )

    # check if sampled table exists
    if not overwrite and exists(sample_table, partition):
        return sample_table
    else:
        # language=HQL
        hiveql.execute("DROP TABLE {0}".format(sample_table))

    # get full table's schema
    schema_column, partition_column = schema(table_name)

    # check the partitions
    if len(set(partition.keys()) ^ set(partition_column.keys())) > 0:
        logging.warning("Sampled table and original table have different partition!")

    # senate allocation
    if overwrite or not exists('senate_allocation',
                               {'ds': partition.get('ds', '~'),
                                'table_name': table_name,
                                'default_rate': sample_rate}):
        # get the frequency of each stratum
        if frequency is None:
            # language=HQL
            frequency = """
                SELECT 
                    {strata},
                    COUNT(*)
                FROM {table_name}
                {partition}
                GROUP BY {group_by}
            """.format(
                table_name=table_name,
                strata=" || '_' || ".join(i for i in group_by),
                group_by=', '.join(i for i in group_by),
                partition='WHERE {0}'.format(' AND '.join("{0} = '{1}'".format(i, partition[i]) for i in partition))
                if len(partition) > 0 else '',
            )

        if isinstance(frequency, str):
            logging.info('Get statistic from full table')
            hiveql.execute(frequency)
            frequency = {r[0]: r[1] for r in hiveql.fetchall()}

        if not isinstance(frequency, dict):
            logging.error('get_frequency is invalid')
            return None

        # senate alloc
        alloc = senate_allocation(frequency, sample_rate)

        logging.info('Store allocation to senate_allocation table')
        # language=HQL
        hiveql.execute("""
            INSERT OVERWRITE TABLE senate_allocation 
            PARTITION(ds='{ds}', table_name='{table_name}', default_rate={default_rate})
            VALUES {values} 
        """.format(
            ds=partition.get('ds', '~'),
            table_name=table_name,
            default_rate=sample_rate,
            values=',\n'.join(["('{0}', {1})".format(k.replace("'", "\\'"), alloc[k]) for k in alloc]),
        ))

    logging.info('Create sampled table')
    # language=HQL - Create sampled table
    hiveql.execute("""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {schema},
                sample_rate double
            )
            {partition}
        """.format(
        table_name=sample_table,
        schema=','.join('{0} {1}'.format(i, schema_column[i]) for i in schema_column if i not in partition),
        partition='PARTITIONED BY ({0})'.format(
            ','.join('{0} {1}'.format(i, partition_column[i]) for i in partition)
        ) if len(partition) > 0 else ''
    ))

    logging.info('Draw sample')
    if select is None:
        # language=HQL
        select = """
            SELECT 
                {schema},
                COALESCE(sample_rate, '{sample_rate}') as sample_rate
            FROM {table_name} full_table        
            LEFT JOIN senate_allocation
                ON senate_allocation.ds = '{ds}'
                AND senate_allocation.table_name = '{table_name}'
                AND senate_allocation.default_rate = '{sample_rate}'
                AND {group_by}
            WHERE RAND() <= COALESCE(sample_rate, '{sample_rate}')
            {partition}
        """.format(
            ds=partition.get('ds', '~'),
            table_name=table_name,
            sample_rate=sample_rate,
            schema=', '.join('full_table.{0}'.format(i) for i in schema_column if i not in partition),
            partition='\n'.join(
                "AND full_table.{0} = '{1}'".format(i, partition[i]) for i in partition
            ) if len(partition) > 0 else '',
            group_by='senate_allocation.stratum = {0}'.format(
                " || '_' || ".join('full_table.{0}'.format(i) for i in group_by)
            ),
        )

    # language=HQL - draw sample
    hiveql.execute("""
        INSERT OVERWRITE TABLE {table_name} {partition}
        SELECT
            {schema},
            COALESCE(sample_rate, '{sample_rate}') AS sample_rate
        FROM (
            {select}
        ) z
    """.format(
        table_name=sample_table,
        partition='PARTITION ({0})'.format(
            ','.join("{0}='{1}'".format(i, partition[i]) for i in partition)
        ) if len(partition) > 0 else '',
        schema=', '.join(i for i in schema_column if i not in partition),
        select=select,
        sample_rate=sample_rate,
    ))
    return sample_table


def stratified_sample(epsilon: float, table_name: str, group_by: Union[str, dict, list],
                      overwrite: bool = False) -> str:
    """
    Create stratifying sampled table
    """
    group_by = [group_by] if isinstance(group_by, str) else group_by

    logging.info("Get frequency and Cv of strata")
    # language=HQL
    hiveql.execute("""
        SELECT 
            country || '_' || parameter || '_' || unit,
            count(*),
            stddev(value) / avg(value)
        FROM {input_table}
        GROUP BY country, parameter, unit
    """.format(input_table=table_name))
    result = hiveql.fetchall()
    frequence = {r[0]: r[1] for r in result}
    sample_rate = {r[0]: min(1, ((r[2] ** 2 + 1) / r[1]) * (1.96 / 0.1) ** 2 if isinstance(r[2], Number) else 1)
                   for r in result}
    logging.info('Sample rate: {0}'.format(sample_rate))
    logging.info('Overall sample rate: {0}'.format(
        sum(frequence[i] * sample_rate.get(i, 1) for i in frequence) / sum(frequence.values())))

    logging.info('Create stratified sample table from {0}'.format(table_name))
    sample_table = sample_table_name(
        base_name=table_name,
        sample_type='stratified',
        group_by=group_by,
    )

    # check if sampled table exists
    if not overwrite and exists(sample_table):
        return sample_table

    # senate allocation
    if overwrite or not exists('stratified_allocation',
                               {'table_name': table_name,
                                'default_rate': 1}):
        logging.info('Store allocation to stratified_allocation table')
        # language=HQL
        hiveql.execute("""
            INSERT OVERWRITE TABLE stratified_allocation 
            PARTITION(table_name='{table_name}', default_rate={default_rate})
            VALUES {values} 
        """.format(
            table_name=table_name,
            default_rate=0.1,
            values=', '.join("('{0}', {1})".format(k, sample_rate[k]) for k in sample_rate),
        ))

        # get full table's schema
        schema_column, _ = schema(table_name)

        logging.info('Draw sample')
        # language=HQL
        hiveql.execute("""
            CREATE TABLE {sample_table} AS
            SELECT
                {schema},
                COALESCE(sample_rate, '{sample_rate}') as sample_rate
            FROM {table_name} full_table        
            LEFT JOIN stratified_allocation alloc
                ON alloc.table_name = '{table_name}'
                AND {group_by}
            WHERE RAND() <= COALESCE(sample_rate, '{sample_rate}')
        """.format(
            table_name=table_name,
            sample_table=sample_table,
            sample_rate=0.1,
            schema=', '.join('full_table.{0}'.format(i) for i in schema_column),
            group_by='alloc.stratum = {0}'.format(
                " || '_' || ".join('full_table.{0}'.format(i) for i in group_by)
            ),
        ))
    return sample_table


def sample_evaluate(table_name, sample_type, sample_rate, group_by_columns, aggregation_columns, partition={},
                    sample_table=None, weighted=False):
    """
    Evaluate sample error by compare results from full table and sampled table. Sample error is inserted into table
    sample_error.
    :param table_name: base table
    :param sample_type: sample type
    :param sample_rate: sample rate
    :param group_by_columns: group by columns
    :param aggregation_columns: aggregation columns
    :param partition: [optional] partition of 2 tables
    :param sample_table: [optional]: customized name for sampled table
    :return:
    """
    logging.info('Evaluate the query over sampled table')

    logging.info('Create sample_error table')
    # language=HQL
    hiveql.execute("""
        CREATE TABLE IF NOT EXISTS sample_error (
            attribute string,
            error double
        )
        PARTITIONED BY (
            ds string,
            table_name string,
            sample_type string,
            sample_rate double
        )
    """)

    if not sample_table:
        sample_table = sample_table_name(table_name, sample_type, sample_rate, partition)

    if isinstance(group_by_columns, str):
        group_by_columns = [group_by_columns]

    if isinstance(aggregation_columns, str):
        aggregation_columns = [aggregation_columns]

    logging.info('Clear partition')
    drop_partition('sample_error', {
        'ds': partition.get('ds', "'~'"),
        'table_name': "'{0}'".format(table_name),
        'sample_type': "'{0}'".format(sample_type),
        'sample_rate': sample_rate,
    })

    logging.info('evaluate')
    # language=HQL
    hiveql.execute("""
        WITH error AS (
            SELECT
                {group_by}, {compare}
            FROM {full_table} full_table
            LEFT JOIN {sampled_table} sampled_table
            ON {group_by_pair}             
            {sample_partition}                          
            {full_partition}            
        )
        INSERT OVERWRITE TABLE {error_tale} 
        PARTITION (
            ds = '{ds}',
            table_name = '{full_table}',
            sample_type = '{sample_type}',
            sample_rate = '{sample_rate}'
        )
        {select}        
    """.format(
        group_by='{0} as stratum'.format(" || '_' || ".join('full_table.{0}'.format(i) for i in group_by_columns)),
        compare=','.join("""
                IF(full_table.{0} IS NULL OR full_table.{0} = 0,
                    IF(sampled_table.{0} IS NULL OR sampled_table.{0} = 0, 0.0, 1.0),
                    IF(sampled_table.{0} IS NULL OR sampled_table.{0} = 0, 1.0, 
                        ABS((full_table.{0} - sampled_table.{0}) * 1.0 / full_table.{0})) 
                ) {0}""".format(i) for i in aggregation_columns),
        error_tale='sampled_aggregate_error' if weighted else 'sample_error',
        full_table=table_name,
        sampled_table=sample_table,
        sample_type=sample_type,
        sample_rate=sample_rate,
        group_by_pair='\n            AND '.join(
            "full_table.{0} = sampled_table.{0}".format(i) for i in group_by_columns),
        sample_partition='\n'.join("AND sampled_table.{0} = '{1}'".format(i, partition[i]) for i in partition),
        full_partition='WHERE {0} '.format(' AND '.join(
            " full_table.{0} = '{1}' ".format(i, partition[i]) for i in partition)) if len(partition) > 0 else '',
        ds=partition.get('ds', '~'),
        select=' UNION ALL\n        '.join("SELECT stratum, {0} FROM error".format(i) for i in aggregation_columns)
        if not weighted else
        ' UNION ALL\n        '.join("SELECT '{0}', stratum, {0} FROM error".format(i) for i in aggregation_columns),
    ))
