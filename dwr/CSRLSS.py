# =============================
# Our Major competitors:
# Congressional Sampling (CS)
# RL Sampling (RL)
# Sample+Seek (SS)
# =============================
from dwr import *

# ----------------------
# Constants
# ----------------------
CS = 'cs'
RL = 'rl'
SS = 'ss'

# ----------------------
# Create allocation tables
# ----------------------
logging.info('Create cs_allocation table')
hiveql.execute("""
        CREATE TABLE IF NOT EXISTS cs_allocation (
            stratum string,
            sample_rate double
        )
        PARTITIONED BY (
            ds string,
            table_name string,
            default_rate double
        )
""")
logging.info('Create rl_allocation table')
hiveql.execute("""
        CREATE TABLE IF NOT EXISTS rl_allocation (
            stratum string,
            sample_rate double
        )
        PARTITIONED BY (
            ds string,
            table_name string,
            default_rate double
        )
""")


# ----------------------
# Utility Functions
# ----------------------
def reallocate(budget, count={}, alloc={}):
    """
    Reallocate sample to reach full memory usage when bounded strata happen.
    :param budget: memory budget - total number of records allowed in sample
    :param count: number of records for each stratum
    :param alloc: current allocation
    :return: new allocation using full memory
    """
    # Recursive ver.
    # new_alloc = {}
    # c = list(count.keys())
    # sum_alloc = sum(i for i in alloc.values())
    # old_len = 0
    # while old_len != len(c):
    #     old_len = len(c)
    #     for i in list(c):
    #         if alloc[i] > count[i]:
    #             new_alloc[i] = count[i]
    #             budget -= count[i]
    #             sum_alloc -= alloc[i]
    #             c.remove(i)
    # for i in c:
    #     new_alloc[i] = budget * alloc[i] / sum_alloc

    # Take up to population count to handle bounded strata
    for i in count.keys():
        alloc[i] = min(count[i], alloc[i])
    sum_alloc = sum(i for i in alloc.values())

    return {i: budget*alloc[i]/sum_alloc for i in count.keys()}


# ----------------------
# Allocation
# ----------------------
def cs_allocation(count, sample_rate, count1=None, count2=None):
    """
    Allocate the sample rate according to Congressional Sampling (cs)
    :param count: number of records for each stratum
    :param count1: number of records group by attribute A
    :param count2: number of records group by attribute B
    :param sample_rate: sample rate
    :return: sample rate for each stratum
    """
    logging.info('cs allocation')

    # Check input
    if sample_rate <= 0:
        logging.error('Invalid sample rate')
        return {}

    # Statistics
    budget = min(sample_rate, 1) * sum(count.values())  # total memory budget
    c = list(count.keys())                              # list of strata

    # Basic Congress
    basic = {}
    for i in list(c):
        # choose either house or senate (before scaling)
        basic[i] = max(count[i] * sample_rate, budget / len(c))

    if count1 is None or count2 is None:
        scale = budget / sum(basic.values())
        alloc = {i: basic[i] * scale for i in count.keys()}
    else:
        congress = {}
        sga = {}
        sgb = {}
        for i in count.keys():
            a, b = i.rsplit("_", 1)
            sga[i] = (budget / len(count1.keys())) * (count[i] / count1[a])
            sgb[i] = (budget / len(count2.keys())) * (count[i] / count2[b])
            congress[i] = max(basic[i], sga[i], sgb[i])
        scale = budget / sum(congress.values())
        # print("Basic unscaled:")
        # print(basic)
        # print("Sga:")
        # print(sga)
        # print("Sgb:")
        # print(sgb)
        # print("Congress unscaled:")
        # print(congress)
        # print("Congress scaled:")
        # print({i: congress[i]*scale for i in count.keys()})
        alloc = {i: congress[i] * scale for i in count.keys()}

    # final_alloc = reallocate(budget, count, alloc)
    final_alloc = alloc.copy()
    # return {i: min(1.0, final_alloc[i] / count[i]) for i in count.keys()}
    return {i: min(1.0, max(final_alloc[i], 5) / count[i]) for i in count.keys()}


def rl_allocation(sample_rate, count, coeff, coeff_a=None, coeff_b=None):
    """
    Allocate the sample rate according to RL paper, using RSD/RSE (CV)
    :param count: number of records for each stratum
    :param sample_rate: sample rate
    :param coeff: relative standard deviation (rsd) - rules applied when mean is between (-1,1)
    :return: sample rate for each stratum
    """
    logging.info('rl allocation')

    # Check input
    if sample_rate <= 0:
        logging.error('Invalid sample rate')
        return {}

    # give the strata with 0 coeff the min coeff value
    min_coeff = min(i for i in coeff.values() if isinstance(i, Number) and i > 0)
    coeff = {i: coeff[i] if isinstance(coeff[i], Number)
                            and coeff[i] > 0 else min_coeff for i in coeff}

    # Statistics
    budget = min(sample_rate, 1) * sum(count.values())  # total memory budget
    flat = {}
    sum_coeff_ab = sum(coeff.values())

    for i in count.keys():
        # flat (unscaled)
        flat[i] = budget * (coeff[i] / sum_coeff_ab)

    if coeff_a is None or coeff_b is None:
        scale = budget / sum(flat.values())
        alloc = {i: flat[i]*scale for i in count.keys()}
    else:
        hierarchical = {}
        sga = {}
        sgb = {}
        coeff_a_b = {a: 0 for a in coeff_a.keys()}
        coeff_b_a = {b: 0 for b in coeff_b.keys()}

        # get coeff_a_b and coeff_b_a
        for i in count.keys():
            a, b = i.rsplit("_",1)
            coeff_a_b[a] += coeff[i]
            coeff_b_a[b] += coeff[i]

        for i in count.keys():
            a, b = i.rsplit("_",1)
            sum_coeff_a = sum(coeff_a.values())
            sum_coeff_b = sum(coeff_b.values())
            sga[i] = budget * (coeff_a[a] / sum_coeff_a) * (coeff[i] / coeff_a_b[a])
            sgb[i] = budget * (coeff_b[b] / sum_coeff_b) * (coeff[i] / coeff_b_a[b])
            hierarchical[i] = max(flat[i], sga[i], sgb[i])
        scale = budget / sum(hierarchical.values())
        alloc = {i: hierarchical[i]*scale for i in count.keys()}

    # final_alloc = reallocate(budget, count, alloc)
    final_alloc = alloc.copy()
    return {i: min(1.0, max(final_alloc[i], 5) / count[i]) for i in count.keys()}


# ----------------------
# Sampler
# ----------------------
def cs_sample(sample_rate: float, table_name: str, group_by: Union[str, dict, list], partition: dict = {},
              frequency: Union[str, dict] = None,
              freq1: Union[str, dict] = None,
              freq2: Union[str, dict] = None,
              select: str = None, sample_table: str = None,
              overwrite: bool = False) -> str:
    """
    Create cs stratifying sampled table
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
    logging.info('Create {0} cs sample table from {1}'.format(sample_rate, table_name))
    if isinstance(group_by, str):
        group_by = [group_by]

    if not sample_table:
        sample_table = sample_table_name(
            base_name=table_name,
            sample_type=CS,
            sample_rate=sample_rate,
            partition=partition,
            group_by=group_by,
        )

    # check if sampled table exists
    if not overwrite and exists(sample_table, partition):
        return sample_table

    # get full table's schema
    schema_column, partition_column = schema(table_name)

    # check the partitions
    if len(set(partition.keys()) ^ set(partition_column.keys())) > 0:
        logging.warning("Sampled table and original table have different partition!")

    # cs allocation
    if overwrite or not exists('cs_allocation',
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

        # cs alloc
        if freq1 is None or freq2 is None:
            alloc = cs_allocation(frequency, sample_rate)
        else:
            alloc = cs_allocation(frequency, sample_rate, freq1, freq2)

        logging.info('Store allocation to cs_allocation table')
        # language=HQL
        hiveql.execute("""
            INSERT OVERWRITE TABLE cs_allocation 
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
            LEFT JOIN cs_allocation
                ON cs_allocation.ds = '{ds}'
                AND cs_allocation.table_name = '{table_name}'
                AND cs_allocation.default_rate = '{sample_rate}'
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
            group_by='cs_allocation.stratum = {0}'.format(
                " || '_' || ".join('full_table.{0}'.format(i) for i in group_by)
            ),
        )

    # language=HQL - draw sample
    #hiveql.execute("""
    logging.info("""
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


def rl_sample(sample_rate, table_name, group_by=None, aggregate_column=None, partition={},
              frequency=None,
              coefficient=None,
              coeff_a=None,
              coeff_b=None,
              select=None,
              sample_table=None,
              overwrite=False):
    """
    Create RL sampled table.
    :param sample_rate: sample rate.
    :param table_name: base table.
    :param group_by: group by columns - stratification based on group by columns.
    :param aggregate_column: [Optional] Aggregate column. If empty, frequency and coefficient must be provided.
    :param partition: [Optional] partition column(s) of sampled table. Empty to use same partition as base table.
    :param frequency: [Optional] frequency of each group.
                        Dict: precalculated / String: query / None: default, aggregate_column must be provided.
    :param coefficient: [Optional] variance coefficient of each group.
                        Dict: precalculated / String: query / None: default, aggregate_column must be provided.
    :param select: [Optional] customized query to draw sample.
    :param sample_table: [Optional] customized name for sampled table.
    :param overwrite: [Optional] overwrite sampled table if it exists. Default is NOT.
    :return: name of the sampled table.
    """
    logging.info('Create {0} rl sample table from {1}'.format(sample_rate, table_name))
    if isinstance(group_by, str):
        group_by = [group_by]

    if not sample_table:
        sample_table = sample_table_name(
            base_name=table_name,
            sample_type=RL,
            sample_rate=sample_rate,
            partition=partition,
            group_by=group_by,
            aggregate=aggregate_column,
        )

    # check if sampled table exists
    if not overwrite and exists(sample_table, partition):
        return sample_table

    # get the frequency of each stratum
    if frequency is None:
        # language=HQL
        frequency = """
            SELECT 
                {strata},
                COUNT(*),
                ABS( AVG({aggregate}) ),
                STDDEV({aggregate})                              
            FROM {table_name} 
            {partition}
            GROUP BY {group_by}
        """.format(
            table_name=table_name,
            strata=" || '_' || ".join(i for i in group_by),
            aggregate=aggregate_column,
            group_by=', '.join(i for i in group_by),
            partition='WHERE {0}'.format(' AND '.join("{0} = '{1}'".format(i, partition[i]) for i in partition))
            if len(partition) > 0 else '',
        )

    if isinstance(frequency, str):
        logging.info('Get statistic from full table')
        hiveql.execute(frequency)
        result = hiveql.fetchall()
        frequency = {r[0]: r[1] for r in result}
        coefficient = {r[0]: r[3] / max(r[2], 1) for r in result}

    if not isinstance(frequency, dict) or not isinstance(coefficient, dict):
        logging.error('frequency and coefficient are invalid')
        return None

    # rl alloc
    if coeff_a is None or coeff_b is None:
        alloc = rl_allocation(sample_rate, frequency, coefficient)
    else:
        alloc = rl_allocation(sample_rate=sample_rate,
                              count=frequency,
                              coeff=coefficient,
                              coeff_a=coeff_a, coeff_b=coeff_b)

    # language=HQL - insert new allocation
    # hiveql.execute("""
    logging.info("""
        INSERT OVERWRITE TABLE rl_allocation 
        PARTITION(ds='{ds}', table_name='{table_name}', default_rate={default_rate})
        VALUES {values} 
    """.format(
        ds=partition.get('ds', '~'),
        table_name=table_name,
        default_rate=sample_rate,
        values=',\n'.join(["('{0}', {1})".format(k.replace("'", "\\'"), alloc[k]) for k in alloc]),
    ))

    # get full table's schema
    schema_column, partition_column = schema(table_name)

    # check the partitions
    if len(set(partition.keys()) ^ set(partition_column.keys())) > 0:
        logging.warning("Sampled table and original table have different partition!")

    logging.info('Create sampled table')
    # language=HQL - Create sampled table
    # hiveql.execute("""
    logging.info("""
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
                COALESCE(sample_rate, '{sample_rate}') AS sample_rate
            FROM {table_name} full_table        
            LEFT JOIN rl_allocation
                ON rl_allocation.ds = '{ds}'
                AND rl_allocation.table_name = '{table_name}'
                AND rl_allocation.default_rate = '{sample_rate}'
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
            group_by='rl_allocation.stratum = {0}'.format(
                " || '_' || ".join(['full_table.{0}'.format(i) for i in set.union(*group_by)])
                if isinstance(group_by, set) else
                " || '_' || ".join('full_table.{0}'.format(i) for i in group_by)
            ),
        )

    # language=HQL - draw sample
    #hiveql.execute("""
    logging.info("""
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


def ss_sample(sample_rate, table_name, aggregate_column=None, partition={},
              total=None,
              sum_value=None,
              sample_table=None,
              overwrite=False):
    """
    Create sample+seek sampled table
    :param sample_rate: sample rate
    :param table_name: base table
    :param partition: partition columns of sampled table. Optional: empty to use sample partition as base table.
    :param sample_table: name of sampled table. Optional: empty to use default sample table name.
    :param overwrite: overwrite if sampled table exist. Optional: default is NOT OVERWRITE
    :return: name of sampled table
    """
    logging.info('Create {0} ss sample table from {1}'.format(sample_rate, table_name))

    if not sample_table:
        sample_table = sample_table_name(
            base_name=table_name,
            sample_type=SS,
            sample_rate=sample_rate,
            partition=partition,
            aggregate=aggregate_column,
        )

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
    #hiveql.execute("""
    logging.info("""    
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
    #hiveql.execute("""
    logging.info("""    
        INSERT OVERWRITE TABLE {sampled_table} {sampled_partition}
        SELECT * FROM
        (
            SELECT 
            {schema},
            '{sample_size}' * {agg} /'{sum_value}' sample_rate
            FROM {full_table}
        ) t
        WHERE rand() <= t.sample_rate
        {full_partition}
    """.format(
        full_table=table_name,
        sampled_table=sample_table,
        sample_size=sample_rate*total,
        schema=','.join(i for i in schema_column if i not in partition),
        sampled_partition='PARTITION ({0})'.format(
            ','.join("{0}='{1}'".format(i, partition[i]) for i in partition)
        ) if len(partition) > 0 else '',
        agg=aggregate_column,
        sum_value=sum_value,
        full_partition='\n'.join(
            "AND {0} = '{1}'".format(i, partition[i]) for i in partition
        ) if len(partition) > 0 else '',
    ))
    return sample_table


# ----------------------
# Test
# ----------------------
# c = {"a1_b1": 300, "a1_b2": 300, "a1_b3": 150, "a2_b3": 250}
# c1 = {"a1": 750, "a2": 250}
# c2 = {"b1": 300, "b2": 300, "b3": 400}
# result = cs_allocation(c, 0.1, c1, c2)
#
# print("Congress Final:")
# print(result)

# count = {"a1_b1": 250, "a1_b2": 250, "a2_b1": 250, "a2_b2": 250}
# c = {"a1_b1": 33.19, "a1_b2": 16.53, "a2_b1": 46.45, "a2_b2": 21.54}
# c1 = {"a1": 29.38, "a2": 41.85}
# c2 = {"b1": 39.67, "b2": 18.64}
# result = rl_allocation(0.1, count, c, c1, c2)
#
# print("RL Final:")
# print(result)
