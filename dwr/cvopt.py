from dwr import *

VOILA = 'voila'
VOILA_CUBE = 'voila_cube'
VOILA2 = 'voila2'
VOILA_INF = 'voila_infinity'
SASG = 'single_agg_single_gb'
SAMG = 'single_agg_multi_gb'
MASG = 'multi_agg_single_gb'
MAMG = 'multi_agg_multi_gb'

# # ds list and sample rate parameters
# ds_list = ['10gb']

# openaq exp
sample_rates = [0.01]

# trips exp
# sample_rates = [0.001, 0.01, 0.05, 0.1]

# ----------------------
# Create metadata tables
# ----------------------

logging.info('Create senate_allocation table')
# language=HQL
hiveql.execute("""
    CREATE TABLE IF NOT EXISTS senate_allocation (
        stratum string,
        sample_rate double 
    )        
    PARTITIONED BY (
        ds string,
        table_name string,
        default_rate double
    )
""")

logging.info('Create stratified_allocation table')
# language=HQL
hiveql.execute("""
    CREATE TABLE IF NOT EXISTS stratified_allocation (
        stratum string,
        sample_rate double 
    )        
    PARTITIONED BY (
        table_name string,
        default_rate double
    )
""")

logging.info('Store allocation to voila_allocation table')
# language=HQL - Create voila allocation table
hiveql.execute("""
    CREATE TABLE IF NOT EXISTS voila_allocation (
        stratum string,
        sample_rate double 
    )        
    PARTITIONED BY (
        ds string,
        table_name string,
        default_rate double
    )
""")

logging.info('Store allocation to voila2_allocation table')
# language=HQL - Create voila allocation table
hiveql.execute("""
    CREATE TABLE IF NOT EXISTS voila2_allocation (
        stratum string,
        sample_rate double 
    )        
    PARTITIONED BY (
        ds string,
        table_name string,
        default_rate double
    )
""")

logging.info('Store allocation to voila_infinity table')
# language=HQL - Create voila allocation table
hiveql.execute("""
    CREATE TABLE IF NOT EXISTS voila_infinity (
        stratum string,
        sample_rate double 
    )        
    PARTITIONED BY (
        ds string,
        table_name string,
        default_rate double
    )
""")

logging.info('Create sampled_aggregate_error table')
# language=HQL
hiveql.execute("""
    CREATE TABLE IF NOT EXISTS sampled_aggregate_error (
        agg string,
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


# ---------------------
# Group by sampling
# ---------------------


def sasg_sample(sample_rate, table_name, group_by=None, aggregate_column=None,
                partition={}, frequency=None, coefficient=None, select=None, sample_table=None, overwrite=False,
                mode=None, alloc_table_name=None):
    """
    Create voila stratifying sampled table.
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
    :param mode: None for CVOPT-L2. Value for CVTOPT-INF.
    :return: name of the sampled table.
    """
    logging.info('Create {0} voila sample table from {1}'.format(sample_rate, table_name))
    if isinstance(group_by, str):
        group_by = [group_by]

    if isinstance(aggregate_column, str):
        aggregate_column = [aggregate_column]

    if mode == "infinity":
        sample_type = VOILA_INF
    elif mode == "voila2":
        sample_type = VOILA2
    else:
        sample_type = VOILA

    if not sample_table:
        sample_table = sample_table_name(
            base_name=table_name,
            sample_type=sample_type,
            sample_rate=sample_rate,
            partition=partition,
            group_by=group_by,
            aggregate=aggregate_column,
        )

    # check if sampled table exists
    if not overwrite and exists(sample_table, partition):
        return sample_table
    else:
        # language=HQL
        hiveql.execute("DROP TABLE {0}".format(sample_table))

    # get the frequency of each stratum
    if frequency is None:
        # language=HQL
        frequency = """
            SELECT 
                {strata},
                COUNT(*),
                ABS(STDDEV({aggregate}) / AVG({aggregate}))
            FROM {table_name} 
            {partition}
            GROUP BY {group_by}
        """.format(
            table_name=table_name,
            strata=" || '_' || ".join(i for i in group_by),
            aggregate=aggregate_column[0],
            group_by=', '.join(i for i in group_by),
            partition='WHERE {0}'.format(' AND '.join("{0} = '{1}'".format(i, partition[i]) for i in partition))
            if len(partition) > 0 else '',
        )

    if isinstance(frequency, str):
        logging.info('Get statistic from full table')
        hiveql.execute(frequency)
        result = hiveql.fetchall()
        frequency = {r[0]: r[1] for r in result}
        coefficient = {r[0]: r[2] for r in result}

    if not isinstance(frequency, dict) or not isinstance(coefficient, dict):
        logging.error('frequency and coefficient are invalid')
        return None

    # cvopt alloc
    if mode == "voila2":
        alloc = cvopt2_allocation(sample_rate, frequency, coefficient)
        alloc_table = "voila2_allocation"
    elif mode == "infinity":
        alloc = cvopt_infinity_allocation(sample_rate, frequency, coefficient)
        alloc_table = "voila_infinity"
    else:
        alloc = cvopt_allocation(sample_rate, frequency, coefficient)
        alloc_table = "voila_allocation"

    # language=HQL - insert new allocation
    # hiveql.execute("""
    logging.info("""
        INSERT OVERWRITE TABLE {alloc_table} 
        PARTITION(ds='{ds}', table_name='{table_name}', default_rate={default_rate})
        VALUES {values} 
    """.format(
        alloc_table=alloc_table,
        ds=partition.get('ds', '~'),
        table_name=sample_table,
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
            LEFT JOIN {alloc_table}
                ON {alloc_table}.ds = '{ds}'
                AND {alloc_table}.table_name = '{sample_table}'
                AND {alloc_table}.default_rate = '{sample_rate}'
                AND {alloc_table}{group_by}
            WHERE RAND() <= COALESCE(sample_rate, '{sample_rate}')
            {partition}
        """.format(
            alloc_table=alloc_table,
            ds=partition.get('ds', '~'),
            table_name=table_name,
            sample_table=sample_table,
            sample_rate=sample_rate,
            schema=', '.join('full_table.{0}'.format(i) for i in schema_column if i not in partition),
            partition='\n'.join(
                "AND full_table.{0} = '{1}'".format(i, partition[i]) for i in partition
            ) if len(partition) > 0 else '',
            group_by='.stratum = {0}'.format(
                " || '_' || ".join(['full_table.{0}'.format(i) for i in set.union(*group_by)])
                if isinstance(group_by, list) and isinstance(group_by[0], set) else
                " || '_' || ".join('full_table.{0}'.format(i) for i in group_by)
            ),
        )

    # language=HQL - draw sample
    # hiveql.execute("""
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


def masg_sample(sample_rate, table_name, group_by=None, aggregate_column=None, aggregate_weight=None,
                partition={}, frequency=None, coefficient=None, select=None, sample_table=None, overwrite=False):
    """
    Create voila stratifying sampled table.
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
    # get the frequency of each stratum
    if frequency is None:
        # language=HQL
        frequency = """
            SELECT 
                {strata},
                COUNT(*),
                {coefficient}                    
            FROM {table_name} 
            {partition}
            GROUP BY {group_by}
        """.format(
            table_name=table_name,
            strata=" || '_' || ".join(i for i in group_by),
            coefficient=",\n                    ".join(
                "ABS(STDDEV({0}) / AVG({0}))".format(i) for i in aggregate_column),
            group_by=', '.join(i for i in group_by),
            partition='WHERE {0}'.format(' AND '.join("{0} = '{1}'".format(i, partition[i]) for i in partition))
            if len(partition) > 0 else '',
        )

    if isinstance(frequency, str):
        logging.info('Get statistic from full table')
        hiveql.execute(frequency)
        result = hiveql.fetchall()
        frequency = {r[0]: r[1] for r in result}
        coefficient = {r[0]: r[2:] for r in result}

    if not isinstance(frequency, dict) or not isinstance(coefficient, dict):
        logging.error('frequency and coefficient are invalid')
        return None

    if aggregate_weight is None:
        coefficient = {i: sqrt(sum(coeff ** 2 for coeff in coefficient[i] if isinstance(coeff, Number))) for i in
                       coefficient}
    else:
        coefficient = {i: sqrt(sum(aggregate_weight[j] * coefficient[i][j] ** 2 for j in range(len(coefficient[i]))
                                   if isinstance(coefficient[i][j], Number))) for i in coefficient}

    return sasg_sample(sample_rate, table_name, group_by, aggregate_column, partition, frequency, coefficient, select,
                       sample_table, overwrite)


def samg_sample(sample_rate, table_name, group_by=None, aggregate_column=None, select=None, sample_table=None,
                overwrite=False):
    """
    Create voila sample for single aggregate multiple group by.
    :param sample_rate: sample rate.
    :param table_name: base table.
    :param group_by: group by columns - stratification based on group by columns.
    :param aggregate_column: [Optional] Aggregate column. If empty, frequency and coefficient must be provided.
    :param partition: [Optional] partition column(s) of sampled table. Empty to use same partition as base table.
    :param coefficient: [Optional] variance coefficient of each group.
                        Dict: precalculated / String: query / None: default, aggregate_column must be provided.
    :param select: [Optional] customized query to draw sample.
    :param sample_table: [Optional] customized name for sampled table.
    :param overwrite: [Optional] overwrite sampled table if it exists. Default is NOT.
    :return: name of the sampled table.
    """

    group_by_attr = []
    for i in group_by:
        group_by_attr = list(set().union(i))

    # language=HQL - get the frequency of each stratum
    frequency = """
        SELECT
            {stratum},
            f,
            SQRT(f*f * ij*ij * ({sum}))
        FROM (
            SELECT 
                {group_by_ij}, 
                count({aggregate}) f,
                stddev({aggregate}) ij
            FROM {table_name}
            GROUP BY {group_by_ij}
        ) c,
        {a_i}
        WHERE {where}              
    """.format(
        table_name=table_name,
        aggregate=aggregate_column,
        # stratum=" || '_' || ".join(['c.{0}'.format(i) for i in set.union(*group_by)]),
        stratum=" || '_' || ".join(['c.{0}'.format(i) for i in group_by_attr]),
        group_by_ij=', '.join(i for i in group_by_attr),
        sum=" + ".join(['1 / (a{0} * a{0})'.format(i) for i in range(1, len(group_by))]),
        a_i=",".join("""        
        (
            SELECT
                {group_by_i},
                count({aggregate}) * avg({aggregate}) a{i}
            FROM {table_name}
            GROUP BY {group_by_i}
        ) t{i} """.format(
            table_name=table_name,
            aggregate=aggregate_column,
            # group_by_i=', '.join(j for j in group_by[i]),
            group_by_i=', '.join(j for j in group_by[i]),
            i=i,
        ) for i in range(1, len(group_by))),
        where=' AND '.join(' AND '.join(['t{0}.{1} = c.{1}'.format(i, j) for j in group_by[i]])
                           for i in range(1, len(group_by))),
    )

    # print(frequency)
    # sys.exit()

    logging.info('Get statistic from full table')
    hiveql.execute(frequency)
    result = hiveql.fetchall()
    frequency = {r[0]: r[1] for r in result}
    coefficient = {r[0]: r[2] for r in result}

    return sasg_sample(sample_rate, table_name, group_by_attr, aggregate_column, {}, frequency, coefficient, select,
                       sample_table, overwrite)


# ----------------------------------------------------------------------------
# WARNING: MAMG ONLY SUPPORTS TWO AGGREGATES FOR NOW
# ----------------------------------------------------------------------------
def mamg_sample(sample_rate, table_name, group_by=None, aggregate_column=None, aggregate_weight=None,
                partition={}, frequency=None, coefficient=None, select=None, sample_table=None, overwrite=False):
    """
        Create voila sample for single aggregate multiple group by.
        :param sample_rate: sample rate.
        :param table_name: base table.
        :param group_by: group by columns - stratification based on group by columns.
        :param aggregate_column: [Optional] Aggregate column. If empty, frequency and coefficient must be provided.
        :param partition: [Optional] partition column(s) of sampled table. Empty to use same partition as base table.
        :param coefficient: [Optional] variance coefficient of each group.
                            Dict: precalculated / String: query / None: default, aggregate_column must be provided.
        :param select: [Optional] customized query to draw sample.
        :param sample_table: [Optional] customized name for sampled table.
        :param overwrite: [Optional] overwrite sampled table if it exists. Default is NOT.
        :return: name of the sampled table.
        """

    group_by_attr = []
    for i in group_by:
        group_by_attr = list(set().union(i))

    # language=HQL - get the frequency of each stratum
    frequency = """
        SELECT
            {stratum},
            f,
            SQRT(c.f * c.f * ({sum}))
        FROM (
            SELECT 
                {group_by_ij}, 
                count(*) f,
                stddev({agg1}) std_1,
                stddev({agg2}) std_2
            FROM {table_name}
            GROUP BY {group_by_ij}
        ) c,
        {a_i}
        WHERE {where}              
    """.format(
        table_name=table_name,
        agg1=aggregate_column[0],
        agg2=aggregate_column[1],
        # stratum=" || '_' || ".join(['c.{0}'.format(i) for i in set.union(*group_by)]),
        stratum=" || '_' || ".join(['c.{0}'.format(i) for i in group_by_attr]),
        group_by_ij=', '.join(i for i in group_by_attr),
        sum=" + ".join(['1 / (n{0} * n{0}) * ( POW(std_1 / ma{0},2) + POW(std_2 / mb{0},2) )'.format(i) for i in
                        range(1, len(group_by))]),
        a_i=",".join("""        
        (
            SELECT
                {group_by_i},
                count(*) n{i},
                AVG({agg1}) ma{i},
                AVG({agg2}) mb{i}
            FROM {table_name}
            GROUP BY {group_by_i}
        ) t{i} """.format(
            table_name=table_name,
            agg1=aggregate_column[0],
            agg2=aggregate_column[1],
            # group_by_i=', '.join(j for j in group_by[i]),
            group_by_i=', '.join(j for j in group_by[i]),
            i=i,
        ) for i in range(1, len(group_by))),
        where=' AND '.join(' AND '.join(['t{0}.{1} = c.{1}'.format(i, j) for j in group_by[i]])
                           for i in range(1, len(group_by))),
    )
    # ----------------------------------------------------------------------------
    # WARNING: MAMG ONLY SUPPORTS TWO AGGREGATES FOR NOW
    # ----------------------------------------------------------------------------

    # print(frequency)
    # sys.exit()

    logging.info('Get statistic from full table')
    hiveql.execute(frequency)
    result = hiveql.fetchall()
    frequency = {r[0]: r[1] for r in result}
    coefficient = {r[0]: r[2] for r in result}

    return sasg_sample(sample_rate, table_name, group_by_attr, aggregate_column, {}, frequency, coefficient, select,
                       sample_table, overwrite)


def samg_combination_sample(sample_rate, table_name, group_by, aggregate_column,
                            partition={}, select=None, sample_table=None, overwrite=False):
    """
    Create voila sample for single aggregate multiple group by.
    :param sample_rate: sample rate.
    :param table_name: base table.
    :param group_by: group by columns - stratification based on group by columns.
    :param aggregate_column: [Optional] Aggregate column. If empty, frequency and coefficient must be provided.
    :param partition: [Optional] partition column(s) of sampled table. Empty to use same partition as base table.
    :param coefficient: [Optional] variance coefficient of each group.
                        Dict: precalculated / String: query / None: default, aggregate_column must be provided.
    :param select: [Optional] customized query to draw sample.
    :param sample_table: [Optional] customized name for sampled table.
    :param overwrite: [Optional] overwrite sampled table if it exists. Default is NOT.
    :return: name of the sampled table.
    """

    if len(group_by) != len(aggregate_column):
        logging.error("Group_by and aggregate_column must have the same amount of elements")
        return False

    for i in range(len(group_by)):
        logging.info('Get statistic from full table for query {0}'.format(i))
        # language=HQL - query i^th
        hiveql.execute("""
            SELECT
                {group_by},
                COUNT(*)
                {cv}
            FROM {table_name}
            GROUP BY {group_by}
        """.format(
            table_name=table_name,
            group_by=', '.join(group_by[i]),
            cv='SQRT({0})'.format(' + '.join('POW(STDDEV({0}) / AVG({0}), 2)'.format(i) for v in aggregate_column[i])),
        ))
        result = hiveql.fetchall()
        frequency = {r[0]: r[1] for r in result}
        coefficient = {r[0]: r[2] for r in result}

    return sasg_sample(sample_rate, table_name, group_by, aggregate_column, partition, frequency, coefficient, select,
                       sample_table, overwrite)


def cvopt_cube(sample_rate, table_name, group_by, aggregate_column, select=None, sample_table=None, overwrite=False):
    """
    Voila cube
    :param sample_rate:
    :param table_name:
    :param group_by:
    :param aggregate_column:
    :param select:
    :param sample_table:
    :param overwrite:
    :return:
    """
    cube = []
    for x in (itertools.combinations(group_by, i) for i in range(len(group_by) + 1)):
        for i in x:
            j = list(i)
            cube.append(j)

    if len(aggregate_column) == 1:
        return samg_sample(
            sample_rate=sample_rate,
            table_name=table_name,
            aggregate_column=aggregate_column,
            select=select,
            sample_table=sample_table,
            overwrite=overwrite,
            group_by=cube,
        )
    else:
        return mamg_sample(
            sample_rate=sample_rate,
            table_name=table_name,
            aggregate_column=aggregate_column,
            select=select,
            sample_table=sample_table,
            overwrite=overwrite,
            group_by=cube,
        )


def cvopt_cube_naive(sample_rate, table_name, group_by=None, aggregate_column=None,
                     partition={}, sample_table=None, overwrite=False):
    logging.info('Create {0} voila sample table from {1}'.format(sample_rate, table_name))
    if isinstance(group_by, str):
        group_by = [group_by]

    if isinstance(aggregate_column, str):
        aggregate_column = [aggregate_column]

    if not sample_table:
        sample_table = sample_table_name(
            base_name=table_name,
            sample_type=VOILA_CUBE,
            sample_rate=sample_rate,
            partition=partition,
            group_by=group_by,
            aggregate=aggregate_column,
        )

    # check if sampled table exists
    if not overwrite and exists(sample_table, partition):
        return sample_table
    else:
        # language=HQL
        hiveql.execute("DROP TABLE {0}".format(sample_table))

    logging.info('Alloc by {0}'.format(group_by[0]))
    # language=HQL
    frequency = """
             SELECT 
                 {group_by},
                 COUNT(*),
                 ABS(STDDEV({aggregate}) / AVG({aggregate}))
             FROM {table_name} 
             GROUP BY {group_by}
         """.format(
        table_name=table_name,
        aggregate=aggregate_column[0],
        group_by=group_by[0],
    )

    logging.info('Get statistic from full table')
    hiveql.execute(frequency)
    result = hiveql.fetchall()
    frequency = {r[0]: r[1] for r in result}
    coefficient = {r[0]: r[2] for r in result}

    # senate alloc
    alloc_a = cvopt_allocation(sample_rate, frequency, coefficient)

    logging.info('Alloc by {0}'.format(group_by[1]))
    # language=HQL
    frequency = """
             SELECT 
                 {group_by},
                 COUNT(*),
                 ABS(STDDEV({aggregate}) / AVG({aggregate}))
             FROM {table_name} 
             GROUP BY {group_by}
         """.format(
        table_name=table_name,
        aggregate=aggregate_column[0],
        group_by=group_by[1],
    )

    logging.info('Get statistic from full table')
    hiveql.execute(frequency)
    result = hiveql.fetchall()
    frequency = {r[0]: r[1] for r in result}
    coefficient = {r[0]: r[2] for r in result}

    # senate alloc
    alloc_b = cvopt_allocation(sample_rate, frequency, coefficient)

    logging.info('Alloc by {0}'.format(group_by))
    # language=HQL
    frequency = """
              SELECT 
                  {group_by},
                  COUNT(*),
                  ABS(STDDEV({aggregate}) / AVG({aggregate}))
              FROM {table_name} 
              GROUP BY {group_by}
          """.format(
        table_name=table_name,
        aggregate=aggregate_column[0],
        group_by=', '.join(i for i in group_by),
    )

    logging.info('Get statistic from full table')
    hiveql.execute(frequency)
    result = hiveql.fetchall()
    frequency = {(r[0], r[1]): r[2] for r in result}
    coefficient = {(r[0], r[1]): r[3] for r in result}

    # senate alloc
    alloc_ab = cvopt_allocation(sample_rate, frequency, coefficient)

    for k in alloc_ab.keys():
        alloc_ab[k] = max(alloc_ab[k], alloc_a[k[0]], alloc_b[k[1]])
    scale = sum(frequency.values()) * sample_rate / sum(alloc_ab[i] * frequency[i] for i in alloc_ab.keys())
    for k in alloc_ab.keys():
        alloc_ab[k] = alloc_ab[k] * scale

    # language=HQL - insert new allocation
    hiveql.execute("""
            INSERT OVERWRITE TABLE voila_allocation 
            PARTITION(ds='{ds}', table_name='{table_name}', default_rate={default_rate})
            VALUES {values} 
        """.format(
        ds=partition.get('ds', '~'),
        table_name=sample_table,
        default_rate=sample_rate,
        values=',\n'.join(["('{0}_{1}', {2})".format(k[0], k[1], alloc_ab[k]) for k in alloc_ab]),
    ))

    # get full table's schema
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
    # language=HQL
    select = """
            SELECT 
                {schema},
                COALESCE(sample_rate, '{sample_rate}') AS sample_rate
            FROM {table_name} full_table        
            LEFT JOIN voila_allocation
                ON voila_allocation.ds = '{ds}'
                AND voila_allocation.table_name = '{sample_table}'
                AND voila_allocation.default_rate = '{sample_rate}'
                AND {group_by}
            WHERE RAND() <= COALESCE(sample_rate, '{sample_rate}')
            {partition}
        """.format(
        ds=partition.get('ds', '~'),
        table_name=table_name,
        sample_table=sample_table,
        sample_rate=sample_rate,
        schema=', '.join('full_table.{0}'.format(i) for i in schema_column if i not in partition),
        partition='\n'.join(
            "AND full_table.{0} = '{1}'".format(i, partition[i]) for i in partition
        ) if len(partition) > 0 else '',
        group_by='voila_allocation.stratum = {0}'.format(
            " || '_' || ".join(['full_table.{0}'.format(i) for i in set.union(*group_by)])
            if isinstance(group_by, list) and isinstance(group_by[0], set) else
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
