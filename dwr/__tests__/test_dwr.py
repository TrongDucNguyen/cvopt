import unittest

import dwr


class TestUtilities(unittest.TestCase):
    def test_sample_table_name_(self):
        dwr.sample_table_name(
            base_name='base_table_name',
            sample_type='sample_type',
            sample_rate=0.1,
            group_by=[{'a', 'b'}, {'c', 'd'}],
            aggregate='aggregate',
        )

    def test_sample_table_name(self):
        base_name = 'base_name'
        sample_type = 'sample_type'
        sample_rate = 0.1
        self.assertEqual('base_name_sampled_sample_type_10pct',
                         dwr.sample_table_name(base_name, sample_type, sample_rate))

        base_name = 'base_name'
        sample_type = 'sample_type'
        sample_rate = 0.57
        self.assertEqual('base_name_sampled_sample_type_57pct',
                         dwr.sample_table_name(base_name, sample_type, sample_rate))

        base_name = 'base_name'
        sample_type = 'sample_type'
        sample_rate = 0.1
        partition = 'ds'
        self.assertEqual('base_name_sampled_ds_sample_type_10pct',
                         dwr.sample_table_name(base_name, sample_type, sample_rate, partition))

        base_name = 'base_name'
        sample_type = 'sample_type'
        sample_rate = 0.57
        partition = ['ds']
        self.assertEqual('base_name_sampled_ds_sample_type_57pct',
                         dwr.sample_table_name(base_name, sample_type, sample_rate, partition))

        base_name = 'base_name'
        sample_type = 'sample_type'
        sample_rate = 0.57
        partition = {'ds': 'today'}
        self.assertEqual('base_name_sampled_ds_sample_type_57pct',
                         dwr.sample_table_name(base_name, sample_type, sample_rate, partition))

    def test_drop_partition(self):
        dwr.drop_partition("table_name", {'a': 1, 'b': 'i'})


class TestAllocation(unittest.TestCase):
    def test_senate_allocation(self):
        count = {
            'a': 100,
        }
        foo = {
            'a': 0.1
        }
        bar = dwr.senate_allocation(count, 0.1)
        unmatched_item = set(foo.items()) ^ set(bar.items())
        self.assertEqual(0, len(unmatched_item))

        count = {
            'a': 100,
            'b': 100,
            'c': 100,
        }
        foo = {
            'a': 10.0 / 100,
            'b': 10.0 / 100,
            'c': 10.0 / 100,
        }
        bar = dwr.senate_allocation(count, 0.1)
        unmatched_item = set(foo.items()) ^ set(bar.items())
        self.assertEqual(0, len(unmatched_item))

        count = {
            'a': 100,
            'b': 200,
            'c': 300,
        }
        foo = {
            'a': 20.0 / 100,
            'b': 20.0 / 200,
            'c': 20.0 / 300,
        }
        bar = dwr.senate_allocation(count, 0.1)
        unmatched_item = set(foo.items()) ^ set(bar.items())
        self.assertEqual(0, len(unmatched_item))

        count = {
            'a': 10,
            'b': 200,
            'c': 300,
        }
        foo = {
            'a': 10.0 / 10,
            'b': 20.5 / 200,
            'c': 20.5 / 300,
        }
        bar = dwr.senate_allocation(count, 0.1)
        unmatched_item = set(foo.items()) ^ set(bar.items())
        self.assertEqual(0, len(unmatched_item))

        count = {
            'a': 100,
            'b': 100,
            'c': 100,
        }
        bar = dwr.senate_allocation(count, 0)
        self.assertEqual(0, len(bar))

        count = {
            'a': 100,
            'b': 100,
            'c': 100,
        }
        bar = dwr.senate_allocation(count, -1)
        self.assertEqual(0, len(bar))

        count = {
            'a': 100,
            'b': 200,
            'c': 300,
        }
        foo = {
            'a': 1.0,
            'b': 1.0,
            'c': 1.0,
        }
        bar = dwr.senate_allocation(count, 1)
        unmatched_item = set(foo.items()) ^ set(bar.items())

        self.assertEqual(0, len(unmatched_item))
        count = {
            'a': 100,
            'b': 200,
            'c': 300,
        }
        foo = {
            'a': 1.0,
            'b': 1.0,
            'c': 1.0,
        }
        bar = dwr.senate_allocation(count, 2)
        unmatched_item = set(foo.items()) ^ set(bar.items())
        self.assertEqual(0, len(unmatched_item))

    def test_gbvoila_allocation(self):
        count = {
            'a': 100,
        }
        coeff = {
            'a': 100,
        }
        foo = {
            'a': 0.1
        }
        bar = dwr.cvopt_allocation(0.1, count, coeff)
        unmatched_item = set(foo.items()) ^ set(bar.items())
        self.assertEqual(0, len(unmatched_item))

        count = {
            'a': 100,
            'b': 100,
            'c': 100,
        }
        coeff = {
            'a': 100,
            'b': 100,
            'c': 100,
        }
        foo = {
            'a': 10.0 / 100,
            'b': 10.0 / 100,
            'c': 10.0 / 100,
        }
        bar = dwr.cvopt_allocation(0.1, count, coeff)
        unmatched_item = set(foo.items()) ^ set(bar.items())
        self.assertEqual(0, len(unmatched_item))

        count = {
            'a': 100,
            'b': 200,
            'c': 300,
        }
        coeff = {
            'a': 100,
            'b': 100,
            'c': 100,
        }
        foo = {
            'a': 20.0 / 100,
            'b': 20.0 / 200,
            'c': 20.0 / 300,
        }
        bar = dwr.cvopt_allocation(0.1, count, coeff)
        unmatched_item = set(foo.items()) ^ set(bar.items())
        self.assertEqual(0, len(unmatched_item))

        count = {
            'a': 10,
            'b': 200,
            'c': 300,
        }
        coeff = {
            'a': 100,
            'b': 100,
            'c': 100,
        }
        foo = {
            'a': 10.0 / 10,
            'b': 20.5 / 200,
            'c': 20.5 / 300,
        }
        bar = dwr.cvopt_allocation(0.1, count, coeff)
        unmatched_item = set(foo.items()) ^ set(bar.items())
        self.assertEqual(0, len(unmatched_item))

        bar = dwr.cvopt_allocation(0, count, coeff)
        self.assertEqual(0, len(bar))

        bar = dwr.cvopt_allocation(0, count, coeff)
        self.assertEqual(0, len(bar))

        count = {
            'a': 100,
            'b': 200,
            'c': 300,
        }
        coeff = {
            'a': 100,
            'b': 200,
            'c': 300,
        }
        foo = {
            'a': 1.0,
            'b': 1.0,
            'c': 1.0,
        }
        bar = dwr.cvopt_allocation(1, count, coeff)
        unmatched_item = set(foo.items()) ^ set(bar.items())
        self.assertEqual(0, len(unmatched_item))

        count = {
            'a': 100,
            'b': 200,
            'c': 300,
        }
        coeff = {
            'a': 100,
            'b': 200,
            'c': 300,
        }
        foo = {
            'a': 10.0 / 100,
            'b': 20.0 / 200,
            'c': 30.0 / 300,
        }
        bar = dwr.cvopt_allocation(0.1, count, coeff)
        unmatched_item = set(foo.items()) ^ set(bar.items())
        self.assertEqual(0, len(unmatched_item))

        count = {
            'a': 100,
            'b': 100,
            'c': 100,
        }
        coeff = {
            'a': 100,
            'b': 200,
            'c': 300,
        }
        foo = {
            'a': 5.0 / 100,
            'b': 10.0 / 100,
            'c': 15.0 / 100,
        }
        bar = dwr.cvopt_allocation(0.1, count, coeff)
        unmatched_item = set(foo.items()) ^ set(bar.items())
        self.assertEqual(0, len(unmatched_item))


class TestHiveOperators(unittest.TestCase):
    def test_schema(self):
        table_name = 'catalog_sales'
        schema, partition = dwr.schema(table_name)
        foo_schema = {u'cs_ship_hdemo_sk': u'bigint', u'cs_net_paid': u'double', u'cs_net_paid_inc_ship': u'double',
                      u'cs_ship_date_sk': u'bigint', u'cs_coupon_amt': u'double', u'cs_ship_customer_sk': u'bigint',
                      u'cs_ext_tax': u'double', u'cs_ship_mode_sk': u'bigint', u'cs_ext_wholesale_cost': u'double',
                      u'cs_ext_discount_amt': u'double', u'cs_sold_time_sk': u'bigint', u'cs_net_profit': u'double',
                      u'cs_bill_addr_sk': u'bigint', u'cs_list_price': u'double', u'ds': u'string',
                      u'cs_promo_sk': u'bigint', u'cs_net_paid_inc_tax': u'double', u'cs_ext_sales_price': u'double',
                      u'cs_sold_date_sk': u'bigint', u'cs_warehouse_sk': u'bigint', u'cs_bill_customer_sk': u'bigint',
                      u'cs_bill_cdemo_sk': u'bigint', u'cs_ship_addr_sk': u'bigint', u'cs_ship_cdemo_sk': u'bigint',
                      u'cs_wholesale_cost': u'double', u'cs_quantity': u'int', u'cs_call_center_sk': u'bigint',
                      u'cs_ext_ship_cost': u'double', u'cs_order_number': u'bigint', u'cs_item_sk': u'bigint',
                      u'cs_net_paid_inc_ship_tax': u'double', u'cs_sales_price': u'double',
                      u'cs_ext_list_price': u'double', u'cs_bill_hdemo_sk': u'bigint', u'cs_catalog_page_sk': u'bigint'}
        foo_partition = {u'ds': u'string'}
        unmatched_item = set(foo_schema.items()) ^ set(schema.items())
        self.assertEqual(0, len(unmatched_item))
        unmatched_item = set(foo_partition.items()) ^ set(partition.items())
        self.assertEqual(0, len(unmatched_item))

    def test_create_uniform_sample(self):
        sample_rate = 0.11
        table_name = 'catalog_sales'
        partition = {'ds': '2018-08-01'}
        dwr.uniform_sample(sample_rate, table_name, partition)

    def test_sample_evaluate(self):
        dwr.sample_evaluate(
            table_name='query2',
            sample_type='uniform',
            sample_rate=0.1,
            group_by_columns=['d_week_seq1'],
            aggregation_columns=['sun', 'mon', 'tue', 'wed', 'thu', 'fri', 'sat'],
            partition={'ds': '2018-08-01'},
        )
