"""Unit tests for Pilot"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import unittest

from datetime import datetime

from superset import db
from superset.views.dataset import SqlMetricInlineView
from superset.models.dataset import SqlMetric
from superset.models.dataset import Dataset
from tests.base_tests import SupersetTestCase
from tests.base_tests import PageMixin


class SqlMetricCRUDTests(SupersetTestCase, PageMixin):
    require_examples = True

    def __init__(self, *args, **kwargs):
        super(SqlMetricCRUDTests, self).__init__(*args, **kwargs)
        self.view = SqlMetricInlineView()

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_listdata(self):
        one_dataset = db.session.query(Dataset).first()
        if not one_dataset:
            return
        self.kwargs['dataset_id'] = one_dataset.id
        self.check_base_param(no_check=True)
        sql_metric_data_list = self.real_value.get('data')

        for sql_metric_data in sql_metric_data_list:
            sql_metric = db.session.query(SqlMetric) \
                .filter_by(id=sql_metric_data.get('id')) \
                .first()
            assert sql_metric.metric_name == sql_metric_data['metric_name']
            assert sql_metric.expression == sql_metric_data['expression']

    def test_add_edit_delete(self):
        one_dataset = db.session.query(Dataset).first()
        if not one_dataset:
            return
        # add
        new_metric_name = 'new_metric_{}'.format(str(datetime.now()))
        json_data = {
            'metric_name': new_metric_name,
            'expression': 'AVG(num)',
            'metric_type': 'avg',
            'dataset_id': one_dataset.id
        }
        new_metric = self.view.populate_object(None, self.user.id, json_data)
        self.view.datamodel.add(new_metric)

        new_metric = db.session.query(SqlMetric)\
            .filter(
                SqlMetric.metric_name == new_metric_name,
                SqlMetric.dataset_id == one_dataset.id
            )\
            .first()
        assert new_metric is not None

        # edit
        json_data['metric_type'] = 'sum'
        json_data['expression'] = 'AVG(num)'
        obj = self.view.populate_object(new_metric.id, self.user.id, json_data)
        self.view.datamodel.edit(obj)
        new_metric = db.session.query(SqlMetric) \
            .filter(
                SqlMetric.metric_name == new_metric_name,
                SqlMetric.dataset_id == one_dataset.id
            ) \
            .first()
        assert new_metric.expression == json_data['expression']

        # delete
        self.view.datamodel.delete(new_metric)
        metric = db.session.query(SqlMetric).filter_by(id=new_metric.id).first()
        assert metric is None


if __name__ == '__main__':
    unittest.main()
