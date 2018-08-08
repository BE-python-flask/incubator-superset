"""Unit tests for Pilot"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import unittest
from datetime import datetime

from superset import db
from superset.views.dataset import TableColumnInlineView
from superset.models.dataset import TableColumn
from superset.models.dataset import Dataset
from tests.base_tests import SupersetTestCase
from tests.base_tests import PageMixin


class TableColumnCRUDTests(SupersetTestCase, PageMixin):
    require_examples = True

    def __init__(self, *args, **kwargs):
        super(TableColumnCRUDTests, self).__init__(*args, **kwargs)
        self.view = TableColumnInlineView()

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_listdata(self):
        one_dataset = db.session.query(TableColumn).first()
        if not one_dataset:
            return
        self.kwargs['dataset_id'] = one_dataset.dataset_id
        kwargs = self.kwargs
        self.real_value = self.view.get_object_list_data(**kwargs)
        real_data = self.real_value.get('data')

        for one_column in real_data:
            column = db.session.query(TableColumn) \
                .filter(
                    TableColumn.id == one_column.get('id'),
                    TableColumn.dataset_id == one_dataset.id) \
                .first()
            assert column is not None

    def test_add_edit_delete(self):
        one_dataset = db.session.query(Dataset).first()
        if not one_dataset:
            return
        # add
        new_column_name = 'new_column_{}'.format(str(datetime.now()))
        json_data = {
            'column_name': new_column_name,
            'expression': 'source',
            'verbose_name': new_column_name,
            'type': 'STRING',
            'filterable': True,
            'count_distinct': True,
            'max': False,
            'min': False,
            'sum': False,
            'avg': False,
            'groupby': True,
            'is_dttm': False,
            'dataset_id': one_dataset.id,
        }
        new_column = self.view.populate_object(None, self.user.id, json_data)
        self.view.datamodel.add(new_column)
        
        new_column = db.session.query(TableColumn) \
            .filter(
                TableColumn.column_name == new_column_name,
                TableColumn.dataset_id == one_dataset.id) \
            .first()
        assert new_column is not None

        #edit
        json_data['expression'] = 'target'
        obj = self.view.populate_object(new_column.id, self.user.id, json_data)
        self.view.datamodel.edit(obj)
        new_column = db.session.query(TableColumn) \
            .filter(
                TableColumn.column_name == new_column_name,
                TableColumn.dataset_id == one_dataset.id) \
            .first()
        assert new_column.expression == json_data['expression']

        #delete
        self.view.datamodel.delete(new_column)
        column = db.session.query(TableColumn).filter_by(id=new_column.id).first()
        assert column is None

if __name__ == '__main__':
    unittest.main()




