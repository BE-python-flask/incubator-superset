"""Unit tests for Pilot"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import unittest
from datetime import datetime
from sqlalchemy import or_
from flask_appbuilder.security.sqla.models import User

from superset import db
from superset.views.dataset import DatasetModelView
from superset.models.dataset import Dataset
from superset import models
from tests.base_tests import SupersetTestCase
from tests.base_tests import PageMixin


class TableCRUDTests(SupersetTestCase, PageMixin):
    require_examples = True

    def __init__(self, *args, **kwargs):
        super(TableCRUDTests, self).__init__(*args, **kwargs)
        self.view = DatasetModelView()

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_listdata(self):
        self.check_base_param()
    
        query = db.session.query(Dataset, User) \
            .filter(
                or_(
                    Dataset.created_by_fk == User.id,
                    Dataset.online is True
                )
            )
        count = query.count()
        assert count == self.real_value.get('count')

        data = self.real_value.get('data')
        for line in data:
            assert '/p/explore/table/' in line.get('explore_url')
            assert isinstance(line.get('id'), int)
            assert isinstance(line.get('online'), bool)

    def test_show(self):
        one_dataset = db.session.query(Dataset).first()
        real_value = self.view.get_show_attributes(one_dataset, self.user.id)
        assert one_dataset.dataset_name == real_value.get('dataset_name')
        assert one_dataset.dataset_type == real_value.get('dataset_type')
        assert one_dataset.schema == real_value.get('schema')
        assert one_dataset.table_name == real_value.get('table_name')
        assert one_dataset.sql == real_value.get('sql')

    def test_add_edit_delete(self):
        one_dataset = db.session.query(Dataset).first()
        if not one_dataset:
            return
        # add
        new_dataset_name = 'new_dataset_{}'.format(str(datetime.now()))
        json_data = {
            'dataset_name': new_dataset_name,
            'schema': one_dataset.schema,
            'table_name': one_dataset.table_name,
            'sql': one_dataset.sql,
            'database_id': one_dataset.database_id,
            'description': 'this dataset is for test',
        }

        new_dataset = self.view.populate_object(None, self.user.id, json_data)
        self.view.datamodel.add(new_dataset)

        new_dataset = db.session.query(Dataset) \
            .filter_by(dataset_name=new_dataset_name) \
            .first()
        assert new_dataset.schema == json_data.get('schema')
        assert new_dataset.table_name == json_data.get('table_name')
        assert new_dataset.sql == json_data.get('sql')
        assert new_dataset.database_id == json_data.get('database_id')

        # edit
        json_data['dataset_name'] = 'edited_dataset_{}'.format(str(datetime.now()))
        obj = self.view.populate_object(new_dataset.id, self.user.id, json_data)
        self.view.datamodel.edit(obj)
        new_dataset = db.session.query(Dataset).filter_by(id=new_dataset.id).first()
        assert new_dataset.dataset_name == json_data['dataset_name']

        # delete
        self.view.datamodel.delete(new_dataset)
        dataset = db.session.query(Dataset).filter_by(id=new_dataset.id).first()
        assert dataset is None

if __name__ == '__main__':
    unittest.main()





