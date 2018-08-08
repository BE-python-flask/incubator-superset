"""Unit tests for Pilot"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import unittest
from datetime import datetime

from superset import db
from superset.models.connection import Database
from superset.views.connection import DatabaseView
from tests.base_tests import SupersetTestCase
from tests.base_tests import PageMixin


class DatabaseCRUDTests(SupersetTestCase, PageMixin):
    require_examples = True

    def __init__(self, *args, **kwargs):
        super(DatabaseCRUDTests, self).__init__(*args, **kwargs)
        self.view = DatabaseView()

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_list(self):
        pass

    def add_database(self, database_name, user_id):
        database = db.session.query(Database)\
            .filter_by(database_name=database_name)\
            .first()
        if not database:
            database = Database(
                database_name=database_name,
                online=True,
                sqlalchemy_uri='mysql://root:120303@localhost:3306/test?charset=utf8',
                args="{'connect_args': {} }",
                created_by_fk=user_id,
            )
            db.session.add(database)
            db.session.commit()

    def test_listdata(self):
        self.add_database('local_mysql', self.user.id)
        self.check_base_param()

        database_list = self.real_value.get('data')
        one_database = database_list[0]
        for database_dict in database_list:
            assert isinstance(database_dict.get('id'), int)

        queried_database = db.session.query(Database) \
            .filter_by(id=one_database.get('id')).first()
        assert one_database.get('backend') == queried_database.backend
        assert one_database.get('database_name') == queried_database.name

    def test_show(self):
        one_database = db.session.query(Database).first()
        attributes = self.view.get_show_attributes(one_database, self.user.id)
        assert one_database.id == attributes.get('id')
        assert one_database.database_name == attributes.get('database_name')
        assert one_database.sqlalchemy_uri == attributes.get('sqlalchemy_uri')

    def test_add_edit_delete(self):
        # add
        new_database = {
            'database_name': 'local_mysql'+str(datetime.now()),
            'sqlalchemy_uri': 'mysql://root:120303@localhost/pilot_test?charset=utf8',
            'description': 'test the database',
            'args': "{'connect_args': {} }"
        }
        new_database_obj = self.view.populate_object(None, self.user.id, new_database)
        self.view.pre_add(new_database_obj)
        self.view.datamodel.add(new_database_obj)

        added_database = db.session.query(Database) \
            .filter_by(database_name=new_database.get('database_name')) \
            .first()
        assert added_database is not None
        new_database_id = added_database.id

        # edit
        new_database['database_name'] = 'local_mysql_edited'+str(datetime.now())
        new_database_obj = self.view.populate_object(
            new_database_id, self.user.id, new_database)
        self.view.datamodel.edit(new_database_obj)

        edited_database = db.session.query(Database) \
            .filter_by(id=new_database_id) \
            .first()
        assert edited_database.database_name == new_database.get('database_name')
        assert edited_database.sqlalchemy_uri == new_database.get('sqlalchemy_uri')

        # delete
        new_database_obj = self.view.get_object(new_database_id)
        self.view.datamodel.delete(new_database_obj)
        target_database = db.session.query(Database) \
            .filter_by(id=new_database_id).first()
        assert target_database is None


if __name__ == '__main__':
    unittest.main()
