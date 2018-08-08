"""Unit tests for Pilot"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import unittest
from datetime import datetime

from flask_appbuilder.security.sqla.models import User
from superset import db
from superset.views.core import SliceModelView
from superset.models.core import Dashboard
from superset.models.core import Slice
from tests.base_tests import SupersetTestCase
from tests.base_tests import PageMixin


class SliceCRUDTests(SupersetTestCase, PageMixin):
    require_examples = True

    def __init__(self, *args, **kwargs):
        super(SliceCRUDTests, self).__init__(*args, **kwargs)
        self.view = SliceModelView()

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_listdata(self):
        self.check_base_param()

        slice_list = self.real_value.get('data')
        one_slice = None
        if slice_list:
            one_slice = slice_list[0]
        for slice_dic in slice_list:
            assert isinstance(slice_dic.get('favorite'), bool)
            assert '/p/explore/table/' in slice_dic.get('slice_url')

        rs = db.session.query(Slice, User.username) \
            .join(User, Slice.created_by_fk == User.id) \
            .filter(Slice.id == one_slice.get('id')) \
            .first()
        [target_slice, user_name] = rs

        assert one_slice.get('id') == target_slice.id
        assert one_slice.get('slice_url') == target_slice.slice_url
        assert one_slice.get('viz_type') == target_slice.viz_type
        assert one_slice.get('slice_name') == target_slice.slice_name
        assert one_slice.get('created_by_user') == user_name

    def test_show(self):
        one_slice = db.session.query(Slice).first()
        showed_attributes = self.view.get_show_attributes(one_slice, self.user.id)
        assert one_slice.id == showed_attributes.get('id')
        assert one_slice.slice_name == showed_attributes.get('slice_name')
        assert len(one_slice.dashboards) == len(showed_attributes['dashboards'])

    def test_online_and_offline(self):
        one_slice = db.session.query(Slice).first()
        if not one_slice:
            return
        # set online
        one_slice.online = True
        db.session.commit()
        edited_slice = db.session.query(Slice).filter_by(id=one_slice.id).first()
        assert edited_slice.online is True
        # set offline
        one_slice.online = False
        db.session.commit()
        edited_slice = db.session.query(Slice).filter_by(id=one_slice.id).first()
        assert edited_slice.online is False

    def test_add_edit_delete(self):
        # add
        new_slice_name = 'new_slice'
        new_slice = self.add_slice(new_slice_name, self.user.id)

        # edit
        one_dashboard = db.session.query(Dashboard).first()
        new_slice_name = 'edit_slice_{}'.format(str(datetime.now()))
        json_data = {
            'dashboards': [
                {'dashboard_title': one_dashboard.dashboard_title, 'id': one_dashboard.id},
            ],
            'slice_name': new_slice_name,
            'description': 'for test',
        }
        obj = self.view.populate_object(new_slice.id, self.user.id, json_data)
        self.view.datamodel.edit(obj)

        included_dashboards = new_slice.dashboards
        assert new_slice.description == json_data.get('description')
        assert new_slice.slice_name == json_data.get('slice_name')
        assert len(included_dashboards) == len(json_data['dashboards'])
        assert included_dashboards[0].id == json_data['dashboards'][0]['id']

        # delete
        self.view.datamodel.delete(new_slice)
        target_slice = db.session.query(Slice) \
            .filter_by(slice_name=new_slice_name).first()
        assert target_slice is None

    @staticmethod
    def add_slice(slice_name, user_id):
        slice = db.session.query(Slice).filter_by(slice_name=slice_name).first()
        if slice:
            slice.created_by_fk = user_id
        else:
            one_slice = db.session.query(Slice).first()
            slice = Slice(
                slice_name=slice_name,
                online=True,
                datasource_id=one_slice.datasource_id,
                datasource_type=one_slice.datasource_type,
                datasource_name=one_slice.datasource_name,
                database_id=one_slice.database_id,
                full_table_name=one_slice.full_table_name,
                viz_type=one_slice.viz_type,
                params=one_slice.params,
                created_by_fk=user_id
            )
            db.session.add(slice)
        db.session.commit()
        return slice


if __name__ == '__main__':
    unittest.main()





