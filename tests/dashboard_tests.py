# -*- coding: utf-8 -*-
<<<<<<< HEAD
"""Unit tests for Superset"""
=======
# pylint: disable=C,R,W
"""Unit tests for Pilot"""
>>>>>>> 037ea32... considering of multiple threads, use database to store cas data
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import json
import unittest

<<<<<<< HEAD
from flask import escape

from superset import db, security_manager
from superset.connectors.sqla.models import SqlaTable
from superset.models import core as models
from .base_tests import SupersetTestCase


class DashboardTests(SupersetTestCase):

    requires_examples = True

    def __init__(self, *args, **kwargs):
        super(DashboardTests, self).__init__(*args, **kwargs)

    @classmethod
    def setUpClass(cls):
        pass

    def setUp(self):
        pass
=======
from datetime import datetime
from superset import db, sm
from superset.models import Dashboard
from superset.views.core import DashboardModelView
from tests.base_tests import SupersetTestCase
from tests.base_tests import PageMixin


class DashboardCRUDTests(SupersetTestCase, PageMixin):
    require_examples = True

    def __init__(self, *args, **kwargs):
        super(DashboardCRUDTests, self).__init__(*args, **kwargs)
        self.view = DashboardModelView()

    def setUp(self):
        self.login()
>>>>>>> 037ea32... considering of multiple threads, use database to store cas data

    def tearDown(self):
        pass

<<<<<<< HEAD
    def test_dashboard(self):
        self.login(username='admin')
        urls = {}
        for dash in db.session.query(models.Dashboard).all():
            urls[dash.dashboard_title] = dash.url
        for title, url in urls.items():
            assert escape(title) in self.client.get(url).data.decode('utf-8')

    def test_dashboard_modes(self):
        self.login(username='admin')
        dash = (
            db.session.query(models.Dashboard)
            .filter_by(slug='births')
            .first()
        )
        url = dash.url
        if dash.url.find('?') == -1:
            url += '?'
        else:
            url += '&'
        resp = self.get_resp(url + 'edit=true&standalone=true')
        self.assertIn('editMode&#34;: true', resp)
        self.assertIn('standalone_mode&#34;: true', resp)

    def test_save_dash(self, username='admin'):
        self.login(username=username)
        dash = db.session.query(models.Dashboard).filter_by(
            slug='births').first()
        data = {
            'css': '',
            'expanded_slices': {},
            'positions': dash.position_array,
            'dashboard_title': dash.dashboard_title,
        }
        url = '/superset/save_dash/{}/'.format(dash.id)
        resp = self.get_resp(url, data=dict(data=json.dumps(data)))
        self.assertIn('SUCCESS', resp)

    def test_save_dash_with_filter(self, username='admin'):
        self.login(username=username)
        dash = db.session.query(models.Dashboard).filter_by(
            slug='world_health').first()

        filters = {str(dash.slices[0].id): {'region': ['North America']}}
        default_filters = json.dumps(filters)
        data = {
            'css': '',
            'expanded_slices': {},
            'positions': dash.position_array,
            'dashboard_title': dash.dashboard_title,
            'default_filters': default_filters,
        }

        url = '/superset/save_dash/{}/'.format(dash.id)
        resp = self.get_resp(url, data=dict(data=json.dumps(data)))
        self.assertIn('SUCCESS', resp)

        updatedDash = db.session.query(models.Dashboard).filter_by(
            slug='world_health').first()
        new_url = updatedDash.url
        self.assertIn('region', new_url)

        resp = self.get_resp(new_url)
        self.assertIn('North America', resp)

    def test_save_dash_with_dashboard_title(self, username='admin'):
        self.login(username=username)
        dash = (
            db.session.query(models.Dashboard)
            .filter_by(slug='births')
            .first()
        )
        origin_title = dash.dashboard_title
        data = {
            'css': '',
            'expanded_slices': {},
            'positions': dash.position_array,
            'dashboard_title': 'new title',
        }
        url = '/superset/save_dash/{}/'.format(dash.id)
        self.get_resp(url, data=dict(data=json.dumps(data)))
        updatedDash = (
            db.session.query(models.Dashboard)
            .filter_by(slug='births')
            .first()
        )
        self.assertEqual(updatedDash.dashboard_title, 'new title')
        # bring back dashboard original title
        data['dashboard_title'] = origin_title
        self.get_resp(url, data=dict(data=json.dumps(data)))

    def test_copy_dash(self, username='admin'):
        self.login(username=username)
        dash = db.session.query(models.Dashboard).filter_by(
            slug='births').first()
        data = {
            'css': '',
            'duplicate_slices': False,
            'expanded_slices': {},
            'positions': dash.position_array,
            'dashboard_title': 'Copy Of Births',
        }

        # Save changes to Births dashboard and retrieve updated dash
        dash_id = dash.id
        url = '/superset/save_dash/{}/'.format(dash_id)
        self.client.post(url, data=dict(data=json.dumps(data)))
        dash = db.session.query(models.Dashboard).filter_by(
            id=dash_id).first()
        orig_json_data = dash.data

        # Verify that copy matches original
        url = '/superset/copy_dash/{}/'.format(dash_id)
        resp = self.get_json_resp(url, data=dict(data=json.dumps(data)))
        self.assertEqual(resp['dashboard_title'], 'Copy Of Births')
        self.assertEqual(resp['position_json'], orig_json_data['position_json'])
        self.assertEqual(resp['metadata'], orig_json_data['metadata'])
        self.assertEqual(resp['slices'], orig_json_data['slices'])

    def test_add_slices(self, username='admin'):
        self.login(username=username)
        dash = db.session.query(models.Dashboard).filter_by(
            slug='births').first()
        new_slice = db.session.query(models.Slice).filter_by(
            slice_name='Mapbox Long/Lat').first()
        existing_slice = db.session.query(models.Slice).filter_by(
            slice_name='Name Cloud').first()
        data = {
            'slice_ids': [new_slice.data['slice_id'],
                          existing_slice.data['slice_id']],
        }
        url = '/superset/add_slices/{}/'.format(dash.id)
        resp = self.client.post(url, data=dict(data=json.dumps(data)))
        assert 'SLICES ADDED' in resp.data.decode('utf-8')

        dash = db.session.query(models.Dashboard).filter_by(
            slug='births').first()
        new_slice = db.session.query(models.Slice).filter_by(
            slice_name='Mapbox Long/Lat').first()
        assert new_slice in dash.slices
        assert len(set(dash.slices)) == len(dash.slices)

        # cleaning up
        dash = db.session.query(models.Dashboard).filter_by(
            slug='births').first()
        dash.slices = [
            o for o in dash.slices if o.slice_name != 'Mapbox Long/Lat']
        db.session.commit()

    def test_remove_slices(self, username='admin'):
        self.login(username=username)
        dash = db.session.query(models.Dashboard).filter_by(
            slug='births').first()
        positions = dash.position_array[:-1]
        origin_slices_length = len(dash.slices)

        data = {
            'css': '',
            'expanded_slices': {},
            'positions': positions,
            'dashboard_title': dash.dashboard_title,
        }

        # save dash
        dash_id = dash.id
        url = '/superset/save_dash/{}/'.format(dash_id)
        self.client.post(url, data=dict(data=json.dumps(data)))
        dash = db.session.query(models.Dashboard).filter_by(
            id=dash_id).first()

        # verify slices data
        data = dash.data
        self.assertEqual(len(data['slices']), origin_slices_length - 1)

    def test_public_user_dashboard_access(self):
        table = (
            db.session
            .query(SqlaTable)
            .filter_by(table_name='birth_names')
            .one()
        )
        # Try access before adding appropriate permissions.
        self.revoke_public_access_to_table(table)
        self.logout()

        resp = self.get_resp('/slicemodelview/list/')
        self.assertNotIn('birth_names</a>', resp)

        resp = self.get_resp('/dashboardmodelview/list/')
        self.assertNotIn('/superset/dashboard/births/', resp)

        self.grant_public_access_to_table(table)

        # Try access after adding appropriate permissions.
        self.assertIn('birth_names', self.get_resp('/slicemodelview/list/'))

        resp = self.get_resp('/dashboardmodelview/list/')
        self.assertIn('/superset/dashboard/births/', resp)

        self.assertIn('Births', self.get_resp('/superset/dashboard/births/'))

        # Confirm that public doesn't have access to other datasets.
        resp = self.get_resp('/slicemodelview/list/')
        self.assertNotIn('wb_health_population</a>', resp)

        resp = self.get_resp('/dashboardmodelview/list/')
        self.assertNotIn('/superset/dashboard/world_health/', resp)

    def test_dashboard_with_created_by_can_be_accessed_by_public_users(self):
        self.logout()
        table = (
            db.session
            .query(SqlaTable)
            .filter_by(table_name='birth_names')
            .one()
        )
        self.grant_public_access_to_table(table)

        dash = db.session.query(models.Dashboard).filter_by(
            slug='births').first()
        dash.owners = [security_manager.find_user('admin')]
        dash.created_by = security_manager.find_user('admin')
        db.session.merge(dash)
        db.session.commit()

        assert 'Births' in self.get_resp('/superset/dashboard/births/')

    def test_only_owners_can_save(self):
        dash = (
            db.session
            .query(models.Dashboard)
            .filter_by(slug='births')
            .first()
        )
        dash.owners = []
        db.session.merge(dash)
        db.session.commit()
        self.test_save_dash('admin')

        self.logout()
        self.assertRaises(
            Exception, self.test_save_dash, 'alpha')

        alpha = security_manager.find_user('alpha')

        dash = (
            db.session
            .query(models.Dashboard)
            .filter_by(slug='births')
            .first()
        )
        dash.owners = [alpha]
        db.session.merge(dash)
        db.session.commit()
        self.test_save_dash('alpha')

    def test_owners_can_view_empty_dashboard(self):
        dash = (
            db.session
            .query(models.Dashboard)
            .filter_by(slug='empty_dashboard')
            .first()
        )
        if not dash:
            dash = models.Dashboard()
            dash.dashboard_title = 'Empty Dashboard'
            dash.slug = 'empty_dashboard'
        else:
            dash.slices = []
            dash.owners = []
        db.session.merge(dash)
        db.session.commit()

        gamma_user = security_manager.find_user('gamma')
        self.login(gamma_user.username)

        resp = self.get_resp('/dashboardmodelview/list/')
        self.assertNotIn('/superset/dashboard/empty_dashboard/', resp)

        dash = (
            db.session
            .query(models.Dashboard)
            .filter_by(slug='empty_dashboard')
            .first()
        )
        dash.owners = [gamma_user]
        db.session.merge(dash)
        db.session.commit()

        resp = self.get_resp('/dashboardmodelview/list/')
        self.assertIn('/superset/dashboard/empty_dashboard/', resp)
=======
    def test_listdata(self):
        resp_data = self.get_json_resp('/dashboard/listdata/')
        assert resp_data.get('status') == 200

        data = resp_data.get('data')
        assert 'count' in data
        assert 'order_column' in data
        assert 'order_direction' in data
        assert 'page' in data
        assert 'page_size' in data

        dash_list = data.get('data')
        for dash_dict in dash_list:
            assert isinstance(dash_dict.get('id'), int)
            assert isinstance(dash_dict.get('favorite'), bool)
            assert '/p/dashboard/' in dash_dict.get('url')

        if dash_list:
            one_dash = dash_list[0]
            queried_dash = Dashboard.get_object(id=one_dash.get('id'))
            assert one_dash.get('name') == queried_dash.name
            assert one_dash.get('url') == queried_dash.url
            assert one_dash.get('description') == queried_dash.description

    def test_show(self):
        one_dash = db.session.query(Dashboard).first()
        resp_data = self.get_json_resp('/dashboard/show/{}/'.format(one_dash.id))
        resp_data = resp_data.get('data')
        assert one_dash.id == resp_data.get('id')
        assert one_dash.name == resp_data.get('name')
        assert one_dash.description == resp_data.get('description')
        assert len(one_dash.slices) == len(resp_data.get('slices'))

    def test_add_edit_delete(self):
        # add
        new_slices = self.get_slices(2)
        ts = datetime.now().isoformat()
        ts = ts.replace('-', '').replace(':', '').split('.')[0]
        data = {'name': 'new_dashboard_{}'.format(ts),
                'description': 'new dashboard',
                'slices': self.view.slices_to_dict(new_slices)
                }
        resp = self.get_json_resp('/dashboard/add/', data=json.dumps(data))
        new_dash_id = resp.get('data').get('object_id')

        added_dash = Dashboard.get_object(id=new_dash_id)
        assert added_dash.name == data.get('name')
        assert added_dash.description == data.get('description')
        new_slices_name = [slc.slice_name for slc in new_slices]
        for slc in added_dash.slices:
            assert slc.slice_name in new_slices_name

        # edit
        new_slices = self.get_slices(3)
        ts = datetime.now().isoformat()
        ts = ts.replace('-', '').replace(':', '').split('.')[0]
        data = {'name': 'edited_dashboard_{}'.format(ts),
                'description': 'edit dashboard',
                'slices': self.view.slices_to_dict(new_slices)}
        resp = self.get_json_resp('/dashboard/edit/{}/'.format(new_dash_id),
                                  data=json.dumps(data))
        assert resp.get('status') == 200

        edited_dash = Dashboard.get_object(id=new_dash_id)
        assert edited_dash.name == data.get('name')
        assert edited_dash.description == data.get('description')
        new_slices_name = [slc.slice_name for slc in new_slices]
        for slc in edited_dash.slices:
            assert slc.slice_name in new_slices_name

        # delete
        resp = self.get_json_resp('/dashboard/delete/{}/'.format(new_dash_id))
        assert resp.get('status') == 200
        dash = Dashboard.get_object(id=new_dash_id)
        assert dash is None
>>>>>>> 037ea32... considering of multiple threads, use database to store cas data


if __name__ == '__main__':
    unittest.main()
