import logging
import json
import functools
import gzip
import os
import requests
import shutil
import threading
from datetime import datetime
from urllib.parse import quote
from flask import g, request, redirect, Response
from flask_appbuilder import expose

from fileRobot_client.FileRobotClientFactory import fileRobotClientFactory
from fileRobot_common.conf.FileRobotConfiguration import FileRobotConfiguartion
from fileRobot_common.conf.FileRobotVars import FileRobotVars
from fileRobot_common.exception.FileRobotException import FileRobotException

from superset import app, db, appbuilder
from superset.cache import TokenCache, FileRobotCache
from superset.message import *
from superset.exception import SupersetException, ParameterException, LoginException
from superset.models import HDFSConnection
from superset.utils import human_size
from .base import BaseSupersetView, catch_exception, json_response

config = app.config

mutex = threading.Lock()


def catch_hdfs_exception(f):
    """
    A decorator to label an endpoint as an API. Catches uncaught exceptions and
    return the response in the JSON format
    """
    def wraps(self, *args, **kwargs):
        try:
            try:
                return f(self, *args, **kwargs)
            except FileRobotException as fe:
                if fe.status == 401:
                    self.login_filerobot()
                    return f(self, *args, **kwargs)
                else:
                    return json_response(status=fe.status, code=fe.returnCode,
                                         message=fe.message)
        except FileRobotException as fe:
            logging.exception(fe)
            return json_response(status=fe.status, code=fe.returnCode, message=fe.message)
        except SupersetException as se:
            logging.exception(se)
            return json_response(status=500, message=str(se), code=se.code)
        except AttributeError as e:
            logging.exception(e)
            if 'AnonymousUserMixin' in str(e):
                return json_response(status=500, message=NO_USER, code=1)
            else:
                return json_response(status=500, message=str(e), code=1)
        except Exception as e:
            logging.exception(e)
            return json_response(status=500, message=str(e), code=1)
    return functools.update_wrapper(wraps, f)


class HDFSBrowser(BaseSupersetView):
    route_base = '/hdfs'
    file_block_size = config.get('FILE_BLOCK_LENGTH')
    special_folders = ['.', '..', '.Trash']
    gzip_avg_ratio = 5

    @catch_exception
    @expose('/')
    def render_html(self):
        self.update_redirect()
        if g.user.get_id() is None:
            return redirect(appbuilder.get_url_for_login)
        try:
            client = self.get_client()
            client.mkdir('/user', g.user.username)
        except Exception as e:
            logging.error('Failed to create default user path for [{}]. '
                          .format(g.user.username) + str(e))
        return self.render_template('superset/hdfsList.html')

    @catch_hdfs_exception
    @expose('/login/', methods=['GET'])
    def login(self):
        self.login_filerobot()
        return json_response(message=LOGIN_FILEROBOT_SUCCESS)

    @catch_hdfs_exception
    @expose('/logout/', methods=['GET'])
    def logout(self):
        httpfs = self.get_httpfs()
        client = self.get_client()
        logging.info('[HDFS] logout {} with httpfs={} from Filerobot'
                     .format(g.user.username, httpfs))
        client.logout()
        FileRobotCache.delete(g.user.username, httpfs)
        return json_response(message=LOGOUT_FILEROBOT_SUCCESS)

    @catch_hdfs_exception
    @expose('/list/', methods=['GET'])
    def list(self):
        client = self.get_client()
        path = request.args.get('path')
        page_num = request.args.get('page_num', 1)
        page_size = request.args.get('page_size')
        logging.info('[HDFS] list {} with page_num={} and page_size={}'
                     .format(path, page_num, page_size))
        response = client.list(path, page_num, page_size)
        data = json.loads(response.text)
        for file in data['files']:
            file['size'] = human_size(file['size'])
        return json_response(data=data, status=response.status_code)

    @catch_hdfs_exception
    @expose('/download/', methods=['GET'])
    def download(self):
        client = self.get_client()
        files = request.args.get('files', None)
        folders = request.args.get('folders', None)
        files = files.split(',') if files else []
        folders = folders.split(',') if folders else []
        data, filename = self.multi_download(client, {'files': files, 'folders': folders})
        response = Response(bytes(data), content_type='application/octet-stream')
        response.headers['Content-Disposition'] = "attachment; filename=" + filename
        return response

    @catch_hdfs_exception
    @expose('/upload/', methods=['POST'])
    def upload(self):
        """Upload files by form"""
        client = self.get_client()
        dest_path = request.args.get('dest_path')
        redirect_url = '/hdfs/?current_path={}'.format(dest_path)
        for f in request.files.getlist('list_file'):
            filename = f.filename
            file_path = os.path.join(dest_path, filename)
            try:
                logging.info('[HDFS] touch {} in {}'.format(filename, dest_path))
                client.touch(dest_path, filename)
            except Exception as e:
                logging.exception(e)
                redirect_url = '{}&error_message={}'.format(redirect_url, str(e))
            else:
                try:
                    while True:
                        file_content = f.read(self.file_block_size)
                        if file_content:
                            logging.info('[HDFS] append {} with {} bytes'
                                         .format(file_path, len(file_content)))
                            client.append(file_path, {'files': [filename, file_content]})
                        else:
                            break
                except Exception as e:
                    logging.exception(e)
                    logging.info('[HDFS] remove {}'.format(file_path))
                    client.remove(file_path, forever=True)
                    redirect_url = '{}&error_message={}'.format(redirect_url, str(e))
        return redirect(redirect_url)

    @catch_hdfs_exception
    @expose('/upload_in_body/', methods=['POST'])
    def upload_in_body(self):
        """Upload file by request body"""
        client = self.get_client()
        f = request.data
        dest_path = request.args.get('dest_path')
        file_name = request.args.get('file_name')
        logging.info('[HDFS] upload {} to {} where data in request body'
                     .format(file_name, dest_path))
        response = client.upload(dest_path, [('files', (file_name, f))])
        return json_response(message=eval(response.text).get("message"),
                             status=response.status_code)

    @catch_hdfs_exception
    @expose('/remove/', methods=['POST'])
    def remove(self):
        client = self.get_client()
        args = self.get_request_data()
        paths = ';'.join(args.get('path'))
        forever = args.get('forever', 'false')
        logging.info('[HDFS] remove {} with forever={}'.format(args.get('path'), forever))
        response = client.remove(paths, forever)
        return json_response(message=eval(response.text).get("message"),
                             status=response.status_code)

    @catch_hdfs_exception
    @expose('/move/', methods=['POST'])
    def move(self):
        client = self.get_client()
        args = self.get_request_data()
        paths = ';'.join(args.get('path'))
        dest_path = args.get('dest_path')
        logging.info('[HDFS] move {} to {}'.format(args.get('path'), dest_path))
        response = client.move(paths, dest_path)
        return json_response(message=eval(response.text).get("message"),
                             status=response.status_code)

    @catch_hdfs_exception
    @expose('/copy/', methods=['POST'])
    def copy(self):
        client = self.get_client()
        args = self.get_request_data()
        paths = ';'.join(args.get('path'))
        dest_path = args.get('dest_path')
        logging.info('[HDFS] copy {} to {}'.format(args.get('path'), dest_path))
        response = client.copy(paths, dest_path)
        return json_response(message=eval(response.text).get("message"),
                             status=response.status_code)

    @catch_hdfs_exception
    @expose('/mkdir/', methods=['GET'])
    def mkdir(self):
        client = self.get_client()
        path = request.args.get('path')
        dir_name = request.args.get('dir_name')
        logging.info('[HDFS] mkdir {} in {}'.format(dir_name, path))
        response = client.mkdir(path, dir_name)
        return json_response(message=eval(response.text).get("message"),
                             status=response.status_code)

    @catch_hdfs_exception
    @expose('/preview/', methods=['GET'])
    def preview(self):
        client = self.get_client()
        path = request.args.get('path')
        offset = request.args.get('offset', 0)
        length = request.args.get('length', 16 * 1024)
        logging.info('[HDFS] preview {} with offset={}, length={}'
                     .format(path, offset, length))
        response = client.preview(path, offset=offset, length=length)
        return json_response(data=response.text,
                             status=response.status_code)

    @catch_hdfs_exception
    @expose('/chmod/', methods=['POST'])
    def chmod(self):
        client = self.get_client()
        args = self.get_request_data()
        paths = ';'.join(args.get('path'))
        mode = args.get('mode')
        recursive = args.get('recursive')
        recursive = True if recursive else False
        logging.info('[HDFS] chmod {} with mode={}, recursive={}'
                     .format(args.get('path'), mode, recursive))
        response = client.chmod(paths, mode, recursive=recursive)
        return json_response(message=eval(response.text).get("message"),
                             status=response.status_code)

    @catch_hdfs_exception
    @expose('/touch/', methods=['GET'])
    def touch(self):
        client = self.get_client()
        path = request.args.get('path')
        filename = request.args.get('filename')
        logging.info('[HDFS] touch {} in {}'.format(filename, path))
        response = client.touch(path, filename)
        return json_response(message=eval(response.text).get("message"),
                             status=response.status_code)

    @catch_hdfs_exception
    @expose('/modify/', methods=['POST'])
    def modify(self):
        client = self.get_client()
        path = request.args.get('path')
        file = request.data
        logging.info('[HDFS] modify file {}'.format(path))
        response = client.modify(path, file)
        return json_response(message=eval(response.text).get("message"),
                             status=response.status_code)

    @classmethod
    def login_filerobot(cls, hdfs_conn_id=None, httpfs=None):
        def get_login_args(hdfs_conn_id=None, httpfs=None):
            httpfs = cls.get_httpfs(hdfs_conn_id) if not httpfs else httpfs
            access_token = TokenCache.get(g.user.username, g.user.password2)
            return {'server': config.get('FILE_ROBOT_SERVER'),
                    'username': g.user.username,
                    'password': g.user.password2,
                    'httpfs': httpfs,
                    'access_token': access_token}

        def do_login(server='', username='', password='', httpfs='', access_token=None):
            if not server:
                raise ParameterException(NO_FILEROBOT_SERVER)
            if not password:
                raise ParameterException(MISS_PASSWORD_FOR_FILEROBOT)
            conf = FileRobotConfiguartion()
            conf.set(FileRobotVars.FILEROBOT_SERVER_ADDRESS.varname, server)
            client = fileRobotClientFactory.getInstance(conf)
            logging.debug('[HDFS] login {} with httpfs={} to Filerobot'
                         .format(username, httpfs))
            response = client.login(username, password, httpfs, access_token=access_token)
            return client, response

        args = get_login_args(hdfs_conn_id, httpfs)
        client, response = do_login(**args)
        if response.status_code != requests.codes.ok:
            raise LoginException(LOGIN_FILEROBOT_FAILED)
        return client

    @classmethod
    def get_httpfs(cls, hdfs_conn_id=None):
        if hdfs_conn_id:
            conn = db.session.query(HDFSConnection).filter_by(id=hdfs_conn_id).first()
            if not conn:
                raise ParameterException(NO_HDFS_CONNECTION)
            return conn.httpfs
        else:
            return config.get('DEFAULT_HTTPFS')

    @classmethod
    def get_client(cls, hdfs_conn_id=None):
        return cls.login_filerobot(hdfs_conn_id)

    @classmethod
    def read_file(cls, client, path):
        data = bytearray()
        offset = 0
        length = cls.file_block_size
        while True:
            logging.info('[HDFS] read {} with offest={}, length={}'
                         .format(path, offset, length))
            response = client.read(path, offset, length)
            content = response.data
            len_con = len(content)
            if content:
                data.extend(content)
                offset += len_con
            if not content or len_con < length:
                break
        return data

    @classmethod
    def read_folder(cls, client, path, file_num=1000):
        """Read the files' content of ~'path' into a bytearray.
        If data size exceeds the limit, will not download it.
        """
        response = client.list(path, page_size=file_num)
        files_json = json.loads(response.text)
        files = files_json.get('files')

        sum_size = 0
        for file in files:
            sum_size += file.get('size')
        if sum_size > cls.gzip_avg_ratio * config.get('MAX_DOWNLOAD_SIZE'):
            logging.info("The files' size in [{}] is {},  and will not download them"
                         .format(path, sum_size))
            return None

        need_line_feed = False
        all_data = bytearray()
        for file in files:
            if file.get('type') == 'file':
                file_path = file.get('path')
                data = cls.read_file(client, file_path)
                if need_line_feed:
                    all_data.extend(b'\t\n')
                all_data.extend(data)
                need_line_feed = True
            else:
                pass
        return all_data

    @classmethod
    def multi_download(cls, client, paths):
        """Download multiple files and folders
        :param client: Filerobot client
        :param paths: {'files': [], 'folders': []}
        :return: data of zip file and filename
        """
        if not isinstance(paths, dict):
            raise ParameterException("Parameter 'paths' should be 'dict'")
        files = paths.get('files')
        folders = paths.get('folders')
        len_files = len(files)
        len_folders = len(folders)

        if len_files == 0 and len_folders == 0:
            raise ParameterException("Parameter 'paths' should not be empty")

        ts = datetime.now().isoformat()
        ts = ts.replace('-', '').replace(':', '').split('.')[0]
        root_path = os.path.join(config.get("GLOBAL_FOLDER"), "download")
        if os.path.exists(root_path):
            shutil.rmtree(root_path)

        if len_files == 1 and len_folders == 0:  # Only one file
            hdfs_path = files[0]
            filename = os.path.basename(hdfs_path)
            data = cls.read_file(client, hdfs_path)
            return data, quote(filename, encoding="utf-8")
        elif len_files == 0 and len_folders == 1:  # Only one folder
            hdfs_path = folders[0]
            local_path = os.path.join(root_path, os.path.basename(hdfs_path))
            cls.download_folder(client, hdfs_path, local_path)
            data, filename = cls.zip_folder(local_path)
            return data, filename
        else:
            work_path = os.path.join(root_path, 'hdfs_{}'.format(ts))
            if not os.path.exists(work_path):
                os.makedirs(work_path)
            for file in files:
                cls.download_file(client,
                                  file,
                                  os.path.join(work_path, os.path.basename(file)))
            for folder in folders:
                cls.download_folder(client,
                                    folder,
                                    os.path.join(work_path, os.path.basename(folder)))
            data, filename = cls.zip_folder(work_path)
            return data, filename

    @classmethod
    def download_file(cls, client, hdfs_path, local_path):
        logging.info("[HDFS] download hdfs file [{}] to local [{}]"
                     .format(hdfs_path, local_path))
        data = cls.read_file(client, hdfs_path)
        with open(local_path, "wb") as f:
            f.write(data)

    @classmethod
    def download_folder(cls, client, hdfs_path, local_path):
        logging.info("[HDFS] download hdfs folder [{}] to local [{}]"
                     .format(hdfs_path, local_path))
        os.makedirs(local_path)

        response = client.list(hdfs_path, page_size=1000)
        files_json = json.loads(response.text)
        paths = files_json.get('files')

        for path in paths:
            name = path.get('name')
            if name not in cls.special_folders:
                ptype = path.get('type')
                hdfs_path = path.get('path')
                local_path2 = os.path.join(local_path, name)
                if ptype == 'dir':
                    cls.download_folder(client, hdfs_path, local_path2)
                elif ptype == 'file':
                    cls.download_file(client, hdfs_path, local_path2)

    @classmethod
    def zip_folder(cls, folder_path):
        logging.info('[HDFS] zip [{0}] to [{0}.zip]'.format(folder_path))
        shutil.make_archive(folder_path, 'zip', folder_path)
        zip_file = '{}.zip'.format(folder_path)
        with open(zip_file, 'rb') as f:
            data = f.read()
        return data, quote(os.path.basename(zip_file), encoding="utf-8")

    @classmethod
    def gzip_compress(cls, data, level=5):
        uncompressed_len = len(data)
        start_time = datetime.now()
        data = gzip.compress(data, level)
        end_time = datetime.now()
        compressed_len = len(data)
        logging.info('Compressed data from {} to {} with {}s time spend'
                     .format(uncompressed_len, compressed_len, (end_time-start_time)))
        return data
