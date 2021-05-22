from fman import Task, submit_task, fs, load_json, save_json, show_status_message, show_alert
from fman.fs import FileSystem, cached
from fman.url import basename as url_basename, join as url_join, splitscheme

from datetime import datetime
import errno
from io import UnsupportedOperation
from os.path import basename as path_basename, join as path_join
import stat
from tempfile import NamedTemporaryFile
from urllib.parse import urlparse

from .sftp import SftpConfig, SftpWrapper
from .ftp import FtpWrapper
from .config import Config, is_file, is_sftp, is_ftp


class SftpFileSystem(FileSystem):
    scheme = Config.scheme

    def get_default_columns(self, path):
        return (
            'core.Name', 'core.Size', 'core.Modified',
            'sftp_plugin.columns.Permissions', 'sftp_plugin.columns.Owner',
            'sftp_plugin.columns.Group')

    @cached
    def size_bytes(self, path):
        try:
            return self.cache.get(path, 'size_bytes')
        except KeyError:
            return None

    @cached
    def modified_datetime(self, path):
        try:
            return self.cache.get(path, 'modified_datetime')
        except KeyError:
            return None

    @cached
    def get_permissions(self, path):
        try:
            return self.cache.get(path, 'get_permissions')
        except KeyError:
            return ''

    @cached
    def get_owner(self, path):
        try:
            return self.cache.get(path, 'get_owner')
        except KeyError:
            return ''

    @cached
    def get_group(self, path):
        try:
            return self.cache.get(path, 'get_group')
        except KeyError:
            return ''

    def iterdir(self, path):
        try:
            if not path:
                for hostname in SftpConfig.get_all_hosts():
                    if hostname != '*':
                        yield hostname
            else:
                with SftpWrapper(self.scheme + path) as sftp:
                    for file_attributes in sftp.conn.listdir_iter(sftp.path):
                        self.save_stats(path_join(path, file_attributes.filename), file_attributes)
                        yield file_attributes.filename
        except:
            raise FileNotFoundError

    @cached
    def exists(self, path):
        if not path or self._is_server_name(path):
            return True
        try:
            self.cache.get(path, 'is_dir')
            return True
        except KeyError:
            return False

    @cached
    def is_dir(self, path):
        if not path or self._is_server_name(path):
            return True
        try:
            return self.cache.get(path, 'is_dir')
        except KeyError:
            return False

    @cached
    def _is_server_path(self, path):
        return path and len(path.split('/')) > 1 and self._is_server_name(path.split('/')[0])

    @cached
    def _is_server_name(self, path):
        return path in SftpConfig.get_all_hosts()
        
    def mkdir(self, path):
        if not self._is_server_path(path):
            show_status_message('Destination path invalid.')
            return
        if self.is_dir(path):
            show_status_message('Directory already exists.')
            return
        with SftpWrapper(self.scheme + path) as sftp:
            sftp.conn.mkdir(sftp.path)
        self.cache.put(path, 'is_dir', True)
        self.notify_file_added(path)
        show_status_message('Directory added.')

    def prepare_copy(self, src_url, dst_url):
        _, dst_path = splitscheme(dst_url)
        if is_sftp(dst_url) and not self._is_server_path(dst_path):
            show_status_message('Destination path invalid.')
            return []
        return self._prepare_copy(src_url, dst_url)

    def _prepare_copy(self, src_url, dst_url):
        _, src_path = splitscheme(src_url)
        _, dst_path = splitscheme(dst_url)
        files_to_copy = []

        if is_sftp(src_url) and is_file(dst_url):
            if self.is_dir(src_path):
                fs.mkdir(dst_url)
                files_to_copy = self.iterdir(src_path)
        elif is_file(src_url) and is_sftp(dst_url):
            if fs.is_dir(src_url):
                self.mkdir(dst_path)
                files_to_copy = fs.iterdir(src_url)
        elif is_sftp(src_url) and is_sftp(dst_url):
            if self.is_dir(src_path):
                self.mkdir(dst_path)
                files_to_copy = self.iterdir(src_path)

        if files_to_copy:
            for fname in files_to_copy:
                yield from self._prepare_copy(url_join(src_url, fname), url_join(dst_url, fname))
        else:
            yield Task('Copying ' + url_basename(src_url), fn=self.copy, args=(src_url, dst_url))

    def copy(self, src_url, dst_url):
        _, src_path = splitscheme(src_url)
        _, dst_path = splitscheme(dst_url)
            
        if is_sftp(src_url) and is_file(dst_url):
            with SftpWrapper(src_url) as sftp:
                sftp.conn.get(sftp.path, dst_path)    
        elif is_file(src_url) and is_sftp(dst_url):
            with SftpWrapper(dst_url) as sftp:
                sftp.conn.put(src_path, sftp.path)
            self.cache.put(dst_path, 'is_dir', False)
            self.notify_file_added(dst_path)
        elif is_sftp(src_url) and is_sftp(dst_url):
            with SftpWrapper(src_url) as src_sftp, SftpWrapper(dst_url) as dst_sftp:
                with src_sftp.conn.open(src_sftp.path) as src_file:
                    dst_sftp.conn.putfo(src_file, dst_sftp.path) 
            self.cache.put(dst_path, 'is_dir', False)
            self.notify_file_added(dst_path)  
        else:
            raise UnsupportedOperation

    def prepare_move(self, src_url, dst_url):
        _, dst_path = splitscheme(dst_url)
        if is_sftp(dst_url) and not self._is_server_path(dst_path):
            show_status_message('Destination path invalid.')
            return []
        return [Task('Moving ' + url_basename(src_url), fn=self.move, args=(src_url, dst_url))]

    def move(self, src_url, dst_url):
        _, src_path = splitscheme(src_url)
        _, dst_path = splitscheme(dst_url)

        # Rename on same server
        if is_sftp(src_url) and is_sftp(dst_url):
            with SftpWrapper(src_url) as src_sftp, SftpWrapper(dst_url) as dst_sftp:
                if src_sftp.host == dst_sftp.host:
                    src_sftp.conn.rename(src_sftp.path, dst_sftp.path)
                    self.cache.put(dst_path, 'is_dir', self.is_dir(src_path))
                    self.notify_file_added(dst_path)
                    self.cache.clear(src_path)
                    self.notify_file_removed(src_path)
                    return

        self.copy(src_url, dst_url)

        if is_sftp(src_url):
            for task in self.prepare_delete(src_path):
                submit_task(task)
        elif is_file(src_url):
            fs.delete(src_url)
        else:
            raise UnsupportedOperation

    def prepare_delete(self, path):
        if self._is_server_name(path):
            show_status_message('Server deletion not implemented.')
            return []
        return self._prepare_delete(path)

    def _prepare_delete(self, path):
        if self.is_dir(path):
            for fname in self.iterdir(path):
                yield from self._prepare_delete(path_join(path, fname))
        yield Task('Deleting ' + path_basename(path), fn=self.delete, args=(path,))

    def delete(self, path):
        with SftpWrapper(self.scheme + path) as sftp:
            if self.is_dir(path):
                sftp.conn.rmdir(sftp.path)
            else:
                sftp.conn.remove(sftp.path)
        self.cache.clear(path)
        self.notify_file_removed(path)

    def touch(self, path):
        if not self._is_server_path(path):
            raise OSError(errno.EADDRNOTAVAIL, "File path invalid")
        if self.exists(path):
            raise OSError(errno.EEXIST, "File exists")
        with SftpWrapper(self.scheme + path) as sftp, NamedTemporaryFile(delete=True) as tmp_file:
            sftp.conn.put(tmp_file.name, sftp.path)
        self.cache.put(path, 'is_dir', False)
        self.notify_file_added(path)


    def samefile(self, path1, path2):
        return path1 == path2

    def save_stats(self, path, file_attributes):
        is_dir = stat.S_ISDIR(file_attributes.st_mode)
        dt_mtime = datetime.utcfromtimestamp(file_attributes.st_mtime)
        st_mode = stat.filemode(file_attributes.st_mode)

        self.cache.put(path, 'is_dir', is_dir)
        self.cache.put(path, 'size_bytes', file_attributes.st_size)
        self.cache.put(path, 'modified_datetime', dt_mtime)
        self.cache.put(path, 'get_permissions', st_mode)
        self.cache.put(path, 'get_owner', file_attributes.st_uid)
        self.cache.put(path, 'get_group', file_attributes.st_gid)


class FtpFileSystem(FileSystem):
    scheme = Config.ftp_scheme

    def iterdir(self, path):
        # show_alert('iterdir '+path)
        try:
            if not path:
                yield from self._all_history_connections().keys()
            else:
                ftp_path =  self._history_connection(path) if self._is_history_connection(path) else self.scheme + path
                with FtpWrapper(ftp_path) as ftp:
                    for file_attributes in ftp.list_files():
                        name, size, timestamp, isdirectory, downloadable, islink, permissions = file_attributes
                        if name == '..':
                            continue
                        self.save_stats(path_join(path, name), file_attributes)
                        yield name
        except:
            raise FileNotFoundError

    @cached
    def exists(self, path):
        # show_alert('exists '+path)
        if not path or self._is_history_connection(path):
            return True
        try:
            self.cache.get(path, 'is_dir')
            return True
        except KeyError:
            return False

    @cached
    def is_dir(self, path):
        if path and self._is_history_connection(path):
            return True
        try:
            return self.cache.get(path, 'is_dir')
        except KeyError:
            return False

    def _all_history_connections(self):
        return load_json(Config.ftp_file, default={})

    def _save_history_connections(self, value=None):
        save_json(Config.ftp_file, value)

    def _history_connection(self, path):
        return self._all_history_connections().get(path, '')

    def _is_history_connection(self, path):
        return path in self._all_history_connections().keys()

    def mkdir(self, path):
        # show_alert('mkdir '+path)
        if not path:
            show_status_message('Destination path invalid.')
            return
        if self.is_dir(path):
            show_status_message('Directory already exists.')
            return
        if self._is_history_connection(urlparse(self.scheme + path).hostname):
            with FtpWrapper(self.scheme + path) as ftp:
                ftp.conn.mkd(ftp.home + ftp.path)
            self.cache.put(path, 'is_dir', True)
            self.notify_file_added(path)
            show_status_message('Directory added.')
        else:
            try:
                with FtpWrapper(self.scheme + path) as ftp:
                    history = self._all_history_connections()
                    history[ftp.host] = self.scheme + path
                    self._save_history_connections(history)
                    self.cache.put(ftp.host, 'is_dir', True)
                    self.notify_file_added(ftp.host)
                    show_status_message('Server added.')
            except:
                show_status_message('Server connection error.')
                show_alert('URL should be [user[:password]@]ftp.host[:port][/path/to/dir]')

    def prepare_copy(self, src_url, dst_url):
        _, dst_path = splitscheme(dst_url)
        if is_ftp(dst_url) and not self._is_history_connection(urlparse(dst_url).hostname):
            show_status_message('Destination path invalid.')
            return []
        return self._prepare_copy(src_url, dst_url)

    def _prepare_copy(self, src_url, dst_url):
        _, src_path = splitscheme(src_url)
        _, dst_path = splitscheme(dst_url)
        files_to_copy = []

        if is_ftp(src_url) and is_file(dst_url):
            if self.is_dir(src_path):
                fs.mkdir(dst_url)
                files_to_copy = self.iterdir(src_path)
        elif is_file(src_url) and is_ftp(dst_url):
            if fs.is_dir(src_url):
                self.mkdir(dst_path)
                files_to_copy = fs.iterdir(src_url)
        elif is_ftp(src_url) and is_ftp(dst_url):
            if self.is_dir(src_path):
                self.mkdir(dst_path)
                files_to_copy = self.iterdir(src_path)

        if files_to_copy:
            for fname in files_to_copy:
                yield from self._prepare_copy(url_join(src_url, fname), url_join(dst_url, fname))
        else:
            yield Task('Copying ' + url_basename(src_url), fn=self.copy, args=(src_url, dst_url))

    def copy(self, src_url, dst_url):
        _, src_path = splitscheme(src_url)
        _, dst_path = splitscheme(dst_url)
            
        if is_ftp(src_url) and is_file(dst_url):
            with FtpWrapper(src_url) as ftp, open(dst_path, 'wb') as dst_file:
                ftp.conn.retrbinary('RETR ' + ftp.home + ftp.path, dst_file.write)
        elif is_file(src_url) and is_ftp(dst_url):
            with FtpWrapper(dst_url) as ftp, open(src_path, 'rb') as src_file:
                ftp.conn.storbinary('STOR ' + ftp.home + ftp.path, src_file)
            self.cache.put(dst_path, 'is_dir', False)
            self.notify_file_added(dst_path)
        elif is_ftp(src_url) and is_ftp(dst_url):
            with FtpWrapper(src_url) as src_ftp, FtpWrapper(dst_url) as dst_ftp, NamedTemporaryFile(delete=True) as tmp_file:
                src_ftp.conn.retrbinary('RETR ' + src_ftp.home + src_ftp.path, tmp_file.write)
                dst_ftp.conn.storbinary('STOR ' + dst_ftp.home + dst_ftp.path, tmp_file)
            self.cache.put(dst_path, 'is_dir', False)
            self.notify_file_added(dst_path)  
        else:
            raise UnsupportedOperation

    def prepare_move(self, src_url, dst_url):
        _, dst_path = splitscheme(dst_url)
        if is_ftp(dst_url) and not self._is_history_connection(urlparse(dst_url).hostname):
            show_status_message('Destination path invalid.')
            return []
        return [Task('Moving ' + url_basename(src_url), fn=self.move, args=(src_url, dst_url))]

    def move(self, src_url, dst_url):
        _, src_path = splitscheme(src_url)
        _, dst_path = splitscheme(dst_url)

        # Rename on same server
        if is_ftp(src_url) and is_ftp(dst_url):
            with FtpWrapper(src_url) as src_ftp, FtpWrapper(dst_url) as dst_ftp:
                if src_ftp.host == dst_ftp.host:
                    src_ftp.conn.rename(src_ftp.home + src_ftp.path, dst_ftp.home + dst_ftp.path)
                    self.cache.put(dst_path, 'is_dir', self.is_dir(src_path))
                    self.notify_file_added(dst_path)
                    self.cache.clear(src_path)
                    self.notify_file_removed(src_path)
                    return

        self.copy(src_url, dst_url)

        if is_ftp(src_url):
            for task in self.prepare_delete(src_path):
                submit_task(task)
        elif is_file(src_url):
            fs.delete(src_url)
        else:
            raise UnsupportedOperation

    def prepare_delete(self, path):
        if self._is_history_connection(path):
            history = self._all_history_connections()
            del history[path]
            self._save_history_connections(history)
            self.cache.clear(path)
            self.notify_file_removed(path)
            show_status_message('Server deleted.')
            return []
        return self._prepare_delete(path)

    def _prepare_delete(self, path):
        if self.is_dir(path):
            for fname in self.iterdir(path):
                yield from self._prepare_delete(path_join(path, fname))
        yield Task('Deleting ' + path_basename(path), fn=self.delete, args=(path,))

    def delete(self, path):
        with FtpWrapper(self.scheme + path) as ftp:
            if self.is_dir(path):
                ftp.conn.rmd(ftp.home + ftp.path)
                show_status_message('Directory deleted.')
            else:
                ftp.conn.delete(ftp.home + ftp.path)
                show_status_message('File deleted.')
        self.cache.clear(path)
        self.notify_file_removed(path)

    def touch(self, path):
        if not self._is_history_connection(urlparse(self.scheme + path).hostname):
            raise OSError(errno.EADDRNOTAVAIL, "File path invalid")
        if self.exists(path):
            raise OSError(errno.EEXIST, "File exists")
        with FtpWrapper(self.scheme + path) as ftp, NamedTemporaryFile(delete=True) as tmp_file:
            ftp.conn.storbinary('STOR ' + ftp.home + ftp.path, tmp_file)
        self.cache.put(path, 'is_dir', False)
        self.notify_file_added(path)
        show_status_message('File added.')

    def samefile(self, path1, path2):
        return path1 == path2

    def save_stats(self, path, file_attributes):
        name, size, timestamp, isdirectory, downloadable, islink, permissions = file_attributes
        self.cache.put(path, 'is_dir', isdirectory)
