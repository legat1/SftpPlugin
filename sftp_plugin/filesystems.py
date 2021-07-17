from fman import Task, submit_task, fs, show_status_message, show_alert
from fman.fs import FileSystem, cached, notify_file_added, notify_file_changed, touch
from fman.url import basename as url_basename, join as url_join, splitscheme

from datetime import datetime
import errno
from io import UnsupportedOperation
from os.path import basename as path_basename, join as path_join, getsize
import stat
from tempfile import NamedTemporaryFile

from .sftp import SftpConfig, SftpWrapper
from .ftp import FtpConfig, FtpWrapper
from .config import Config, is_file, is_sftp, is_ftp
from .cache import SftpCache, FtpCache


class SftpFileSystem(FileSystem):
    scheme = Config.sftp_scheme

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
                    SftpCache.clear(path, 'is_dir', only_content=True)
                    for file_attributes in sftp.conn.listdir_attr(sftp.path):
                        self.save_stats(path_join(path, file_attributes.filename), file_attributes)
                        yield file_attributes.filename
        except:
            raise FileNotFoundError

    def exists(self, path):
        if not path or self._is_server_name(path):
            return True
        if SftpCache.get(path, 'is_dir') is None:
            return False
        return True

    def is_dir(self, path):
        if not path or self._is_server_name(path):
            return True
        return SftpCache.get(path, 'is_dir', False)

    def _is_server_path(self, path):
        return path and len(path.split('/')) > 1 and self._is_server_name(path.split('/')[0])

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
        SftpCache.put(path, 'is_dir', True)
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
            yield SftpCopyFileTask(src_url, dst_url)

    def prepare_move(self, src_url, dst_url):
        _, dst_path = splitscheme(dst_url)
        if is_sftp(dst_url) and not self._is_server_path(dst_path):
            show_status_message('Destination path invalid.')
            return []
        return [Task('Moving ' + url_basename(src_url), fn=self._move, args=(src_url, dst_url))]

    def _move(self, src_url, dst_url):
        _, src_path = splitscheme(src_url)
        _, dst_path = splitscheme(dst_url)

        # Rename on same server
        if is_sftp(src_url) and is_sftp(dst_url):
            with SftpWrapper(src_url) as src_sftp, SftpWrapper(dst_url) as dst_sftp:
                if src_sftp.host == dst_sftp.host:
                    src_sftp.conn.rename(src_sftp.path, dst_sftp.path)
                    SftpCache.put(dst_path, 'is_dir', SftpCache.pop(src_path, 'is_dir'))
                    self.notify_file_added(dst_path)
                    self.notify_file_removed(src_path)
                    return

        for task in self.prepare_copy(src_url, dst_url):
            submit_task(task)

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
        yield Task('Deleting ' + path_basename(path), fn=self._delete, args=(path,))

    def _delete(self, path):
        with SftpWrapper(self.scheme + path) as sftp:
            if self.is_dir(path):
                sftp.conn.rmdir(sftp.path)
                show_status_message('Directory deleted.')
            else:
                sftp.conn.remove(sftp.path)
                show_status_message('File deleted.')
        SftpCache.clear(path, 'is_dir')
        self.notify_file_removed(path)

    def touch(self, path):
        if not self._is_server_path(path):
            raise OSError(errno.EADDRNOTAVAIL, "File path invalid")
        if self.exists(path):
            raise OSError(errno.EEXIST, "File exists")
        with SftpWrapper(self.scheme + path) as sftp, NamedTemporaryFile(delete=True) as tmp_file:
            sftp.conn.put(tmp_file.name, sftp.path)
        SftpCache.put(path, 'is_dir', False)
        self.notify_file_added(path)
        show_status_message('File added.')

    def samefile(self, path1, path2):
        return path1 == path2

    def save_stats(self, path, file_attributes):
        is_dir = stat.S_ISDIR(file_attributes.st_mode)
        dt_mtime = datetime.utcfromtimestamp(file_attributes.st_mtime)
        st_mode = stat.filemode(file_attributes.st_mode)
        try:
            owner = file_attributes.longname.split()[2]
        except:
            owner = file_attributes.st_uid
        try:
            group = file_attributes.longname.split()[3]
        except:
            group = file_attributes.st_gid

        SftpCache.put(path, 'is_dir', is_dir)
        self.cache.put(path, 'size_bytes', file_attributes.st_size)
        self.cache.put(path, 'modified_datetime', dt_mtime)
        self.cache.put(path, 'get_permissions', st_mode)
        self.cache.put(path, 'get_owner', owner)
        self.cache.put(path, 'get_group', group)


class SftpCopyFileTask(Task):
    def __init__(self, src_url, dst_url):
        super().__init__('Copying ' + url_basename(src_url))
        self._src_url = src_url
        self._dst_url = dst_url
        self._set_size(src_url)

    def __call__(self):
        if self.get_size() > 0:
            self._copy(self._src_url, self._dst_url)
        else:
            touch(self._dst_url)

    def _set_size(self, src_url):
        _, src_path = splitscheme(src_url)

        if is_sftp(src_url):
            with SftpWrapper(src_url) as sftp:
                self.set_size(sftp.conn.stat(sftp.path).st_size)
        elif is_file(src_url):
            self.set_size(getsize(src_path))
        else:
            raise UnsupportedOperation

    def _copy(self, src_url, dst_url):
        _, src_path = splitscheme(src_url)
        _, dst_path = splitscheme(dst_url)
            
        if is_sftp(src_url) and is_file(dst_url):
            with SftpWrapper(src_url) as sftp:
                sftp.conn.get(sftp.path, dst_path, callback=self._callback)
        elif is_file(src_url) and is_sftp(dst_url):
            with SftpWrapper(dst_url) as sftp:
                sftp.conn.put(src_path, sftp.path, callback=self._callback)
            SftpCache.put(dst_path, 'is_dir', False)
        elif is_sftp(src_url) and is_sftp(dst_url):
            with SftpWrapper(src_url) as src_sftp, SftpWrapper(dst_url) as dst_sftp:
                with src_sftp.conn.open(src_sftp.path) as src_file:
                    dst_sftp.conn.putfo(src_file, dst_sftp.path, callback=self._callback) 
            SftpCache.put(dst_path, 'is_dir', False)
        else:
            raise UnsupportedOperation

        try:
            notify_file_added(dst_url)
        except:
            notify_file_changed(dst_url)

    def _callback(self, size, file_size):
        self.set_progress(size)


class FtpFileSystem(FileSystem):
    scheme = Config.ftp_scheme

    def get_default_columns(self, path):
        return (
            'core.Name', 'core.Size', 'core.Modified',
            'sftp_plugin.columns.Permissions')

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

    def iterdir(self, path):
        try:
            if not path:
                yield from FtpConfig.get_all_host_names()
            else:
                with FtpWrapper(FtpConfig.get_host_url(path)) as ftp:
                    FtpCache.clear(path, 'is_dir', only_content=True)
                    for file_attributes in ftp.list_files():
                        name = file_attributes[0]
                        if name == '..':
                            continue
                        self._save_stats(path_join(path, name), file_attributes)
                        yield name
        except:
            raise FileNotFoundError

    def exists(self, path):
        if not path or self._is_server_name(path):
            return True
        if FtpCache.get(path, 'is_dir') is None:
            return False
        return True
 
    def is_dir(self, path):
        if not path or self._is_server_name(path):
            return True
        return FtpCache.get(path, 'is_dir', False)

    def _is_server_path(self, path):
        return path and len(path.split('/')) > 1 and self._is_server_name(path.split('/')[0])

    def _is_server_name(self, path):
        return path in FtpConfig.get_all_host_names()

    def mkdir(self, path):
        if not path:
            show_status_message('Destination path invalid.')
            return
        if self.is_dir(path):
            show_status_message('Directory already exists.')
            return
        if self._is_server_path(path):
            with FtpWrapper(FtpConfig.get_host_url(path)) as ftp:
                ftp.conn.mkd(ftp.path)
            FtpCache.put(path, 'is_dir', True)
            self.notify_file_added(path)
            show_status_message('Directory added.')
        else:
            try:
                with FtpWrapper(FtpConfig.get_host_url(path)) as ftp:
                    FtpConfig.add_host(ftp.host, self.scheme + path)
                    self.notify_file_added(ftp.host)
                    show_status_message('Server added.')
            except:
                show_status_message('Server connection error.')
                show_alert('URL should be [user[:password]@]ftp.host[:port][/path/to/dir]')

    def prepare_copy(self, src_url, dst_url):
        _, dst_path = splitscheme(dst_url)
        if is_ftp(dst_url) and not self._is_server_path(dst_path):
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
            yield FtpCopyFileTask(src_url, dst_url)

    def prepare_move(self, src_url, dst_url):
        _, dst_path = splitscheme(dst_url)
        if is_ftp(dst_url) and not self._is_server_path(dst_path):
            show_status_message('Destination path invalid.')
            return []
        
        return [Task('Moving ' + url_basename(src_url), fn=self._move, args=(src_url, dst_url))]

    def _move(self, src_url, dst_url):
        _, src_path = splitscheme(src_url)
        _, dst_path = splitscheme(dst_url)

        # Rename on same server
        if is_ftp(src_url) and is_ftp(dst_url):
            with FtpWrapper(FtpConfig.get_host_url(src_url)) as src_ftp, FtpWrapper(FtpConfig.get_host_url(dst_url)) as dst_ftp:
                if src_ftp.host == dst_ftp.host:
                    src_ftp.conn.rename(src_ftp.path, dst_ftp.path)
                    FtpCache.put(dst_path, 'is_dir', FtpCache.pop(src_path, 'is_dir'))
                    self.notify_file_added(dst_path)
                    self.notify_file_removed(src_path)
                    return

        for task in self.prepare_copy(src_url, dst_url):
            submit_task(task)

        if is_ftp(src_url):
            for task in self.prepare_delete(src_path):
                submit_task(task)
        elif is_file(src_url):
            fs.delete(src_url)
        else:
            raise UnsupportedOperation

    def prepare_delete(self, path):
        if self._is_server_name(path):
            FtpConfig.remove_host(path)
            self.notify_file_removed(path)
            show_status_message('Server deleted.')
            return []
        return self._prepare_delete(path)

    def _prepare_delete(self, path):
        if self.is_dir(path):
            for fname in self.iterdir(path):
                yield from self._prepare_delete(path_join(path, fname))
        yield Task('Deleting ' + path_basename(path), fn=self._delete, args=(path,))

    def _delete(self, path):
        with FtpWrapper(FtpConfig.get_host_url(path)) as ftp:
            if self.is_dir(path):
                ftp.conn.rmd(ftp.path)
                show_status_message('Directory deleted.')
            else:
                ftp.conn.delete(ftp.path)
                show_status_message('File deleted.')
        FtpCache.clear(path, 'is_dir')
        self.notify_file_removed(path)

    def touch(self, path):
        if not self._is_server_path(path):
            raise OSError(errno.EADDRNOTAVAIL, "File path invalid")
        if self.exists(path):
            raise OSError(errno.EEXIST, "File exists")
        with FtpWrapper(FtpConfig.get_host_url(path)) as ftp, NamedTemporaryFile(delete=True) as tmp_file:
            ftp.conn.storbinary('STOR ' + ftp.path, tmp_file)
        FtpCache.put(path, 'is_dir', False)
        self.notify_file_added(path)
        show_status_message('File added.')

    def samefile(self, path1, path2):
        return path1 == path2

    def _save_stats(self, path, file_attributes):
        name, size, timestamp, isdirectory, downloadable, islink, permissions = file_attributes
        dt_mtime = datetime.utcfromtimestamp(timestamp)

        FtpCache.put(path, 'is_dir', bool(isdirectory))
        self.cache.put(path, 'size_bytes', size)
        self.cache.put(path, 'modified_datetime', dt_mtime)
        self.cache.put(path, 'get_permissions', permissions)


class FtpCopyFileTask(Task):
    def __init__(self, src_url, dst_url):
        super().__init__('Copying ' + url_basename(src_url))
        self._src_url = src_url
        self._dst_url = dst_url
        self._size_written = 0
        self._set_size(src_url)

    def __call__(self):
        if self.get_size() > 0:
            self._copy(self._src_url, self._dst_url)
        else:
            touch(self._dst_url)

    def _set_size(self, src_url):
        _, src_path = splitscheme(src_url)

        if is_ftp(src_url):
            with FtpWrapper(FtpConfig.get_host_url(src_url)) as ftp:
                self.set_size(ftp.conn.size(ftp.path))
        elif is_file(src_url):
            self.set_size(getsize(src_path))
        else:
            raise UnsupportedOperation

    def _copy(self, src_url, dst_url):
        _, src_path = splitscheme(src_url)
        _, dst_path = splitscheme(dst_url)
            
        if is_ftp(src_url) and is_file(dst_url):
            with FtpWrapper(FtpConfig.get_host_url(src_url)) as ftp, open(dst_path, 'wb') as dst_file:
                def callback(data):
                    dst_file.write(data)
                    self._callback(data)
                ftp.conn.retrbinary('RETR ' + ftp.path, callback)
        elif is_file(src_url) and is_ftp(dst_url):
            with FtpWrapper(FtpConfig.get_host_url(dst_url)) as ftp, open(src_path, 'rb') as src_file:
                ftp.conn.storbinary('STOR ' + ftp.path, src_file, callback=self._callback)
            FtpCache.put(dst_path, 'is_dir', False)
        elif is_ftp(src_url) and is_ftp(dst_url):
            with FtpWrapper(FtpConfig.get_host_url(src_url)) as src_ftp, FtpWrapper(FtpConfig.get_host_url(dst_url)) as dst_ftp, NamedTemporaryFile(delete=True) as tmp_file:
                src_ftp.conn.retrbinary('RETR ' + src_ftp.path, tmp_file.write)
                dst_ftp.conn.storbinary('STOR ' + dst_ftp.path, tmp_file, callback=self._callback)
            FtpCache.put(dst_path, 'is_dir', False)
        else:
            raise UnsupportedOperation

        try:
            notify_file_added(dst_url)
        except:
            notify_file_changed(dst_url)

    def _callback(self, data):
        self._size_written += len(data)
        self.set_progress(self._size_written)


class NetworkFileSystem(FileSystem):
    scheme = Config.network_scheme

    def iterdir(self, path):
        yield 'ftp'
        yield 'sftp'
  
    def exists(self, path):
        return True
 
    def is_dir(self, path):
        return True
