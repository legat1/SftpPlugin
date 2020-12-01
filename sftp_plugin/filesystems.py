from fman import Task, fs, show_status_message, show_alert
from fman.fs import FileSystem, cached
from fman.url import basename as url_basename, join as url_join, splitscheme

from datetime import datetime
import errno
from io import UnsupportedOperation
from os.path import basename as path_basename, commonprefix, dirname as path_dirname, join as path_join
import re
import stat
from tempfile import NamedTemporaryFile

from .sftp import SftpConfig, SftpWrapper

is_file = re.compile('^file://').match
is_sftp = re.compile('^sftp://').match


class SftpFileSystem(FileSystem):
    scheme = 'sftp://'

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
        if not path:
            for hostname in SftpConfig.get_all_host():
                if hostname != '*':
                    yield hostname
        else:
            with SftpWrapper(self.scheme + path) as sftp:
                for file_attributes in sftp.conn.listdir_iter(sftp.path):
                    self.save_stats(path_join(path, file_attributes.filename), file_attributes)
                    yield file_attributes.filename

    @cached
    def exists(self, path):
        if not path or len(path.split('/')) < 2:
            return True
        try:
            self.cache.get(path, 'is_dir')
            return True
        except KeyError:
            return False

    @cached
    def is_dir(self, path):
        if not path or len(path.split('/')) < 2:
            return True
        try:
            return self.cache.get(path, 'is_dir')
        except KeyError:
            return False
        
    def mkdir(self, path):
        if self.is_dir(path):
            return
        with SftpWrapper(self.scheme + path) as sftp:
            sftp.conn.mkdir(sftp.path)
        self.cache.put(path, 'is_dir', True)
        self.notify_file_added(path)

    def prepare_copy(self, src_url, dst_url):
        return self._prepare_copy(src_url, dst_url)

    def _prepare_copy(self, src_url, dst_url):
        _, src_path = splitscheme(src_url)
        _, dst_path = splitscheme(dst_url)
        files_to_copy = []

        if is_sftp(src_url) and is_file(dst_url):
            if self.is_dir(src_path):
                yield Task('Creating ' + url_basename(dst_url), fn=fs.mkdir, args=(dst_url,))
                files_to_copy = self.iterdir(src_path)
        elif is_file(src_url) and is_sftp(dst_url):
            if fs.is_dir(src_url):
                yield Task('Creating ' + url_basename(dst_url), fn=self.mkdir, args=(dst_path,))
                files_to_copy = fs.iterdir(src_url)
        elif is_sftp(src_url) and is_sftp(dst_url):
            if self.is_dir(src_path):
                yield Task('Creating ' + url_basename(dst_url), fn=self.mkdir, args=(dst_path,))
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
        return [Task('Moving ' + url_basename(src_url), fn=self.move, args=(src_url, dst_url))]

    def move(self, src_url, dst_url):
        # Rename on same server
        src_scheme, src_path = splitscheme(src_url)
        dst_scheme, dst_path = splitscheme(dst_url)
        if src_scheme == dst_scheme and commonprefix([src_path, dst_path]):
            with SftpWrapper(src_url) as src_sftp, SftpWrapper(dst_url) as dst_sftp:
                src_sftp.conn.rename(src_sftp.path, dst_sftp.path)
            self.cache.put(dst_path, 'is_dir', self.is_dir(src_path))
            self.notify_file_added(dst_path)
            self.cache.clear(src_path)
            self.notify_file_removed(src_path)
            return

        fs.copy(src_url, dst_url)
        if fs.exists(src_url):
            fs.delete(src_url)

    def prepare_delete(self, path):
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
        if self.exists(path):
            raise OSError(errno.EEXIST, "File exists")
        with SftpWrapper(self.scheme + path) as sftp:
            with NamedTemporaryFile(delete=True) as tmp:
                sftp.conn.put(tmp.name, sftp.path)
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
