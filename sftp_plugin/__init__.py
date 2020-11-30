from fman.fs import FileSystem, notify_file_added
from fman.url import basename, splitscheme
from fman import DirectoryPaneCommand, Task, show_alert

import io
import stat
import sys

#
# In order to load the Paramiko library, we need to put the
# plugin's path into the os' sys path.
#
sys.path.append('/Users/blegat/Documents/projects/fman-sftp/lib')

import paramiko

class SftpFileSystem(FileSystem):
    scheme = 'sftp://'
    _config = paramiko.config.SSHConfig.from_path('/Users/blegat/.ssh/config')
    _connections = {}

    def iterdir(self, path):
        if not path:
            for hostname in self._config.get_hostnames():
                if hostname != '*':
                    self.cache.put(self.scheme + hostname, 'is_dir', True)
                    yield hostname
        else:
            server_name, server_path = self._parse_path(path)
            if server_name not in self._connections:
                self._connect(server_name)

            for file_attributes in self._connections[server_name].listdir_iter(server_path):
                file_path = self.scheme + path + '/' + file_attributes.filename
                self.cache.put(file_path, 'is_dir',
                               stat.S_ISDIR(file_attributes.st_mode))
                yield file_attributes.filename

    def is_dir(self, path):
        if not path:
            return True
        try:
            return self.cache.get(self.scheme + path, 'is_dir')
        except KeyError:
            raise FileNotFoundError()

    def mkdir(self, path):
        try:
            self.is_dir(path)
            raise FileExistsError()
        except FileNotFoundError:
            server_name, server_path = self._parse_path(path)
            self._connections[server_name].mkdir(server_path)
            self.notify_file_added(path)
            self.cache.put(self.scheme + path, 'is_dir', True)


    def prepare_copy(self, src_url, dst_url):
        return [Task('Copying ' + basename(src_url), fn=self.copy, args=(src_url, dst_url))]

    def copy(self, src_url, dst_url):
        src_scheme, src_path = splitscheme(src_url)
        dst_scheme, dst_path = splitscheme(dst_url)
        if src_scheme == self.scheme and dst_scheme == 'file://':
            server_name, server_path = self._parse_path(src_path)
            self._connections[server_name].get(server_path, dst_path)
            notify_file_added(dst_url)
        elif src_scheme == 'file://' and dst_scheme == self.scheme:
            server_name, server_path = self._parse_path(dst_path)
            self._connections[server_name].put(src_path, server_path)
            self.notify_file_added(dst_path)
            self.cache.put(self.scheme + dst_path, 'is_dir', False)
        elif src_scheme == dst_scheme:
            src_server_name, src_server_path = self._parse_path(src_path)
            dst_server_name, dst_server_path = self._parse_path(dst_path)
            with self._connections[src_server_name].open(src_server_path) as src_file:
                self._connections[dst_server_name].putfo(src_file, dst_server_path)
            self.notify_file_added(dst_path)
            self.cache.put(self.scheme + dst_path, 'is_dir', False)
        else:
            raise io.UnsupportedOperation()

    def prepare_delete(self, path):
        server_name, server_path = self._parse_path(path)
        return [Task('Deleting ' + server_path, fn=self.delete, args=(path,))]

    def delete(self, path):
        server_name, server_path = self._parse_path(path)
        if self.is_dir(path):
            self._connections[server_name].rmdir(server_path)
        else:
            self._connections[server_name].remove(server_path)
        self.notify_file_removed(path)
        self.cache.clear(self.scheme + path)

    def _parse_path(self, path):
        server = path.split('/', 1)
        server_name = server[0]
        server_path = '/'
        if len(server) > 1:
            server_path += server[1]
        return server_name, server_path

    def _connect(self, server_name):
        host = self._config.lookup(server_name)
        t = paramiko.Transport((host['hostname'], 22))
        pk = open(host['identityfile'][0])
        t.connect(
            username=host['user'],
            pkey=paramiko.RSAKey.from_private_key(pk)
        )
        self._connections[server_name] = paramiko.SFTPClient.from_transport(t)


class OpenSftp(DirectoryPaneCommand):
    def __call__(self):
        self.pane.set_path(SftpFileSystem.scheme)
