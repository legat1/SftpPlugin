from fman.fs import FileSystem
from fman import DirectoryPaneCommand, show_alert

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
    _config = paramiko.config.SSHConfig.from_path(
        "/Users/blegat/.ssh/config")
    _connections = {}

    def iterdir(self, path):
        if not path:
            for hostname in self._config.get_hostnames():
                if hostname != '*':
                    file_path = self.scheme + hostname
                    self.cache.put(file_path, 'is_dir', True)
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
        try:
            return self.cache.get(self.scheme + path, 'is_dir')
        except KeyError:
            return True

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
        self.pane.set_path('sftp://')
