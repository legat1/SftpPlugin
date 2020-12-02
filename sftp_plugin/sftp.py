from fman import show_status_message
from fman.url import splitscheme

from os.path import expanduser

#
# In order to load the Paramiko library, we need to put the
# plugin's path into the os' sys path.
#
try:
    import paramiko
except ImportError:
    import os
    import sys
    sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'lib'))
    import paramiko


class SftpConfig():
    _config = paramiko.config.SSHConfig.from_path(expanduser('~/.ssh/config'))

    @staticmethod
    def get_host(host_name):
        return SftpConfig._config.lookup(host_name)

    @staticmethod
    def get_all_host():
        return SftpConfig._config.get_hostnames()


class SftpWrapper():
    _connections = {}

    def __init__(self, url):
        self._url = url
        _, path = splitscheme(url)
        self._host, self._path = self._parse_path(path)

    def __enter__(self):
        if not self._host or self._host in SftpWrapper._connections:
            return self
        show_status_message('Connecting to %s...' % (self._host,))
        SftpWrapper._connections[self._host] = self._connection()
        show_status_message('Ready.')
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        # try:
        #     SftpWrapper._connections[self._host].close()
        # except:
        #     pass
        # finally:
        #     if self._host in SftpWrapper._connections:
        #         del SftpWrapper._connections[self._host]
        return

    @property
    def conn(self):
        if self._host not in SftpWrapper._connections:
            raise Exception('Not connected')
        return SftpWrapper._connections[self._host]

    @property
    def host(self):
        return self._host

    @property
    def path(self):
        return self._path

    def _parse_path(self, path):
        server = path.split('/', 1)
        server_name = server[0]
        server_path = '/'
        if len(server) > 1:
            server_path += server[1]
        return server_name, server_path

    def _connection(self):
        host = SftpConfig.get_host(self._host)
        t = paramiko.Transport((host['hostname'], 22))
        pk = open(host['identityfile'][0])
        t.connect(
            username=host['user'],
            pkey=paramiko.RSAKey.from_private_key(pk)
        )
        return paramiko.SFTPClient.from_transport(t)
