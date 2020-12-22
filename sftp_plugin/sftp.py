from fman import show_status_message
from fman.url import splitscheme

from .config import Config

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
    _config = paramiko.config.SSHConfig.from_path(Config.file_path)

    @staticmethod
    def get_host(host_name):
        return SftpConfig._config.lookup(host_name)

    @staticmethod
    def get_all_hosts():
        return SftpConfig._config.get_hostnames()


class SftpWrapper():
    _connections = {}

    def __init__(self, url):
        self._url = url
        _, path = splitscheme(url)
        self._host, self._path = self._parse_path(path)

    def __enter__(self):
        if not self._host or self._is_connected():
            return self
        show_status_message('Connecting to %s...' % (self._host,))
        try:
            SftpWrapper._connections[self._host] = self._connection()
            show_status_message('Ready.')
        except:
            show_status_message('Connection error.')
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        # self._close_connection()
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

    @staticmethod
    def get_all_active_connections():
        return SftpWrapper._connections.keys()

    @staticmethod
    def close_connection(host):
        if host not in SftpWrapper._connections:
            return
        try:
            SftpWrapper._connections[host].close()
        except:
            pass
        finally:
            del SftpWrapper._connections[host]

    def _parse_path(self, path):
        server = path.split('/', 1)
        server_name = server[0]
        server_path = '/'
        if len(server) > 1:
            server_path += server[1]
        return server_name, server_path

    def _connection(self):
        host = SftpConfig.get_host(self._host)
        client = paramiko.SSHClient()
        client.load_system_host_keys()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
            proxy = paramiko.ProxyCommand(host['proxycommand'])
        except:
            proxy = None
        if 'identityfile' in host:
            key = host['identityfile'][0]
        else:
            key = None
        client.connect(hostname=host['hostname'], username=host['user'], key_filename=key, sock=proxy)
        return client.open_sftp()

    def _close_connection(self):
        SftpWrapper.close_connection(self._host)

    def _is_connected(self):
        return self._host in SftpWrapper._connections and SftpWrapper._connections[self._host].get_channel().get_transport().is_authenticated()
