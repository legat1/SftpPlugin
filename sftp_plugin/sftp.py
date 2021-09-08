from fman import show_prompt, show_status_message
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
    _config = paramiko.config.SSHConfig.from_path(Config.sftp_file)

    @staticmethod
    def get_host(host_name):
        return SftpConfig._config.lookup(host_name)

    @staticmethod
    def get_all_hosts():
        return SftpConfig._config.get_hostnames()


class SftpWrapper():
    _connections = {}

    def __init__(self, url):
        _, path = splitscheme(url)
        self._host, self._path = SftpWrapper.parse_path(path)

    def __enter__(self):
        if not self._host or self._is_connected():
            return self
        show_status_message('Connecting to %s...' % (self._host,))
        try:
            SftpWrapper._connections[self._host] = SftpWrapper.connection(self._host)
            show_status_message('Ready.')
        except ValueError:
            show_status_message('Connection error.')
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        pass

    @property
    def conn(self):
        if not self._is_connected():
            raise ValueError('Not connected')
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
        except Exception:
            pass
        finally:
            del SftpWrapper._connections[host]

    @staticmethod
    def parse_path(path):
        server = path.split('/', 1)
        server_name = server[0]
        server_path = '/'
        if len(server) > 1:
            server_path += server[1]
        return server_name, server_path

    @staticmethod
    def connection(hostname):
        host = SftpConfig.get_host(hostname)
        client = paramiko.SSHClient()
        client.load_system_host_keys()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        
        try:
            proxy = paramiko.ProxyCommand(host['proxycommand'])
        except Exception:
            proxy = None
        if 'user' in host:
            user = host['user']
        else:
            user, ok = show_prompt('Please enter username')
            if not ok or not user:
                raise ValueError
        if 'identityfile' in host:
            key = host['identityfile']
        else:
            key = None
        password = None

        try:
            client.connect(hostname=host['hostname'], username=user, password=password, key_filename=key, sock=proxy)
        except Exception:
            password, ok = show_prompt('Please enter password')
            if not ok or not password:
                raise ValueError
            try:
                client.connect(hostname=host['hostname'], username=user, password=password, key_filename=key, sock=proxy)
            except Exception:
                raise ValueError
        
        return client.open_sftp()

    def _is_connected(self):
        return self._host in SftpWrapper._connections and SftpWrapper._connections[self._host].get_channel().get_transport().is_authenticated()


class SftpBackgroundWrapper():
    def __init__(self, url):
        _, path = splitscheme(url)
        self._host, self._path = SftpWrapper.parse_path(path)
        self._background_connection = None

    def __enter__(self):
        if not self._host or self._is_connected():
            return self
        try:
            self._background_connection = SftpWrapper.connection(self._host)
        except ValueError:
            pass
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        try:
            self._background_connection.close()
        except Exception:
            pass

    @property
    def conn(self):
        if not self._is_connected():
            raise ValueError('Not connected')
        return self._background_connection

    @property
    def host(self):
        return self._host

    @property
    def path(self):
        return self._path

    def _is_connected(self):
        return self._background_connection.get_channel().get_transport().is_authenticated() if self._background_connection else False
