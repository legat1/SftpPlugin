from urllib.parse import urlparse
from ftplib import FTP

from fman import load_json, save_json, show_status_message
from fman.url import join as url_join, normalize as url_normalize

from .config import Config

#
# In order to load the ftpparser library, we need to put the
# plugin's path into the os' sys path.
#
try:
    import ftpparser
except ImportError:
    import os
    import sys
    sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'lib'))
    import ftpparser


class FtpConfig():
    _config = {}

    @staticmethod
    def get_all_host_names():
        return FtpConfig._get_all_hosts().keys()

    @staticmethod
    def get_host(host_name):
        return FtpConfig._get_all_hosts().get(host_name, Config.ftp_scheme + host_name)

    @staticmethod
    def add_host(host_name, host):
        all_hosts = FtpConfig._get_all_hosts()
        all_hosts[host_name] = host
        FtpConfig._save_all_hosts(all_hosts)

    @staticmethod
    def remove_host(host_name):
        all_hosts = FtpConfig._get_all_hosts()
        if host_name in all_hosts:
            del all_hosts[host_name]
            FtpConfig._save_all_hosts(all_hosts)

    @staticmethod
    def get_host_url(url_or_path):
        url = urlparse(url_or_path) if url_or_path.startswith(Config.ftp_scheme) else urlparse(Config.ftp_scheme + url_or_path)
        return url_normalize(url_join(FtpConfig.get_host(url.hostname), url.path))

    @staticmethod
    def _get_all_hosts():
        if not FtpConfig._config:
            FtpConfig._config = load_json(Config.ftp_file, default=FtpConfig._config)
        return FtpConfig._config

    @staticmethod
    def _save_all_hosts(value=None):
        save_json(Config.ftp_file, value)
        FtpConfig._config = value


class FtpWrapper():
    _connections = {}

    def __init__(self, url):
        self._url = urlparse(url)

    def __enter__(self):
        if self._is_connected():
            return self
        show_status_message('Connecting to %s...' % (self.host,))
        try:
            FtpWrapper._connections[self.host] = FtpWrapper.connection(self._url)
            show_status_message('Ready.')
        except EOFError:
            show_status_message('Connection error.')
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        pass

    @property
    def conn(self):
        if not self._is_connected():
            raise Exception('Not connected')
        return FtpWrapper._connections[self.host]

    @property
    def host(self):
        return self._url.hostname

    @property
    def path(self):
        return self._url.path

    @staticmethod
    def get_all_active_connections():
        return FtpWrapper._connections.keys()

    @staticmethod
    def close_connection(host):
        if host not in FtpWrapper._connections:
            return
        try:
            FtpWrapper._connections[host].quit()
        except Exception:
            pass
        finally:
            del FtpWrapper._connections[host]

    def list_files(self):
        cmd = 'LIST'
        cmd = cmd + (' ' + self.path)
        files = []
        self.conn.retrlines(cmd, files.append)
        return ftpparser.FTPParser().parse(files)

    @staticmethod
    def connection(url):
        ftp = FTP(host=url.hostname, user=url.username, passwd=url.password)
        ftp.encoding = 'utf-8'
        return ftp

    def _is_connected(self):
        if self.host not in FtpWrapper._connections:
            return False
        try:
            FtpWrapper._connections[self.host].voidcmd('TYPE I')
            return True
        except Exception:
            return False

class FtpBackgroundWrapper():
    def __init__(self, url):
        self._url = urlparse(url)
        self._background_connection = None

    def __enter__(self):
        if self._is_connected():
            return self
        try:
            self._background_connection = FtpWrapper.connection(self._url)
        except EOFError:
            pass
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        try:
            self._background_connection.quit()
        except Exception:
            pass

    @property
    def conn(self):
        if not self._is_connected():
            raise Exception('Not connected')
        return self._background_connection

    @property
    def host(self):
        return self._url.hostname

    @property
    def path(self):
        return self._url.path

    def _is_connected(self):
        if not self._background_connection:
            return False
        try:
            self._background_connection.voidcmd('TYPE I')
            return True
        except Exception:
            return False
