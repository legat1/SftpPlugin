from urllib.parse import urlparse
from ftplib import FTP

from fman import show_status_message

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


class FtpWrapper():
    _connections = {}

    def __init__(self, url):
        self._url = urlparse(url)

    def __enter__(self):
        if self._is_connected():
            return self
        show_status_message('Connecting to %s...' % (self.host,))
        try:
            FtpWrapper._connections[self.host] = self._connection()
            show_status_message('Ready.')
        except EOFError as e:
            show_status_message('Connection error.')
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        return

    @property
    def conn(self):
        if not self._is_connected():
            raise Exception('Not connected')
        return FtpWrapper._connections[self.host]['conn']

    @property
    def home(self):
        if not self._is_connected():
            raise Exception('Not connected')
        return FtpWrapper._connections[self.host]['home']

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
            FtpWrapper._connections[host]['conn'].quit()
        except:
            pass
        finally:
            del FtpWrapper._connections[host]

    def list_files(self):
        path = self.path if self.path.startswith(self.home) else self.home + self.path
        cmd = 'LIST'
        cmd = cmd + (' ' + path)
        files = []
        self.conn.retrlines(cmd, files.append)
        return ftpparser.FTPParser().parse(files)

    def _connection(self):
        ftp = FTP(host=self.host, user=self._url.username, passwd=self._url.password)
        ftp.cwd(self.path)
        ftp.encoding = 'utf-8'
        return {'conn': ftp, 'home': self.path}

    def _close_connection(self):
        FtpWrapper.close_connection(self.host)

    def _is_connected(self):
        return self.host in FtpWrapper._connections
