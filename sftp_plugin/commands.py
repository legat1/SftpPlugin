from urllib.parse import urlparse

from fman import DirectoryPaneCommand, DirectoryPaneListener, ApplicationCommand, QuicksearchItem, show_quicksearch, show_status_message, show_alert
from fman.url import join as url_join, splitscheme

from .sftp import SftpWrapper
from .ftp import FtpWrapper
from .config import Config, is_ftp, is_sftp


class OpenSftp(DirectoryPaneCommand):
    aliases = ('Open sftp connection',)

    def __call__(self, url=Config.sftp_scheme):
        self.pane.set_path(url)


class OpenFtp(DirectoryPaneCommand):
    aliases = ('Open ftp connection',)

    def __call__(self, url=Config.ftp_scheme):
        self.pane.set_path(url)


class OpenNetwork(DirectoryPaneCommand):
    aliases = ('Open network connection',)

    def __call__(self, url=Config.network_scheme):
        self.pane.set_path(url)


class CloseNetwork(ApplicationCommand):
    aliases = ('Close network connection',)

    def __call__(self):
        self._sftp_active_connections = SftpWrapper.get_all_active_connections()
        self._ftp_active_connections = FtpWrapper.get_all_active_connections()
        if self._sftp_active_connections or self._ftp_active_connections:
            result = show_quicksearch(self._get_items)
            if result:
                _, value = result
                _, path = splitscheme(value)
                if is_sftp(value):
                    SftpWrapper.close_connection(path)    
                elif is_ftp(value):
                    FtpWrapper.close_connection(path)
                show_status_message('Connection '+path+' closed.')
                self._exit_current(value)
        else:
            show_status_message('No active connection.')

    def _get_items(self, query):
        for item in self._sftp_active_connections:
            try:
                index = item.lower().index(query)
            except ValueError:
                continue
            else:
                # The characters that should be highlighted:
                highlight = range(index, index + len(query))
                yield QuicksearchItem(url_join(Config.sftp_scheme, item), title=item, highlight=highlight, hint='sftp')
        for item in self._ftp_active_connections:
            try:
                index = item.lower().index(query)
            except ValueError:
                continue
            else:
                # The characters that should be highlighted:
                highlight = range(index, index + len(query))
                yield QuicksearchItem(url_join(Config.ftp_scheme, item), title=item, highlight=highlight, hint='ftp')

    def _exit_current(self, url):
        closed_scheme, closed_path = splitscheme(url)
        for pane in self.window.get_panes():
            current_scheme, current_path = splitscheme(pane.get_path())   
            if current_scheme == closed_scheme and \
                ((current_scheme == Config.sftp_scheme and current_path and len(current_path.split('/')) > 0 and current_path.split('/')[0] == closed_path) or \
                (current_scheme == Config.ftp_scheme and urlparse(pane.get_path()).hostname == closed_path)):
                pane.set_path(current_scheme)


class NetworkListener(DirectoryPaneListener):
    def on_command(self, command_name, args):
        # show_alert('command '+ command_name)
        if command_name == 'open_directory':
            url = args.get('url', self.pane.get_path())
            if url == url_join(Config.network_scheme, 'ftp') :
                new_args = dict(args)
                new_args['url'] = Config.ftp_scheme
                return 'open_ftp', new_args
            if url == url_join(Config.network_scheme, 'sftp') :
                new_args = dict(args)
                new_args['url'] = Config.sftp_scheme
                return 'open_sftp', new_args
        if command_name == 'go_up':
            url = args.get('url', self.pane.get_path())
            if url == Config.sftp_scheme or url == Config.ftp_scheme:
                new_args = dict(args)
                new_args['url'] = Config.network_scheme
                return 'open_network', new_args
        if command_name in ('open_with_editor', 'create_directory', 'create_and_edit_file', 'rename', 'move_to_trash'):
            url = args.get('url', self.pane.get_path())
            if url == Config.sftp_scheme:
                new_args = dict(args)
                new_args['url'] = 'file://' + Config.sftp_file
                return 'open_with_editor', new_args
            if url == Config.ftp_scheme:
                new_args = dict(args)
                new_args['url'] = 'file://' + Config.ftp_file
                return 'open_with_editor', new_args
