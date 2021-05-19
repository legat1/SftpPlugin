from fman import DirectoryPaneCommand, DirectoryPaneListener, ApplicationCommand, QuicksearchItem, show_quicksearch, show_status_message
from fman.url import splitscheme

from .sftp import SftpWrapper
from .config import Config


class OpenSftp(DirectoryPaneCommand):
    aliases = ('Open sftp connection',)

    def __call__(self):
        self.pane.set_path(Config.scheme)


class CloseSftp(ApplicationCommand):
    aliases = ('Close sftp connection',)

    def __call__(self):
        self._active_connections = SftpWrapper.get_all_active_connections()
        if self._active_connections:
            result = show_quicksearch(self._get_items)
            if result:
                _, value = result
                if value:
                    SftpWrapper.close_connection(value)
                    show_status_message('Connection '+value+' closed.')
                    self._exit_current(value)
        else:
            show_status_message('No active connection.')

    def _get_items(self, query):
        for item in self._active_connections:
            try:
                index = item.lower().index(query)
            except ValueError:
                continue
            else:
                # The characters that should be highlighted:
                highlight = range(index, index + len(query))
                yield QuicksearchItem(item, highlight=highlight)

    def _exit_current(self, server):
        for pane in self.window.get_panes():
            scheme, path = splitscheme(pane.get_path())
            if scheme == Config.scheme and path and len(path.split('/')) > 0 and path.split('/')[0] == server:
                pane.set_path(Config.scheme)


class SftpEditListener(DirectoryPaneListener):
    def on_command(self, command_name, args):
        if command_name in ('open_with_editor', 'create_directory', 'create_and_edit_file', 'rename', 'move_to_trash'):
            url = args.get('url', self.pane.get_path())
            if url == Config.scheme:
                new_args = dict(args)
                new_args['url'] = 'file://' + Config.file_path
                return 'open_with_editor', new_args

