from fman import DirectoryPaneCommand, ApplicationCommand, QuicksearchItem, show_quicksearch, show_status_message

from .sftp import SftpWrapper


class OpenSftp(DirectoryPaneCommand):
    aliases = ('Open sftp connection',)

    def __call__(self):
        self.pane.set_path('sftp://')


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
