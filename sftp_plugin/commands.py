from fman import DirectoryPaneCommand, ApplicationCommand, QuicksearchItem, show_quicksearch

from .sftp import SftpWrapper


class OpenSftp(DirectoryPaneCommand):
    aliases = ('Open sftp connection',)

    def __call__(self):
        self.pane.set_path('sftp://')


class CloseSftp(ApplicationCommand):
    aliases = ('Close sftp connection',)

    def __call__(self):
        result = show_quicksearch(self._get_items)
        if result:
            _, value = result
            if value:
                SftpWrapper.close_connection(value)

    def _get_items(self, query):
        for item in SftpWrapper.get_all_active_connections():
            try:
                index = item.lower().index(query)
            except ValueError:
                continue
            else:
                # The characters that should be highlighted:
                highlight = range(index, index + len(query))
                yield QuicksearchItem(item, highlight=highlight)
