from fman import DirectoryPaneCommand


class OpenSftp(DirectoryPaneCommand):
    def __call__(self):
        self.pane.set_path('sftp://')
