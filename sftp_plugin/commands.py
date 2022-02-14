import json
import platform
from subprocess import call
from tempfile import NamedTemporaryFile
from urllib.parse import urlparse

from fman import (NO, YES, ApplicationCommand, DirectoryPaneCommand,
                  DirectoryPaneListener, QuicksearchItem, show_alert,
                  show_quicksearch, show_status_message, submit_task)
from fman.fs import exists, is_dir
from fman.url import join as url_join
from fman.url import splitscheme

from .config import Config, is_ftp, is_sftp
from .filesystems import FtpCopyFileTask, SftpCopyFileTask
from .ftp import FtpWrapper
from .sftp import SftpWrapper


class OpenSftp(DirectoryPaneCommand):
    aliases = ('Open sftp connection',)

    def __call__(self, url=Config.sftp_scheme):
        self.pane.set_path(url)


class OpenFtp(DirectoryPaneCommand):
    aliases = ('Open ftp connection',)

    def __call__(self, url=Config.ftp_scheme):
        self.pane.set_path(url)


class EditSftpFile(DirectoryPaneCommand):
    def is_visible(self):
        return False

    def __call__(self, url=None):
        if not url:
            url = self.pane.get_file_under_cursor()
        if url and exists(url) and not is_dir(url):
            with NamedTemporaryFile(delete=True) as tmp_file:
                local_file_url = 'file://' + tmp_file.name
                submit_task(SftpCopyFileTask(url, local_file_url))
                
                self.pane.run_command('open_with_editor', args={'url': local_file_url})
                
                choice = show_alert('Would you like to upload edited file?', buttons=YES | NO, default_button=YES)
                if choice == YES:
                    submit_task(SftpCopyFileTask(local_file_url, url))


class EditFtpFile(DirectoryPaneCommand):
    def is_visible(self):
        return False

    def __call__(self, url=None):
        if not url:
            url = self.pane.get_file_under_cursor()
        if url and exists(url) and not is_dir(url):
            with NamedTemporaryFile(delete=True) as tmp_file:
                local_file_url = 'file://' + tmp_file.name
                submit_task(FtpCopyFileTask(url, local_file_url))
                
                self.pane.run_command('open_with_editor', args={'url': local_file_url})
                
                choice = show_alert('Would you like to upload edited file?', buttons=YES | NO, default_button=YES)
                if choice == YES:
                    submit_task(FtpCopyFileTask(local_file_url, url))


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
        # show_alert('args '+ json.dumps(args))
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
        if command_name == 'open_with_editor':
            url = args.get('url', self.pane.get_path())
            if url.startswith(Config.sftp_scheme):
                new_args = dict(args)
                new_args['url'] = self.pane.get_file_under_cursor()
                return 'edit_sftp_file', new_args
            if url.startswith(Config.ftp_scheme):
                new_args = dict(args)
                new_args['url'] = self.pane.get_file_under_cursor()
                return 'edit_ftp_file', new_args
        if command_name == 'open_terminal':
            url = args.get('url', self.pane.get_path())
            if url.startswith(Config.sftp_scheme):
                new_args = dict(args)
                new_args['url'] = url
                return 'open_ssh_terminal', new_args


class OpenSshTerminal(DirectoryPaneCommand):
    def is_visible(self):
        return False

    def __call__(self, url=None):
        if not url:
            url = self.pane.get_path()
        self._open_terminal(url)

    def _open_terminal(self, url):
        scheme, path = splitscheme(url)
        if scheme != Config.sftp_scheme:
            show_alert('No such path supported.')
            return
        ssh_path = url_join('ssh://', path)
        system = platform.system()
        if system == 'Windows':
            commands = (['wt'], ['cmd', '/c start cmd'])
        elif system == 'Darwin':
            commands = (['open', '-a', 'iterm'], ['open', '-a', 'terminal'])
        elif system == 'Linux':
            commands = (['gnome-terminal'], ['xfce4-terminal'], ['konsole'], ['x-terminal-emulator'])
        else:
            show_alert('Unknown platform.')
            return
        self._start_first_existed(
            map(lambda command: command + [ssh_path], commands)
        )

    def _start_first_existed(self, commands):
        for command in commands:
            try:
                res = call(command)
                if res == 0:
                    return
            except FileNotFoundError:
                pass
        show_alert('No terminal found.')
