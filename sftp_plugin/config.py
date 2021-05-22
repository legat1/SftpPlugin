from fman.url import splitscheme

from os.path import expanduser


class Config():
    scheme = 'sftp://'
    file_path = expanduser('~/.ssh/config')
    ftp_scheme = 'ftp://'
    ftp_file = 'FTP History.json'

def is_file(url):
    try:
        scheme, _ = splitscheme(url)
        return scheme == 'file://'
    except ValueError:
        return False

def is_sftp(url):
    try:
        scheme, _ = splitscheme(url)
        return scheme == Config.scheme
    except ValueError:
        return False

def is_ftp(url):
    try:
        scheme, _ = splitscheme(url)
        return scheme == Config.ftp_scheme
    except ValueError:
        return False