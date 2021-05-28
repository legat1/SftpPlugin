from fman.url import splitscheme

from os.path import expanduser


class Config():
    sftp_scheme = 'sftp://'
    sftp_file = expanduser('~/.ssh/config')
    ftp_scheme = 'ftp://'
    ftp_file = 'FTP History.json'
    network_scheme = 'network://'

def is_file(url):
    try:
        scheme, _ = splitscheme(url)
        return scheme == 'file://'
    except ValueError:
        return False

def is_sftp(url):
    try:
        scheme, _ = splitscheme(url)
        return scheme == Config.sftp_scheme
    except ValueError:
        return False

def is_ftp(url):
    try:
        scheme, _ = splitscheme(url)
        return scheme == Config.ftp_scheme
    except ValueError:
        return False

def is_network(url):
    try:
        scheme, _ = splitscheme(url)
        return scheme == Config.network_scheme
    except ValueError:
        return False
