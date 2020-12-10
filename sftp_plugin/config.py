from fman.url import splitscheme

from os.path import expanduser


class Config():
    scheme = 'sftp://'
    file_path = expanduser('~/.ssh/config')

def is_file(url):
    scheme, _ = splitscheme(url)
    return scheme == 'file://'

def is_sftp(url):
    scheme, _ = splitscheme(url)
    return scheme == Config.scheme
