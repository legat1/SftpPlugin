from urllib.parse import urlparse
from ftplib import FTP, CRLF

def list_files(line):
    facts_found, _, name = line.rstrip(CRLF).partition(' ')
    print(name)

url = urlparse('ftp://username:password@hostname/path')
ftp = FTP(host=url.hostname, user=url.username, passwd=url.password)
# ftp.cwd(url.path)
ftp.dir(url.path, list_files)
# for name, fact in ftp.mlsd(path=url.path, facts=['size']):
# for name in ftp.nlst(url.path):
    # print(name)