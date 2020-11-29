import paramiko
from paramiko.py3compat import input

# setup logging
paramiko.util.log_to_file("demo_sftp.log")

# get hostname
config = paramiko.config.SSHConfig.from_path("/Users/blegat/.ssh/config")
print(config.get_hostnames())
hostname = input("Hostname: ")
host = config.lookup(hostname)

# now, connect and use paramiko Transport to negotiate SSH2 across the connection
with paramiko.Transport((host['hostname'], 22)) as t, open(host['identityfile'][0]) as pk:
    t.connect(
        username=host['user'],
        pkey=paramiko.RSAKey.from_private_key(pk)
    )
    with paramiko.SFTPClient.from_transport(t) as sftp:
        # dirlist on remote host
        dirlist = sftp.listdir(".")
        print("Dirlist: %s" % dirlist)
