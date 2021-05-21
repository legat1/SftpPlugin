import re
import datetime
from .parse_time import parse_time

class FTPParser(object):

    def __init__(self):
        self._init_re()

    def parse_single(self, line, now=None):
        # Regex for standard Unix listing formats 
        match = self.unix_re.fullmatch(line)
        if match:
            typ = match[1]
            permissions = match[2]
            link_count = match[3]
            owner = match[4]
            group = match[5]
            try:
                size = int(match[6])
            except ValueError:
                size = 0
            date = match[7]
            name = match[8]
            m_name = name
            trycwd = 0
            islink = 0
            tryretr = 0
            if typ == "d":
                trycwd = 1
            elif typ == "-":
                tryretr = 1
            elif typ == "l":
                #symlink
                islink = 1
                tryretr = 1
                trycwd = 1
                try:
                    m_name, _link = m_name.split(" -> ")
                except ValueError:
                    pass
            mtime = parse_time(date, now)

            #name, size, _sizetype, mtime, _mtimetype, cwd, retr, _id, _idtype, islink
            return m_name, size, int(mtime.timestamp()), trycwd, tryretr, islink, permissions

        match = self.netware_re.fullmatch(line)
        if match:
            typ = match[1]
            permissions = match[2]
            owner = match[3]
            size = int(match[4])
            date = match[5]
            name = match[6]
        
            trycwd = 0
            tryretr = 0
            if typ[0] == "d":
                trycwd = 1
            elif typ[0] == "-":
                tryretr = 1
        
            mtime = parse_time(date, now)
            islink = 0
            return name, size, int(mtime.timestamp()), trycwd, tryretr, islink, None

        match = self.netpresenz_re.fullmatch(line)
        if match:
            typ = match[1]
            permissions = match[2]
            stuff = match[3]
            size = int(match[4])
            date = match[5]
            name = match[6]
            trycwd = 0
            tryretr = 0
            islink = 0
            if typ[0] == 'd':
                trycwd = 1
            if typ[0] == '-':
                tryretr = 1
            if typ[0] == 'l':
                trycwd = 1
                tryretr = 1
                islink = 1
                # handle symlink
                try:
                    name, _link = name.split(" -> ")
                except ValueError:
                    pass
            mtime = parse_time(date, now)
            return name, size, int(mtime.timestamp()), trycwd, tryretr, islink, permissions

        match = self.eplf_re.fullmatch(line)
        if match:
            facts = match[1]
            name = match[2]
            # Get the type, size, and date from the facts 
            
            size = 0
            mtime = 0
            typ = ' '

            typ_match = re.search("(\\+|,)(r|/),", facts)
            if typ_match:
                typ = typ_match[2]
            size_match = re.search("(\\+|,)s(\\d+),", facts)
            if size_match:
                size = int(size_match[2])
            date_match = re.search("(\\+|,)m(\\d+),", facts)
            if date_match:
                mtime = int(date_match[2])
            
            trycwd = 0
            tryretr = 0
            islink = 0

            if typ == '/':
                trycwd = 1
            if typ == 'r':
                tryretr = 1

            return name, size, mtime, trycwd, tryretr, islink, None
        
        match = self.multinet_re.fullmatch(line)
        if match:
            name = match[1]
            #version = match.groups(2)
            #file_id = match.groups(3)
            date = match[4]
            owner = match[6]
            permissions = match[7]
        
            trycwd = 0
            tryretr = 0
            islink = 0

            if name.endswith('.DIR'):
                trycwd = 1
                name = name[:-4]
            else:
                tryretr = 1

            mtime = int(parse_time(date, now).timestamp())
            
            # Multinet doesn't provide a size
            size = 0

            return name, size, mtime, trycwd, tryretr, islink, None
    
        match = self.msdos_re.fullmatch(line)
        if match:
            date = match[1]
            size = match[2]
            name = match[3]
            trycwd = 0
            tryretr = 0
            if size == '<DIR>':
                trycwd = 1
                size = 0
            else:
                tryretr = 1
                try:
                    size = int(size)
                except ValueError:
                    size = 0
        
            mtime = int(parse_time(date, now).timestamp())

            return name, size, mtime, trycwd, tryretr, 0, None
        
        return None

    def parse(self, lines, now=None):
        return [self.parse_single(line, now) for line in lines]

    def _init_re(self):
        self.unix_re = re.compile("^([-bcdlps])" # type
            "([-rwxXsStT]{1,9})" # permissions
            "\\s+(\\d+)" # hard link count
            "\\s+(\\w+)" # owner
            "\\s+(\\w+)" # group
            "\\s+(\\d+)" # size
            "\\s+([A-Za-z]{3}\\s+\\d{1,2}\\s+[:\\d]{4,5})" # modification date
            "\\s(.+)$" # name
        )

        # Regex for NetWare listing formats 
        # See http://www.novell.com/documentation/oes/ftp_enu/data/a3ep22p.html#fbhbaijf 
        self.netware_re = re.compile("^([-d])" # type
            "\\s+(\\[[-SRWCIEMFA]{8}\\])" # rights
            "\\s+(\\w+)" # owner
            "\\s+(\\d+)" # size
            "\\s+([A-Za-z]{3}\\s+\\d{1,2}\\s+[:\\d]{4,5})" # time
            "\\s+(.+)$" # name
        )

        # Regex for NetPresenz 
        # See http://files.stairways.com/other/ftp-list-specs-info.txt 
        # Here we will capture permissions and size if given 
        self.netpresenz_re = re.compile("^([-dl])" # type
            "([-rwx]{9}|)" # permissions
            "\\s+(.*)" # stuff
            "\\s+(\\d+|)" # size
            "\\s+([A-Za-z]{3}\\s+\\d{1,2}\\s+[:\\d]{4,5})" # modification date
            "\\s+(.+)$" # name
        )

        # Regex for EPLF 
        # See http://cr.yp.to/ftp/list/eplf.html 
        # SAVE: "(/,|r,|s\\d+,|m\\d+,|i[\\d!#@$%^&*()]+(\\.[\\d!#@$%^&*()]+|),)+" 
        self.eplf_re = re.compile("^\\+" # initial "plus" sign
            "([^\\s]+)" # facts
            "\\s(.+)$" # name
        )

        # Regex for MultiNet 
        # Best documentation found was
        # http://www-sld.slac.stanford.edu/SLDWWW/workbook/vms_files.html 
        self.multinet_re = re.compile("^([^;]+)" # name
            ";(\\d+)" # version
            "\\s+([\\d/]+)" # file id
            "\\s+(\\d{1,2}-[A-Za-z]{3}-\\d{4}\\s+\\d{2}:\\d{2}(:\\d{2})?)" # date
            "\\s+\\[([^\\]]+)\\]" # owner,group
            "\\s+\\(([^\\)]+)\\)$" # permissions
        )

        # Regex for MSDOS 
        self.msdos_re = re.compile("^(\\d{2}-\\d{2}-\\d{2}\\s+\\d{2}:\\d{2}[AP]M)" # date
            "\\s+(<DIR>|[\\d]+)" # dir or size
            "\\s+(.+)$" # name
        )