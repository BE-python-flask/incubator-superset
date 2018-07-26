import os
import logging
import threading


lock = threading.Lock()


class PgtFile(object):
    """Use file to store pgt and pgtiou

    Deprecated
    """
    pgt_file = '/tmp/pilot/pilot.cas'
    strip_str = ' : '
    read_max_bytes = 4 * 1024

    @classmethod
    def get_pgt(cls, pgtiou):
        if not os.path.exists(cls.pgt_file):
            os.mknod(cls.pgt_file)
        pgt = None
        with open(cls.pgt_file, 'rb') as f:
            logging.info('[CAS] Read pgt file: [{}]'.format(cls.pgt_file))
            lines = f.readlines(cls.read_max_bytes)
        for line in lines:
            line = str(line, encoding='utf-8')
            kv = line.split(cls.strip_str)
            if pgtiou == kv[0].strip():
                pgt = kv[1].rstrip('\n').strip()
                break
        return pgt

    @classmethod
    def add_pgt_to_file(cls, pgtiou, pgt):
        if lock.acquire(1):
            if not os.path.exists(cls.pgt_file):
                os.mknod(cls.pgt_file)
            with open(cls.pgt_file, 'rb+') as f:
                logging.info('[CAS] Write pgt file: [{}]'.format(cls.pgt_file))
                lines = f.readlines(cls.read_max_bytes)
                new_line = '{}{}{}\n'.format(pgtiou, cls.strip_str, pgt)
                lines.insert(0, new_line.encode('utf-8'))
                f.seek(0)
                f.writelines(lines)
                f.truncate()
        lock.release()
