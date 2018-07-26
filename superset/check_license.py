from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import logging
from jpype import *
from superset.jvm import start_jvm, shutdown_jvm


def check_license():
    start_jvm()
    try:
        Check = JClass('io.transwarp.pilot.license.CheckLicense')
        check = Check()
        check.checkLicense()
        logging.info("License checking succeed")
    except JavaException as ex:
        logging.error("License check failed: " + str(ex))
        shutdown_jvm()
        raise ex
