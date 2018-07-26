import glob
import logging
import os
from jpype import *

JAR_PATH = '/usr/local/lib'
JAR_SUFFIX = 'transwarp-6.0.0.jar'
GUARDIAN_SITE_PATH = '/etc/pilot/conf/'
# LICENSE_JAR = 'pilot-license-1.0-transwarp-6.0.0.jar'
# GAURDIAN_JAR = 'guardian-client-2.0-transwarp-6.0.0.jar'


def start_jvm():
    if not isJVMStarted():
        paths = get_java_paths()
        logging.info('Java class paths: {}'.format(paths))
        startJVM(get_default_jvm_path(), '-ea',
                 '-Djava.class.path={}'.format(':'.join(paths)))
        attachThreadToJVM()


def shutdown_jvm():
    if isJVMStarted():
        shutdownJVM()


def get_java_paths():
    paths = [GUARDIAN_SITE_PATH, ]
    os.chdir(JAR_PATH)
    for file in glob.glob("*{}".format(JAR_SUFFIX)):
        paths.append('{}/{}'.format(JAR_PATH, file))
    return paths
