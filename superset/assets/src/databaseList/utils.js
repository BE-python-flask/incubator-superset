import { PILOT_PREFIX } from './../utils/utils';
import { connectionTypes } from './actions';

export default {
    getAbsUrl: relativePath => window.location.origin + relativePath
}

export function argsValidate(args) {
    let validate = true;
    if (!(args && args.length > 0)) {
        validate = false;
    }
    try {
        JSON.parse(args);
    } catch ( e ) {
        validate = false;
    }
    return validate;
}

export function transformObjectToArray(objectArray, attr) {
    let array = [];
    objectArray.map(obj => {
        array.push(obj[attr]);
    });
    return array;
}

export function isCorrectConnection(connectionType, connectionTypes) { /* include inceptor,mysql,oracle,mssql connection */
    if (connectionType === connectionTypes.inceptor || connectionType === connectionTypes.mysql
            || connectionType === connectionTypes.oracle || connectionType === connectionTypes.mssql) {
        return true;
    } else {
        return false;
    }
}
;

export const connectDefaultInfo = {
    'HDFS': {
        'httpfs': {
            'defaultValue': '',
            'tip': 'HDFS_HTTPFS'
        },
        'defaultIncConnect': {
            'defaultValue': '',
            'tip': 'HDFS_DEFAULTINCCONNECT'
        },
        'str': {
            'defaultValue': '',
            'tip': 'HDFS_STR'
        },
        'args': {
            'defaultValue': '',
            'tip': 'HDFS_ARGS'
        }
    },
    'INCEPTOR': {
        'str': {
            'defaultValue': 'inceptor://172.0.0.1:10000/database',
            'tip': 'INCEPTOR_STR'
        },
        'args': {
            'defaultValue': {
                'connect_args': {
                    'framed': 0,
                    'hive': 'Hive Server 2',
                    'mech': 'Kerberos',
                    'kuser': 'username@TDH',
                    'krbconf': '/etc/krb5.conf',
                    'fqdn': 'tw-node1360'
                }
            },
            'tip': 'INCEPTOR_ARGS'
        },
        'httpfs': {
            'defaultValue': '',
            'tip': 'INCEPTOR_HTTPFS'
        },
        'defaultIncConnect': {
            'defaultValue': '',
            'tip': 'INCEPTOR_DEFAULTINCCONNECT'
        }
    },
    'MYSQL': {
        'str': {
            'defaultValue': 'mysql://username:password@172.0.0.1:3306/database?charset=utf8',
            'tip': 'MYSQL_STR'
        },
        'args': {
            'defaultValue': {
                "connect_args": {

                }
            },
            'tip': 'NO_NEED_CONN_PARAMS'
        },
        'httpfs': {
            'defaultValue': '',
            'tip': 'MYSQL_HTTPFS'
        },
        'defaultIncConnect': {
            'defaultValue': '',
            'tip': 'MYSQL_DEFAULTINCCONNECT'
        }
    },
    'ORACLE': {
        'str': {
            'defaultValue': 'oracle://username:password@172.0.0.1:1521/sid',
            'tip': 'ORACLE_STR'
        },
        'args': {
            'defaultValue': {
                "connect_args": {

                }
            },
            'tip': 'NO_NEED_CONN_PARAMS'
        },
        'httpfs': {
            'defaultValue': '',
            'tip': 'ORACLE_HTTPFS'
        },
        'defaultIncConnect': {
            'defaultValue': '',
            'tip': 'ORACLE_DEFAULTINCCONNECT'
        }
    },
    'MSSQL': {
        'str': {
            'defaultValue': 'mssql+pymssql://username:password@172.0.0.1.109:1433/pilot?charset=utf8',
            'tip': 'MYSQL_STR'
        },
        'args': {
            'defaultValue': {
                "connect_args": {

                }
            },
            'tip': 'NO_NEED_CONN_PARAMS'
        },
        'httpfs': {
            'defaultValue': '',
            'tip': 'MSSQL_HTTPFS'
        },
        'defaultIncConnect': {
            'defaultValue': '',
            'tip': 'MSSQL_DEFAULTINCCONNECT'
        }
    }
};