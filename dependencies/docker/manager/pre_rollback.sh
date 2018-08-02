#!/bin/bash

set -e

PILOT_CONFIG_FILE=$CONF_DIR/pilot_config.py


if [ -f $PILOT_CONFIG_FILE ]
then
    cat $PILOT_CONFIG_FILE | while read line
    do
        # SQLALCHEMY_DATABASE_URI = 'mysql://pilot:123456@172.16.1.198:3306/pilot_dev?charset=utf8'
        if echo $line | grep -E "^SQLALCHEMY_DATABASE_URI*"
        then
            line=${line#*://}
            user=${line%%:*}
            echo "user is $user"

            line=${line#*:}
            password=${line%%@*}
            echo "password is $password"

            line=${line#*@}
            host=${line%%:*}
            echo "host is $host"

            line=${line#*:}
            port=${line%%/*}
            echo "port is $port"

            line=${line#*/}
            database=${line%%\?*}
            echo "database is $database"

            echo "start to dump data from $database to $DUMP_FILE"
            mysql --host=$host --port=$port --user=$user --password=$password $database < $DUMP_FILE
            echo "dump data from $database successfully"
        fi
    done
else
    echo "File $PILOT_CONFIG_FILE does not exist!"
fi
