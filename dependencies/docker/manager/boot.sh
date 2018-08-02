#!/bin/bash

[ -f /external/scripts/init.sh ] && {
  . /external/scripts/init.sh
}

set -x

echo 'umount /etc/hosts'
umount /etc/hosts
echo 'rm -rf /etc/hosts'
rm -rf /etc/hosts
echo 'ln -s /etc/transwarp/conf/hosts /etc/'
ln -s /etc/transwarp/conf/hosts /etc/

DEBUG=${DEBUG:-0}

source /etc/pilot/conf/pilot-env.sh

case $1 in
  PILOT_SERVER)
    pilot db upgrade
    pilot init
    pilot runserver -p $PILOT_EXPOSE_PORT
  ;;
esac

[ $DEBUG -eq 1 ] && {
  echo "Waiting for debug before exit"
  while true
  do
    sleep 10
  done
}

