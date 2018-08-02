#!/bin/bash

[ -f /external/scripts/init.sh ] && {
  . /external/scripts/init.sh
}

set -ex

source /usr/lib/guardian-plugins/scripts/tdh-env.sh

PRINCIPAL_SUFFIX=${PRINCIPAL_SUFFIX:-"tos"}

export MASTER_PRINCIPAL=${KRB_USER:-pilot}/${PRINCIPAL_SUFFIX}@${KRB_REALM:-"TDH"}

setup_keytab root $MASTER_PRINCIPAL


if [ x"$KRB_PLUGIN_ENABLE" = x"true" ]; then
  CLASSPATH=$CLASSPATH:/usr/lib/guardian-plugins/lib/*
fi

confd -onetime

pilot db upgrade
pilot init
pilot runserver
