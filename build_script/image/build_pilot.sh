#!/usr/bin/env bash

function build_pilot
{
    set -e
    unset DOCKER_HOST
    if [ "${REVISION}" == "" ]; then
      cd ${COMPONENT}
      REVISION=`git rev-list HEAD |wc|awk '{print $1}'`
      cd -
    fi

    #REPO_DIR=$WORKSPACE/repo/${TRANSWARP}/$OSTYPE/$OSVERSION/$COMPONENT/$REVISION/
    REPO_DIR=/root/target/rpm/${componentBaseName}
    sudo mkdir -p $REPO_DIR
    sudo chmod 777 -R $REPO_DIR

    RPM_DIR="${WORKSPACE}/pilot/dependencies/docker/rpm"
    NODE_MODULES="/home/pilot/docker/node_modules"

    mkdir -p ${RPM_DIR}
    sudo chmod 777 -R $RPM_DIR

    cd sqlalchemy
    python3 setup.py bdist_wheel
    find . -type f -iname "sqlalchemy*whl" -exec cp {} ${RPM_DIR} \;
    cd -

    cd flask-appbuilder
    python3 setup.py bdist_wheel
    find . -type f -iname "flask_appbuilder*whl" -exec cp {} ${RPM_DIR} \;
    cd -

    cd  pilot/superset/assets/
    #wget http://172.16.1.46/pilot/node_modules.tar.gz
    curl -u 'jiajie:jiajie' -O http://172.16.1.97:8080/remote.php/webdav/pilot/node_modules-transwarp-6.0.1.tar.gz
    tar -zxvf node_modules-transwarp-6.0.1.tar.gz
    rm -f node_modules-transwarp-6.0.1.tar.gz
    npm run publish
    cd -

    cd pilot
    python3 setup.py bdist_rpm
    find . -type f -iname "superset*tar.gz" -exec cp {} $RPM_DIR \;
    cd -

    cd fileRobot/fileRobot-client
    python3.5 setup.py bdist_wheel
    find . -type f -iname "fileRobot*client*whl" -exec cp {} ${RPM_DIR} \;
    cd -

    cd pilot/dependencies/docker
    docker build -t ${DOCKER_REPO_URL}/${BUILDER}/pilot:${IMAGE_TAG} .
    docker push ${DOCKER_REPO_URL}/${BUILDER}/pilot:${IMAGE_TAG}

}
