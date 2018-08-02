FROM 172.16.1.41:5000/jenkins/pilot-base:transwarp-6.0

# license jar
COPY ./jar/ /usr/local/lib/

# pilot
COPY ./rpm/ /tmp/rpm/
RUN find /tmp/rpm -maxdepth 2 -type f -iname "fileRobot*" -exec pip install {} \; && \
    find /tmp/rpm -maxdepth 2 -type f -iname "sqlalchemy*" -exec pip install {} \; && \
    find /tmp/rpm -maxdepth 2 -type f -iname "flask*appbuilder*" -exec pip install {} \; && \
    find /tmp/rpm -maxdepth 2 -type f -iname "pilot*" -exec pip install {} \; && \
    mkdir -p /etc/pilot/conf/ /var/log/pilot/ && \
    touch /var/log/pilot/pilot.log && \
    rm -rf /tmp/*


# env
ENV PYTHONPATH=/etc/pilot/conf/:$PYTHONPATH \
    LD_LIBRARY_PATH=/usr/local/lib/:$LD_LIBRARY_PATH


# for manager
COPY manager/* /usr/bin/
RUN chmod +x /usr/bin/boot.sh && \
    chmod +x /usr/bin/pre_upgrade.sh && \
    chmod +x /usr/bin/pre_rollback.sh


# for tos
ADD tos/* /tmp/
RUN mkdir -p /etc/confd/conf.d/ /etc/confd/templates/ && \
    cp /tmp/confd.toml /etc/confd/ && \
    cp /tmp/pilot.toml /etc/confd/conf.d/ && \
    cp /tmp/pilot_config.py.tmpl /etc/confd/templates/ && \
    cp /usr/lib/guardian-plugins/templates/guardian-site.xml.tmpl /etc/confd/templates/ && \
    cp /tmp/bootstrap.sh /usr/local/bin/ && \
    chmod +x /usr/local/bin/bootstrap.sh && \
    rm -rf /tmp/*


# Deploy
EXPOSE 8086
CMD ["pilot", "runserver"]
