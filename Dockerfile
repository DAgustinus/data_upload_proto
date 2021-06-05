
FROM centos:centos7
MAINTAINER The CentOS Project <cloud-ops@centos.org>

RUN yum -y update; yum clean all
RUN yum -y install sudo epel-release; yum clean all
RUN yum -y install postgresql-server postgresql postgresql-contrib supervisor pwgen; yum clean all

RUN yum -y install python
RUN yum -y install python3-pip -y

ADD ./postgresql-setup /usr/bin/postgresql-setup
ADD ./supervisord.conf /etc/supervisord.conf
ADD ./start_postgres.sh /start_postgres.sh

#Sudo requires a tty. fix that.
RUN sed -i 's/.*requiretty$/#Defaults requiretty/' /etc/sudoers
RUN chmod +x /usr/bin/postgresql-setup
RUN chmod +x /start_postgres.sh

RUN /usr/bin/postgresql-setup initdb

ADD ./postgresql.conf /var/lib/pgsql/data/postgresql.conf

RUN chown -v postgres.postgres /var/lib/pgsql/data/postgresql.conf

RUN echo "host    all             all             0.0.0.0/0               md5" >> /var/lib/pgsql/data/pg_hba.conf

VOLUME ["/var/lib/pgsql"]

EXPOSE 5432


#
# ENV POSTGRES_PASSWORD=secret
# ENV POSTGRES_USER=username
# ENV POSTGRES_DB=db_test

# RUN apt-get update || : && apt-get install python -y
# RUN apt-get install python3-pip -y

# WORKDIR /src
# VOLUME /src
copy . .
# RUN pip3 install psycopg2-binary==2.8.6
# RUN pip3 install psycopg2==2.8.6
RUN pip3 install SQLAlchemy==1.4.17
RUN pip3 install pandas==1.0.1

# RUN pip3 install -r requirements.txt

CMD ["/bin/bash", "/start_postgres.sh"]