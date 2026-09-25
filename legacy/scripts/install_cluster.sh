#!/bin/sh

set -e

sudo apt install net-tools
sudo apt-get install selinux-tools
sudo apt-get install gnupg -y


wget https://archive.cloudera.com/cm6/6.3.1/ubuntu1804/apt/cloudera-manager.list ; sudo mv cloudera-manager.list /etc/apt/sources.list.d/

wget https://archive.cloudera.com/cm6/6.3.1/ubuntu1804/apt/archive.key ; sudo apt-key add archive.key

sudo apt-get update
sudo apt-get install openjdk-8-jdk -y

#only for the cloudera manager host
#sudo apt-get install cloudera-manager-daemons cloudera-manager-agent cloudera-manager-server

sudo apt install mysql-server libmysql-java
sudo service mysql stop
sudo update-rc.d mysql enable
#Y Y N Y Y - answers for next step
sudo /usr/bin/mysql_secure_installation
#password - gsd
#go inside mysql, create several tables and grant privileges, follow what is in the website table=user=password

#do this for all the databases
sudo /opt/cloudera/cm/schema/scm_prepare_database.sh mysql navms navms navms
sudo /opt/cloudera/cm/schema/scm_prepare_database.sh mysql scm scm scm
sudo /opt/cloudera/cm/schema/scm_prepare_database.sh mysql amon amon amon
sudo /opt/cloudera/cm/schema/scm_prepare_database.sh mysql rman rman rman
sudo /opt/cloudera/cm/schema/scm_prepare_database.sh mysql hue hue hue
sudo /opt/cloudera/cm/schema/scm_prepare_database.sh mysql metastore hive hive
sudo /opt/cloudera/cm/schema/scm_prepare_database.sh mysql sentry sentry sentry
sudo /opt/cloudera/cm/schema/scm_prepare_database.sh mysql nav nav nav
sudo /opt/cloudera/cm/schema/scm_prepare_database.sh mysql navms navms navms
sudo /opt/cloudera/cm/schema/scm_prepare_database.sh mysql oozie oozie oozie
sudo /opt/cloudera/cm/schema/scm_prepare_database.sh mysql oozie oozie oozie


