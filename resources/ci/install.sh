#!/bin/bash

# stop running postgres and remove any existing versions
sudo service postgresql stop
sudo rm -rf /etc/postgresql/
sudo rm -rf /etc/postgresql-common/
sudo rm -rf /var/lib/postgresql/

# install postgres
sudo apt-get install -y postgresql
sudo sed -i "s|peer|trust|g" /etc/postgresql/9.6/main/pg_hba.conf
sudo sed -i "s|md5|trust|g" /etc/postgresql/9.6/main/pg_hba.conf
