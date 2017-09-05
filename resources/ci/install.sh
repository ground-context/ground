#!/bin/bash

# change postgres permissions
sudo sed -i "s|peer|trust|g" /etc/postgresql/9.2/main/pg_hba.conf
sudo sed -i "s|md5|trust|g" /etc/postgresql/9.2/main/pg_hba.conf
