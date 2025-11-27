#!/bin/bash

bin_path=/usr/local/bin/mlhttp_tunnel
config_path=/usr/local/etc/dictionaries/ml_http_tunnel_config
alert_path=/usr/local/bin/mlhttp_tunnel/alerts
model_path=/usr/local/bin/mlhttp_tunnel/model
service_path=/etc/systemd/system
temp_preprocessing_dir="/usr/local/bin/mlhttp_tunnel/preproc_http_tunnel_input"
log_dir=/var/log/mlanalysis


mkdir -p $bin_path
mkdir -p $config_path
mkdir -p $alert_path
mkdir -p $model_path
mkdir -p $temp_preprocessing_dir
mkdir -p $log_dir

log_file=$log_dir/mlhttp_tunnel.log
touch $log_file

#mkdir -p /usr/local/bin/mlhttps_c2/alerts
#mkdir -p /usr/local/bin/mlhttps_c2/model
#mkdir -p /var/log/vehere/mlhttps_c2-alerts

cd /home/sudipta/ML_HTTP_TUNNEL
cp src/*.py $bin_path
cp model/*.pkl $model_path
cp ml_http_tunnel_config/http_tunnel_config.json $config_path
cp src/mlhttp_tunnel.sh $bin_path
cp mlhttp_tunnel.service $service_path


chmod 755 $bin_path/*
chmod +x $bin_path/*.sh
chmod 755 $config_path/http_tunnel_config.json
chmod 777 $service_path/mlhttp_tunnel.service


systemctl daemon-reload
systemctl enable mlhttp_tunnel.service
systemctl start mlhttp_tunnel.service


