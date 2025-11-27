#!/bin/bash
CONDA_SCRIPT_FILE="/root/miniconda3/etc/profile.d/conda.sh"
SPARK_SUBMIT="/usr/local/src/ml-analyzer-v3.2/bin/spark-submit"
HTTP_TUNNEL_SCRIPT="/usr/local/bin/mlhttp_tunnel/http_tunnel.py"
HTTP_TUNNEL_PREPROCESSOR_INPUT="/usr/local/bin/mlhttp_tunnel/preproc_http_tunnel_input" #temporary pre-processing directory
AGGREGATION_TIME_INTERVAL=5m
alert_input_dir="/usr/local/bin/mlhttp_tunnel/alerts"
alert_logstash_path="/var/log/vehere/mlhttp_tunnel-alerts"



#remove all files in temporary pre-processing directory
rm -f $HTTP_TUNNEL_PREPROCESSOR_INPUT/*

mkdir -p $alert_logstash_path

if ls $alert_logstash_path/*.json 1> /dev/null 2>&1; then
    chown logstash:logstash $alert_logstash_path/*.json
fi

chmod 755 $alert_logstash_path
alerts_window_mins=1440
alerts_marker_file="/var/log/vehere/mlhttp_tunnel-alerts/marker.txt"
source $CONDA_SCRIPT_FILE
conda activate dnn
touch $alerts_marker_file





while true; do

    "$SPARK_SUBMIT" \
        --master local[4] \
        --conf spark.driver.cores=4 \
        --conf spark.executor.cores=4 \
        "$HTTP_TUNNEL_SCRIPT"

    #"$SPARK_SUBMIT" "$HTTPS_C2_SCRIPT"
    #python3 $HTTPS_C2_SCRIPT
    sleep $AGGREGATION_TIME_INTERVAL
    find "$alert_input_dir" -type f -newer "$alerts_marker_file" -exec cp {} "$alert_logstash_path" \;
    find "$alert_input_dir" -name "*.json" -mmin +"$alerts_window_mins" -exec rm -f {} \;
    chown logstash:logstash $alert_logstash_path/*.json
    touch $alerts_marker_file
done




