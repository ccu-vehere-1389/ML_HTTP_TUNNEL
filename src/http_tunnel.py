import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, dayofweek, count, lit, avg, expr, date_format, to_timestamp, lower, udf
from datetime import datetime
import os
import logging
from https_c2_config_loader import Http_tunnelConfigLoader
from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType
import json
import ipaddress
from pyspark.sql import functions as F
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType
from pyspark.sql.types import ArrayType, DoubleType, FloatType
from collections import Counter
import numpy as np
import math
import pickle
import joblib
import shutil
from collections import defaultdict
import time






class Http_tunnelParser:
    def __init__(self):

        #initialize configloader object
        config_loader_object = Http_tunnelConfigLoader()
        self.log_file_path, self.http_tunnel_preprocessor_input_dir, self.get_model_path, self.threshold, self.alerts_dir = config_loader_object.main()
        #Setup logging
        self.logger = config_loader_object.logger
        self.schema = StructType([
            StructField("network", StructType([
                StructField("src_ip", StringType(), True),
                StructField("dst_ip", StringType(), True)
            ]), True),
            StructField("transport", StructType([
                StructField("src_port", LongType(), True),
                StructField("dst_port", LongType(), True),
                StructField("protocol", StringType(), True),
                StructField("src_flags", StringType(), True),
                StructField("dst_flags", StringType(), True)
            ]), True),
            StructField("session", StructType([
                StructField("duration", LongType(), True), #in milisec
                StructField("total_bytes", LongType(), True),
                StructField("transmitted_bytes", LongType(), True),
                StructField("received_bytes", LongType(), True),
                StructField("total_packets", LongType(), True),
                StructField("transmitted_packets", LongType(), True),
                StructField("received_packets", LongType(), True),

                StructField("client_max_pkt_size", LongType(), True),
                StructField("server_max_pkt_size", LongType(), True),

                StructField("client_average_interarrival_time", LongType(), True),
                StructField("server_average_interarrival_time", LongType(), True),
                StructField("client_std_dev_interarrival_time", LongType(), True),
                StructField("server_std_dev_interarrival_time", LongType(), True),

                StructField("client_total_payload_size", LongType(), True),
                StructField("server_total_payload_size", LongType(), True),
                StructField("client_nonempty_packet_count", LongType(), True),
                StructField("server_nonempty_packet_count", LongType(), True),
                
                StructField("protocol", StringType(), True),
                StructField("start_time", LongType(), True),
                StructField("client_entropy", LongType(), True),
                StructField("server_entropy", LongType(), True)
            ]), True),
        
        ])


        self._spark_session = None

    # Create a spark Session 
    def create_spark_session(self):
        if not self._spark_session:
            self._spark_session = SparkSession.builder.appName("HTTP_TUNNEL Parser").getOrCreate()
        return self._spark_session

    
    # Preprocessing
    def extract_features(self):
        spark = self.create_spark_session()
        filenames = os.listdir(self.http_tunnel_preprocessor_input_dir)
        
        if len(filenames) == 0:
            self.logger.info("No Files in input Directory. Skipping!")
            return None, None
        '''
        # Move files to temp preprocessing dir
        os.makedirs(self.http_tunnel_preprocessor_input_dir, exist_ok=True)

        print(f" Moveing parquet files to : {self.http_tunnel_preprocessor_input_dir} ...")
        self.logger.info(f"Moving parquet files to: {self.http_tunnel_preprocessor_input_dir} ...")
        moved_files = []
        for filename in filenames:
            if not filename.endswith(".parquet"):
                continue

            full_file_path = os.path.join(self.http_tunnel_raw_input_dir, filename)
            
            src = os.path.join(self.http_tunnel_raw_input_dir, filename)
            dst = os.path.join(self.http_tunnel_preprocessor_input_dir, filename)
            
            try:
                shutil.move(src, dst)  
                moved_files.append(dst)

            except Exception as e:
                self.logger.error(f"Failed to move {src} -> {dst}: {e}")

        if not moved_files:
            self.logger.info("No files moved to temp preprocessing dir. Skipping!")
            return None, None    
        '''
        self.logger.info("Processing perquet files....")
        df_list = []
        for filename in filenames:
            if not filename.endswith('.parquet'):
                continue

            full_file_path = os.path.join(self.http_tunnel_preprocessor_input_dir, filename)
            print(f"Processing file: {full_file_path}")
            self.logger.info(f"Processing file: {full_file_path}")
            try:
                df = spark.read.schema(self.schema).parquet(full_file_path)
                #df = spark.read.parquet(full_file_path)
                print(f"Read {df.count()} rows from {full_file_path}")
                df_list.append(df)

            except Exception as e:
                self.logger.error(f"Error processing file {full_file_path}: {e}")

        if not df_list:
            self.logger.info("No valid DataFrames created. Skipping!")
            return None, None

        merged_df = df_list[0]
        for df in df_list[1:]:
            merged_df = merged_df.unionAll(df)

        print(f"{merged_df.count()} rows extracted from perquet...")
        self.logger.info(f"{merged_df.count()} rows extracted from perquet...")

        # Filter HTTP sessions before feature extraction
        df_filtered = merged_df.filter(
                 F.lower(F.col("session.protocol")).isin("http", "http-alt") 
            )
        
        print(f"{df_filtered.count()} rows after http filtering...")
        self.logger.info(f"{df_filtered.count()} rows after http filtering...")


       
        df_intermediate = df_filtered.select(
            # Basic network information
            F.col("network.src_ip").alias("src_ip"),
            F.col("network.dst_ip").alias("dst_ip"),
            F.col("transport.src_port").cast("int").alias("src_port"),
            F.col("transport.dst_port").cast("int").alias("dst_port"),
            F.col("transport.protocol").alias("protocol"),
            
            # Session duration (milliseconds)
            F.col("session.duration").alias("Flow_Duration"),
            F.col("session.transmitted_packets").alias("Total_Fwd_Packets"),
            F.col("session.received_packets").alias("Total_Backward_Packets"),

            F.when(F.col("session.transmitted_packets") > 0,
                F.col("session.received_packets").cast("double") / F.col("session.transmitted_packets").cast("double")
            ).otherwise(0.0).alias("backward_vs_forward"),

        
            F.col("session.received_bytes").alias("Bwd_Packets_Length_Total"),
            F.col("session.server_max_pkt_size").alias("Bwd_Packet_Length_Max"),

            # --- Mean calculations ---


            F.when(F.col("session.received_packets") > 0,
                F.col("session.received_bytes").cast("double") / F.col("session.received_packets").cast("double")
            ).otherwise(0.0).alias("Bwd_Packet_Length_Mean"),

            # --- Flow statistics ---
            F.when((F.col("session.duration") / 1000.0) > 0,
                F.col("session.total_bytes").cast("double") / (F.col("session.duration") / 1000.0)
            ).otherwise(0.0).alias("Flow_Bytes_per_s"),



            # --- IAT (Inter-Arrival Time) metrics ---
            (
                (
                    (F.col("session.transmitted_packets").cast("double") * F.col("session.client_std_dev_interarrival_time").cast("double")) +
                    (F.col("session.received_packets").cast("double") * F.col("session.server_std_dev_interarrival_time").cast("double"))
                ) / 2.0
            ).alias("Flow_IAT_Mean"),

            

            F.col("session.client_average_interarrival_time").alias("Fwd_IAT_Mean"),
            
        
            
            # --- Directional packet rates ---


            F.when((F.col("session.duration") / 1000.0) > 0,
                F.col("session.received_packets").cast("double") / (F.col("session.duration") / 1000.0)
            ).otherwise(0.0).alias("Bwd_Packets_per_s"),

            # --- Combined packet stats ---


            F.when(
                F.col("session.total_packets") > 0,
                F.col("session.total_bytes").cast("double") / F.col("session.total_packets").cast("double")
            ).otherwise(0.0).alias("Packet_Length_Mean"),

            # client mean and server mean
            F.when(
                F.col("session.client_nonempty_packet_count") > 0,
                F.col("session.client_total_payload_size").cast("double") / F.col("session.client_nonempty_packet_count").cast("double")
            ).otherwise(0.0).alias("client_mean"),

            F.when(
                F.col("session.server_nonempty_packet_count") > 0,
                F.col("session.server_total_payload_size").cast("double") / F.col("session.server_nonempty_packet_count").cast("double")
            ).otherwise(0.0).alias("server_mean"),

            # --- Down/Up ratio ---
            F.when(F.col("session.transmitted_packets") > 0,
                F.col("session.received_packets").cast("double") / F.col("session.transmitted_packets")
            ).otherwise(0.0).alias("Down_Up_Ratio"),

            # --- Average segment sizes ---

            F.when(F.col("session.server_nonempty_packet_count") > 0,
                F.col("session.server_total_payload_size").cast("double") / F.col("session.server_nonempty_packet_count")
            ).otherwise(0.0).alias("Avg_Bwd_Segment_Size"),

            # --- HTTP entropy ---
            #F.col("session.client_entropy").alias("request_body_entropy"),
            #F.col("session.server_entropy").alias("response_body_entropy"),

            F.col("session.start_time").cast("int").alias("start_time")
        )

        
        df_flat = df_intermediate.select(
            "*",  
            
            # --- Packet variance & std ( reference  from client_mean and server_mean) ---
            (
                (
                    ((F.col("client_mean") - F.col("Packet_Length_Mean")) ** 2) +
                    ((F.col("server_mean") - F.col("Packet_Length_Mean")) ** 2)
                ) / 2.0
            ).alias("Packet_Length_Variance"),


        )

        print(f"{df_flat.count()} rows after feature engineering....")
        self.logger.info("Feature engineering completed")

        return df_flat, filenames

    

    def derive_required_feature(self,df_engineered):
        features = [
             "Flow_Duration", "Total_Fwd_Packets", "Total_Backward_Packets","backward_vs_forward",
            "Bwd_Packets_Length_Total", "Bwd_Packet_Length_Max",
            "Bwd_Packet_Length_Mean", "Flow_Bytes_per_s",
            "Flow_IAT_Mean", "Fwd_IAT_Mean", "Bwd_Packets_per_s",
            "Packet_Length_Mean", "Packet_Length_Variance",
            "Down_Up_Ratio", "Avg_Bwd_Segment_Size"
       ]

        # Select only the required features
        df_final = df_engineered.select(*features)
        print("Derived only required features...\n")
        self.logger.info("Derived  required features for Model ...\n")
        

        return df_final

    

    def temporal_correlation(self, malicious_sessions):
        window_sec = 600
        repeat_threshold = 3
        confirmed_idx = []

        malicious_sessions = malicious_sessions.copy()
        malicious_sessions["timestamp"] = malicious_sessions["start_time"] / 1000.0
        malicious_sessions = malicious_sessions.sort_values("timestamp")

        dst_ip_tracker = defaultdict(list)

        for i, row in malicious_sessions.iterrows():
            dst = row["dst_ip"]
            ts = row["timestamp"]

            dst_ip_tracker[dst].append(ts)
            dst_ip_tracker[dst] = [t for t in dst_ip_tracker[dst] if t >= ts - window_sec]

            if len(dst_ip_tracker[dst]) >= repeat_threshold:
                confirmed_idx.append(i)

        confirmed_alerts_df = malicious_sessions.loc[confirmed_idx]   

        if not confirmed_alerts_df.empty:
            print(f"  Confirmed {len(confirmed_alerts_df)} alerts after temporal correlation")
            self.logger.info(f"  Confirmed {len(confirmed_alerts_df)} alerts after temporal correlation")
        else:
            print("  No alerts met temporal correlation criteria")
            self.logger.info("  No alerts met temporal correlation criteria")

        return confirmed_alerts_df
      



    def load_existing_alerts(self):
        """
        Load existing alerts from the directory and store the src_ip and dst_ip pairs.
        Works with JSON alerts saved in alerts_dir.
        """
        existing_alerts = set()
        for filename in os.listdir(self.alerts_dir):
            if filename.endswith(".json"):
                file_path = os.path.join(self.alerts_dir, filename)
                try:
                    with open(file_path, 'r') as f:
                        json_data = json.load(f)
                        src_ip = json_data["match_body"]["network"]["src_ip"]
                        dst_ip = json_data["match_body"]["network"]["dst_ip"]
                        existing_alerts.add(f"{src_ip}_{dst_ip}")
                except Exception as e:
                    print(f"Error loading existing alert {filename}: {e}")
                    self.logger.error(f"Error loading existing alert {filename}: {e}")
        return existing_alerts

    def make_alertgeneration_http_tunnel(self, malicious_sessions):
        """
        Generate alerts from malicious_sessions DataFrame (Pandas).
        Avoid duplicates using src_ip + dst_ip keys.
        """
        #self.logger.info(f"HII am inside alert generation func...")
        # Load existing alerts to check for duplicates
        existing_alerts = self.load_existing_alerts()
        
        for _, row in malicious_sessions.iterrows():
            src_ip = row["src_ip"]
            dst_ip = row["dst_ip"]
            src_port = row["src_port"]
            dst_port = row["dst_port"]

            # Create unique key
            unique_key = f"{src_ip}_{dst_ip}"
            
            if unique_key in existing_alerts:
                continue  # Skip if already alerted

            # Build alert JSON
            json_dict = {
                "match_body": {
                    "session": {
                        "protocol": "http",   # static since filtered already
                        "session_duration": row["Flow_Duration"],

                    },
                    "network": {
                        "src_ip": src_ip,
                        "dst_ip": dst_ip,
                    },
                    "transport": {
                        "src_port": src_port,
                        "dst_port": dst_port,
                    },
                },
                "score": row["malicious_score"],
                "rule_name": "ML HTTP_TUNNEL Alert",
                "alert_type": "http-tunnel",
                #"severity": 2,
                "alert_category": "Suspicious tunnelling Anomaly",
                #"risk_score": 20,
                "priority": 1,
                "alert_description": "HTTP tunnel Anomaly",
            }

            # Generate unique filename
            timestamp = datetime.now().strftime("%Y%m%d%H%M%S%f")
            filename = f"alert-{timestamp}.json"
            
            file_path = os.path.join(self.alerts_dir, filename)


            try:
                with open(file_path, "w") as f:
                    json.dump(json_dict, f, indent=4)
                    
            except Exception as e:
                print(f"Error generating alert {filename}: {e}")
                self.logger.error(f"Error generating alert {filename}: {e}")


            # Add to existing set
            existing_alerts.add(unique_key)
            #self.logger.info(f"Aadded")
    
        print(f"Alerts generated in {self.alerts_dir}")
        self.logger.info(f"Alerts generated in {self.alerts_dir}")
    
    def delete_files(self, filenames):
        for file in filenames:
            full_file_path = os.path.join(self.http_tunnel_preprocessor_input_dir, file)
            if os.path.exists(full_file_path):
                try:
                    os.remove(full_file_path)
                    print(f" Deleted {full_file_path}")
                    
                except Exception as e:
                    print(f" Could not delete {full_file_path}: {e}")
                    self.logger.error(f" Could not delete {full_file_path}: {e}")
            else:
                print(f"File already missing, skipping: {full_file_path}")
                self.logger.info(f"File already missing, skipping: {full_file_path}")

        self.logger.info(" Deleted perquet files...")    

    def shutdown_spark_session(self):
        if self._spark_session:
            self._spark_session.stop()            



if __name__ == "__main__":
    http_tunnel_parser = Http_tunnelParser()

    try:
        while True:

            filenames = []
            try:
                extracted_data, filenames = http_tunnel_parser.extract_features()
                
                if extracted_data is not None:
                    # Deriveing model required features
                    df_final = http_tunnel_parser.derive_required_feature(extracted_data)
                    #df_final.printSchema()

                    rows = df_final.count()
                    cols = len(df_final.columns)
                    print(f"df_final shape: ({rows}, {cols})")

                    if rows == 0:
                        print(" No HTTP rows found after feature extraction. Skipping prediction.")
                        http_tunnel_parser.logger.info(" No HTTP rows found after feature extraction. Skipping prediction.")
                    else:
                        # Converting PySpark DataFrame to Pandas
                        df_pandas = df_final.toPandas()
                        print(f"converted to pandas df...\n")
                        print("df_pandas shape:", df_pandas.shape)
                        http_tunnel_parser.logger.info("converted to pandas df.")
                        
                        # Replace infinities with NaN, then fill NaN with 0
                        X_new = df_pandas.replace([np.inf, -np.inf], np.nan).fillna(0)

                        # Loading model and preprocessing pipeline
                        http_tunnel_parser.logger.info("Loading model and preprocessing pipeline.")
                        try:
                            full_pipeline = joblib.load(http_tunnel_parser.get_model_path)
                            
                        except FileNotFoundError as e:
                            print(f" Model not found: {e}")
                            http_tunnel_parser.logger.error(f" Model not found: {e}")
                            exit(1)
                        except Exception as e:
                            print(f" Error loading pipeline: {e}")
                            http_tunnel_parser.logger.error(f" Error loading pipeline: {e}")
                            exit(1)

                        # MAKE PREDICTIONS

                        http_tunnel_parser.logger.info("Start prediction...")
                        try:
                            y_pred = full_pipeline.predict(X_new)
                            
                            if hasattr(full_pipeline, 'predict_proba'):
                                y_pred_proba = full_pipeline.predict_proba(X_new)
                                print("Prediction probabilities computed")
                                http_tunnel_parser.logger.info("Prediction probabilities computed")
                            else:
                                y_pred_proba = None
                                print("  Model doesn't support probability predictions")
                                http_tunnel_parser.logger.error("Model doesn't support probability predictions")
                        except Exception as e:
                            print(f" Error during prediction: {e}")
                            http_tunnel_parser.logger.error(f" Error during prediction: {e}")
                            exit(1)

                        # Add predictions back to extracted_data
                        extracted_data=extracted_data.toPandas()
                        extracted_data = extracted_data.copy()  # to avoid modifying original
                        extracted_data["prediction"] = y_pred # bydefault threshold 0.5

                        if y_pred_proba is not None:
                            # Save probability of "malicious" class (label=1)
                            extracted_data["malicious_score"] = y_pred_proba[:, 1]

                        # Filter only malicious sessions
                        threshold = 0.8
                        malicious_sessions = extracted_data[(extracted_data["malicious_score"] > threshold)]
                        #print(f" Found {len(malicious_sessions)} malicious sessions")

                    
                        # temporal correlation of alerts by dst IP
                        if malicious_sessions.empty:
                            print(" No malicious sessions detected, skipping alert generation...")
                            http_tunnel_parser.logger.info(" No malicious sessions detected, skipping alert generation...")
                        else:
                            print(f" Found {len(malicious_sessions)} malicious sessions before temporal filtering")
                            http_tunnel_parser.logger.info(f" Found {len(malicious_sessions)} malicious sessions before temporal filtering")
                            #malicious_sessions = http_tunnel_parser.temporal_correlation(malicious_sessions)
                            if malicious_sessions.empty:
                                print(" No malicious sessions detected,after temporal filtering skipping alert generation...")
                                http_tunnel_parser.logger.info(" No malicious sessions detected,after temporal filtering skipping alert generation...")
                            else:

                                # Select only the columns I want to display
                                display_columns = [
                                    "src_ip","dst_ip","Flow_Bytes_per_s","Total_Fwd_Packets", "Total_Backward_Packets","malicious_score"
                                ]

                                # Keep only the selected columns (ignore if missing)
                                malicious_info = malicious_sessions[display_columns].copy()

                                # Print  as a table
                                print("\n=== Malicious Sessions (summary) ===")
                                print(malicious_info.to_string(index=False))
                                http_tunnel_parser.logger.info(malicious_info.to_string(index=False))
                                #https_c2_parser.logger.info(f" Found {len(malicious_sessions)} malicious sessions.")

                                http_tunnel_parser.make_alertgeneration_http_tunnel(malicious_sessions)
                else:
                    http_tunnel_parser.logger.info("No Input Data received, skipping!")
                    print("No Input Data received, skipping!")
            finally:
                # Only delete files after each iteration, don't shutdown Spark
                if filenames is not None:
                    http_tunnel_parser.delete_files(filenames)

            time.sleep(120)
    
    finally:
        # Shutdown Spark session only when exiting the entire program
        http_tunnel_parser.logger.info("shutting down spark...")
        http_tunnel_parser.shutdown_spark_session()
            
            