import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)
    
    t_env.get_config().get_configuration().set_string("pipeline.jars", "file:///opt/flink/lib/flink-sql-connector-kafka-1.17.1.jar")
    
    kafka_broker = os.getenv("KAFKA_BROKER", "kafka:29092")

    # 1. Source Table (Raw Transactions)
    t_env.execute_sql(f"""
        CREATE TABLE transaction_raw (
            transaction_id STRING, 
            user_id STRING, 
            amount DOUBLE, 
            location STRING, 
            `timestamp` BIGINT, 
            is_simulated_fraud BOOLEAN, 
            ts AS TO_TIMESTAMP_LTZ(`timestamp`, 3), 
            WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka', 
            'topic' = 'transaction-raw', 
            'properties.bootstrap.servers' = '{kafka_broker}', 
            'properties.group.id' = 'flink_unified_group', 
            'scan.startup.mode' = 'latest-offset', 
            'format' = 'json'
        )
    """)

    # 2. Sink Table (Flink Suspects to be judged by OpenAI)
    t_env.execute_sql(f"""
        CREATE TABLE flink_suspects (
            transaction_id STRING, 
            risk_score DOUBLE, 
            severity STRING, 
            reason STRING
        ) WITH (
            'connector' = 'kafka', 
            'topic' = 'flink-suspects', 
            'properties.bootstrap.servers' = '{kafka_broker}', 
            'format' = 'json'
        )
    """)

    # 3. Intermediate Views (In-Memory Processing)
    t_env.execute_sql("""
        CREATE TEMPORARY VIEW transaction_enriched AS
        SELECT 
            transaction_id, 
            user_id, 
            amount, 
            location, 
            is_simulated_fraud, 
            AVG(amount) OVER (
                PARTITION BY user_id 
                ORDER BY ts 
                RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND CURRENT ROW
            ) AS hourly_avg_spend,
            PROCTIME() AS proctime
        FROM transaction_raw 
        WHERE amount > 50
    """)

    t_env.execute_sql("""
        CREATE TEMPORARY VIEW behavior_scored AS
        SELECT 
            transaction_id, 
            user_id, 
            amount, 
            location, 
            COUNT(transaction_id) OVER (
                PARTITION BY user_id 
                ORDER BY proctime 
                RANGE BETWEEN INTERVAL '10' MINUTE PRECEDING AND CURRENT ROW
            ) AS velocity_count, 
            CASE 
                WHEN hourly_avg_spend > 0 THEN (amount / hourly_avg_spend) * 10 
                ELSE 0.0 
            END AS behavior_score
        FROM transaction_enriched
    """)

    # 4. Final Insert into Sink (Executes the whole graph)
    process_sql = """
        INSERT INTO flink_suspects
        SELECT 
            transaction_id, 
            (behavior_score * 3.0) + (velocity_count * 10.0) AS risk_score, 
            CASE 
                WHEN ((behavior_score * 3.0) + (velocity_count * 10.0)) >= 80 THEN 'HIGH RISK' 
                WHEN ((behavior_score * 3.0) + (velocity_count * 10.0)) >= 50 THEN 'MEDIUM RISK'
                ELSE 'LOW RISK' 
            END AS severity, 
            CASE 
                WHEN velocity_count > 5 THEN 'High velocity anomalous spend detected (Rapid Transactions)'
                WHEN ((behavior_score * 3.0) + (velocity_count * 10.0)) >= 80 THEN 'Anomalous behavior spike compared to user history'
                WHEN ((behavior_score * 3.0) + (velocity_count * 10.0)) >= 50 THEN 'Slightly abnormal velocity pattern'
                ELSE 'Normal behavior'
            END AS reason
        FROM behavior_scored
    """
    
    # execute_sql() on an INSERT statement triggers the job execution asynchronously
    t_env.execute_sql(process_sql)

if __name__ == '__main__':
    main()
