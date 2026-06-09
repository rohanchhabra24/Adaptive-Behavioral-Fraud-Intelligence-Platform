import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)
    kafka_broker = os.getenv("KAFKA_BROKER", "kafka:29092")
    source_ddl = f"""
        CREATE TABLE transaction_enriched (
            transaction_id STRING, user_id STRING, amount DOUBLE, location STRING, is_simulated_fraud BOOLEAN, hourly_avg_spend DOUBLE, proctime AS PROCTIME()
        ) WITH ('connector' = 'kafka', 'topic' = 'transaction-enriched', 'properties.bootstrap.servers' = '{kafka_broker}', 'properties.group.id' = 'flink_behavior_group', 'scan.startup.mode' = 'latest-offset', 'format' = 'json')
    """
    t_env.execute_sql(source_ddl)
    sink_ddl = f"""
        CREATE TABLE behavior_scored (
            transaction_id STRING, user_id STRING, amount DOUBLE, location STRING, velocity_count BIGINT, behavior_score DOUBLE
        ) WITH ('connector' = 'kafka', 'topic' = 'behavior-scored', 'properties.bootstrap.servers' = '{kafka_broker}', 'format' = 'json')
    """
    t_env.execute_sql(sink_ddl)
    process_sql = """
        INSERT INTO behavior_scored
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
    """
    t_env.execute_sql(process_sql)
if __name__ == '__main__':
    main()
