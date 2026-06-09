import os, json
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)
    kafka_broker = os.getenv("KAFKA_BROKER", "kafka:29092")
    source_ddl = f"""
        CREATE TABLE transaction_raw (
            transaction_id STRING, user_id STRING, amount DOUBLE, location STRING, `timestamp` BIGINT, is_simulated_fraud BOOLEAN, ts AS TO_TIMESTAMP_LTZ(`timestamp`, 3), WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
        ) WITH ('connector' = 'kafka', 'topic' = 'transaction-raw', 'properties.bootstrap.servers' = '{kafka_broker}', 'properties.group.id' = 'flink_enrichment_group', 'scan.startup.mode' = 'latest-offset', 'format' = 'json')
    """
    t_env.execute_sql(source_ddl)
    sink_ddl = f"""
        CREATE TABLE transaction_enriched (
            transaction_id STRING, user_id STRING, amount DOUBLE, location STRING, is_simulated_fraud BOOLEAN, hourly_avg_spend DOUBLE
        ) WITH ('connector' = 'kafka', 'topic' = 'transaction-enriched', 'properties.bootstrap.servers' = '{kafka_broker}', 'format' = 'json')
    """
    t_env.execute_sql(sink_ddl)
    process_sql = """
        INSERT INTO transaction_enriched
        SELECT transaction_id, user_id, amount, location, is_simulated_fraud, amount * 0.8 AS hourly_avg_spend
        FROM transaction_raw WHERE amount > 50
    """
    t_env.execute_sql(process_sql)
if __name__ == '__main__':
    main()
