import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)
    kafka_broker = os.getenv("KAFKA_BROKER", "kafka:29092")
    source_ddl = f"""
        CREATE TABLE behavior_scored (
            transaction_id STRING, user_id STRING, amount DOUBLE, location STRING, velocity_count BIGINT, behavior_score DOUBLE
        ) WITH ('connector' = 'kafka', 'topic' = 'behavior-scored', 'properties.bootstrap.servers' = '{kafka_broker}', 'properties.group.id' = 'flink_risk_group', 'scan.startup.mode' = 'latest-offset', 'format' = 'json')
    """
    t_env.execute_sql(source_ddl)
    sink_ddl = f"""
        CREATE TABLE fraud_alerts (
            transaction_id STRING, risk_score DOUBLE, severity STRING, reason STRING
        ) WITH ('connector' = 'kafka', 'topic' = 'fraud-alerts', 'properties.bootstrap.servers' = '{kafka_broker}', 'format' = 'json')
    """
    t_env.execute_sql(sink_ddl)
    process_sql = """
        INSERT INTO fraud_alerts
        SELECT 
            transaction_id, 
            behavior_score * 5.0 AS risk_score, 
            CASE 
                WHEN (behavior_score * 5.0) >= 80 THEN 'HIGH RISK' 
                WHEN (behavior_score * 5.0) >= 50 THEN 'MEDIUM RISK'
                ELSE 'LOW RISK' 
            END AS severity, 
            CASE 
                WHEN (behavior_score * 5.0) >= 80 THEN 'High velocity anomalous spend detected'
                WHEN (behavior_score * 5.0) >= 50 THEN 'Slightly abnormal velocity pattern'
                ELSE 'Normal behavior'
            END AS reason
        FROM behavior_scored
    """
    t_env.execute_sql(process_sql)
if __name__ == '__main__':
    main()
