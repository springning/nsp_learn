package com.nsp.flink.sql;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class SinkEs {

    public static void main(String[] args) {

        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance().inStreamingMode()
                .useBlinkPlanner().build();

        //启用并设置checkpoint相关属性
        streamExecutionEnvironment.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);
        streamExecutionEnvironment.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        streamExecutionEnvironment.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        streamExecutionEnvironment.getCheckpointConfig().setCheckpointTimeout(60000);

//        streamExecutionEnvironment.setStateBackend(new HashMapStateBackend());
//        streamExecutionEnvironment.getCheckpointConfig().setCheckpointStorage(new FileSystemCheckpointStorage("hdfs://xxxx"));

        streamExecutionEnvironment.setStateBackend(new EmbeddedRocksDBStateBackend());
        streamExecutionEnvironment.getCheckpointConfig().setCheckpointStorage(new JobManagerCheckpointStorage());
        streamExecutionEnvironment.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        //重启策略
        streamExecutionEnvironment.setRestartStrategy(RestartStrategies.noRestart());



        String sourceSql = "create table shafa_mid_log_json (\n" +
                "    ext String,\n" +
                "    os String,\n" +
                "    agent_id String,\n" +
                "    openid String,\n" +
                "    ctx String,\n" +
                "    ip String,\n" +
                "    network String,\n" +
                "    uid String,\n" +
                "    admin_id String,\n" +
                "    host String,\n" +
                "    referral_id String,\n" +
                "    theme String,\n" +
                "    tag String,\n" +
                "    page String,\n" +
                "    track String,\n" +
                "    channel_id String,\n" +
                "    map_json String,\n" +
                "    vt String,\n" +
                "    ua String,\n" +
                "    domain String,\n" +
                "    reqtime BIGINT,\n" +
                "    ts AS TO_TIMESTAMP(FROM_UNIXTIME(reqtime/1000,'yyyy-MM-dd HH:mm:ss')), -- flink 官方版本暂不支持 传入 bigint的类型.阿里云文档中可以.\n" +
                "    proctime as PROCTIME(),   -- 通过计算列产生一个处理时间列\n" +
                "    WATERMARK FOR ts as ts - INTERVAL '5' SECOND  -- 在et上定义watermark，et成为事件时间列\n" +
                ")\n" +
                "WITH (\n" +
                "    connector.type' = 'kafka',\n" +
                "    connector.version' = 'universal',\n" +
                "    connector.topic' = 'mid_shafa_json_log',\n" +
                "    connector.startup-mode' = 'latest-offset',\n" +
                "    connector.properties.group.id' = 'flink_sql_consume_mid_json',\n" +
                "    connector.properties.zookeeper.connect' = '%s',\n" +
                "    connector.properties.bootstrap.servers' = '%s',\n" +
                "    format.type' = 'json',\n" +
                "    update-mode'='append'\n" +
                ")";

        String sinkSql = "CREATE TABLE sink_es_shafa_pv_uv (\n" +
                "\tts  STRING,\n" +
                "\tchannel_id String,\n" +
                "\tpv BIGINT,\n" +
                "\tuv BIGINT\n" +
                "\t) WITH (\n" +
                "\t'connector.type' = 'elasticsearch',\n" +
                "\t'connector.version' = '7',\n" +
                "\t'connector.hosts' = '%s',\n" +
                "\t'connector.index' = 'shafa_pv_uv_dh',\n" +
                "\t'connector.document-type' = 'shafa_pv_uv',\n" +
                "\t'connector.bulk-flush.max-actions' = '1',\n" +
                "\t'format.type' = 'json',\n" +
                "\t'update-mode' = 'upsert'\n" +
                ")";

        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(streamExecutionEnvironment, environmentSettings);
        streamTableEnvironment.executeSql(String.format(sourceSql, "", ""));
        streamTableEnvironment.executeSql(String.format(sinkSql, ""));

        streamTableEnvironment.executeSql("select\n" +
                "\tDATE_FORMAT(TUMBLE_START(ts, interval '10' minute),'yyyyMMddHHmm') as ts,\n" +
                "\t'all' as channel_id,\n" +
                "\tcount(uid) as pv,\n" +
                "\tcount(distinct uid) as pv\n" +
                "\tfrom shafa_mid_log_json\n" +
                "\tgroup by tumble(ts,interval '10' minute )");

    }
}
