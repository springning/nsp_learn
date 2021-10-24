package org.example.bo;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Rowtime;
import org.apache.flink.table.descriptors.Schema;

public class TableEnvironmentDemo {

    public static void main(String[] args) {

    }

    public static StreamTableEnvironment createBlinkStreamTableEnvironment() {
        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance().inStreamingMode()
                .useBlinkPlanner().build();
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(streamExecutionEnvironment, environmentSettings);
        return streamTableEnvironment;
    }

    public static TableEnvironment createBlinkBatchTableEnvironment() {
        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();
        TableEnvironment tableEnvironment = TableEnvironment.create(environmentSettings);
        return tableEnvironment;
    }

    public static TableEnvironment createFlinkStreamEnvironment() {
        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
        StreamExecutionEnvironment fsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(fsEnv, fsSettings);
        return streamTableEnvironment;
    }

    public static TableEnvironment createFlinkBatchEnvironment() {
        ExecutionEnvironment fbEnv = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment batchTableEnvironment = BatchTableEnvironment.create(fbEnv);
        return batchTableEnvironment;
    }

    public static void createConnectorTable() {
        TableEnvironment blinkStreamTableEnvironment = createBlinkStreamTableEnvironment();
        blinkStreamTableEnvironment.connect(
                new Kafka().version("0.10").topic("test")
                        .property("bootstrap.servers", "").property("group.id", "")
                        .startFromGroupOffsets())
                .withFormat(new Json().failOnMissingField(false).deriveSchema())
                .withSchema(new Schema()
                        .field("rowtime", Types.SQL_TIMESTAMP)
                        .rowtime(new Rowtime()
                                .timestampsFromField("eventtime")
                                .watermarksPeriodicBounded(2000)
                        )
                        .field("fruit", Types.STRING)
                        .field("number", Types.INT))
                .inAppendMode()
                .createTemporaryTable("xxx");
    }
}
