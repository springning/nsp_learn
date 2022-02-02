package com.nsp.test.flink.datatypes;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessSpec;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessUtils;

public class MemeoryTest {

    public static void main(String[] args) {
        final MemorySize totalProcessMemorySize = MemorySize.parse("2048m");
        @SuppressWarnings("deprecation")
        final ConfigOption<MemorySize> legacyOption = TaskManagerOptions.TOTAL_PROCESS_MEMORY;
        Configuration conf = new Configuration();
        conf.set(legacyOption, totalProcessMemorySize);
        TaskExecutorProcessSpec taskExecutorProcessSpec = TaskExecutorProcessUtils.processSpecFromConfig(conf);
        System.out.println(taskExecutorProcessSpec.getTotalProcessMemorySize());
        System.out.println(totalProcessMemorySize);
    }

}
