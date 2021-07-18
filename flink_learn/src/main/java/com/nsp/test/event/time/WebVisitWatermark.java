package com.nsp.test.event.time;

import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.example.bo.WebVisit;

public class WebVisitWatermark implements WatermarkStrategy<WebVisit> {

    @Override
    public TimestampAssigner<WebVisit> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
        return (webVisit, recordTimestamp) -> webVisit.getOpenTime().getTime();
    }

    @Override
    public WatermarkGenerator<WebVisit> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
        return new WatermarkGenerator<WebVisit>() {
            private long MAX_EVENTTIME = 0L;
            private long ALLOW_DELAY = 2000;
            @Override
            public void onEvent(WebVisit event, long eventTimestamp, WatermarkOutput output) {
                long max = Math.max(MAX_EVENTTIME, event.getOpenTimestamp() - ALLOW_DELAY);
                Watermark watermark = new Watermark(max);
                output.emitWatermark(watermark);
            }

            @Override
            public void onPeriodicEmit(WatermarkOutput output) {

            }
        };
    }
}
