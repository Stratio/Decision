package com.stratio.streaming.task;

import com.stratio.streaming.service.StreamingFailoverService;

public class FailOverTask implements Runnable {

    private final StreamingFailoverService streamingFailoverService;

    public FailOverTask(StreamingFailoverService streamingFailoverService) {
        this.streamingFailoverService = streamingFailoverService;
        streamingFailoverService.load();
    }

    @Override
    public void run() {
        streamingFailoverService.save();
    }

}
