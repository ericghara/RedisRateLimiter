package com.ericgha.service.snapshot_consumer;

import com.ericgha.dto.EventStatus;
import com.ericgha.dto.EventTime;
import com.ericgha.dto.message.KeyFrameMessage;
import com.ericgha.service.snapshot_mapper.SnapshotMapper;
import org.springframework.messaging.simp.SimpMessagingTemplate;

import java.util.ArrayList;
import java.util.List;

public class SnapshotSTOMPMessenger implements SnapshotConsumer {

    private final SimpMessagingTemplate template;
    private final SnapshotMapper<EventStatus> snapshotMapper;
    private final String prefix;
    private int chunkSize = 1_000;

    public SnapshotSTOMPMessenger(SimpMessagingTemplate template, String prefix, SnapshotMapper<EventStatus> snapshotMapper) {
        this.template = template;
        this.prefix = prefix;
        this.snapshotMapper = snapshotMapper;
    }

    public int chunkSize() {
        return this.chunkSize;
    }

    public void chunkSize(int chunkSize) {
        this.chunkSize = chunkSize;
    }

    List<EventStatus> chunkedMap(List<EventTime> snapshot) {
        List<EventStatus> mapped = new ArrayList<>(snapshot.size());
        int mappedI = 0;
        while (mappedI < snapshot.size() ) {
            int thisChunkSize = Math.min(chunkSize, snapshot.size() - mappedI);
            List<EventTime> chunk = snapshot.subList(mappedI, mappedI+thisChunkSize);
            List<EventStatus> mappedChunk = snapshotMapper.apply(chunk);
            mapped.addAll(mappedChunk);
            mappedI = mapped.size();
        }
        return mapped;
    }

    @Override
    public void accept(Long timestamp, List<EventTime> snapshot) {
        List<EventStatus> mappedSnapshot = chunkedMap( snapshot );
        KeyFrameMessage keyFrameMessage = new KeyFrameMessage( timestamp, mappedSnapshot );
        template.convertAndSend( prefix, keyFrameMessage );
    }

    public String prefix() {
        return this.prefix;
    }

}
