package com.ericgha.service.snapshot_consumer;

import java.util.List;
import java.util.function.BiConsumer;

public interface SnapshotConsumer<T> extends BiConsumer<Long, List<T>> {

}
