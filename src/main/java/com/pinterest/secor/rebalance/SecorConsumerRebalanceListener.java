package com.pinterest.secor.rebalance;

import com.pinterest.secor.common.ZookeeperConnector;
import com.pinterest.secor.util.StatsUtil;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SecorConsumerRebalanceListener implements ConsumerRebalanceListener {

    private static final Logger LOG = LoggerFactory.getLogger(SecorConsumerRebalanceListener.class);

    private RebalanceHandler handler;


    public SecorConsumerRebalanceListener(RebalanceHandler handler) {
        this.handler = handler;
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> assignedPartitions) {
        // Here we do a force upload/commit so the other consumer take over partition(s) later don't need to rewrite the same messages
        LOG.info("re-balance starting, forcing uploading current assigned partitions {}", assignedPartitions);

        List<com.pinterest.secor.common.TopicPartition> tps = assignedPartitions.stream().map(p -> new com.pinterest.secor.common.TopicPartition(p.topic(), p.partition())).collect(Collectors.toList());
        tps.stream().map(p -> p.getTopic()).collect(Collectors.toSet()).forEach(topic -> StatsUtil.incr("secor.consumer_rebalance_count." + topic));
        handler.uploadOnRevoke(tps);
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> collection) {
        // Pass
    }

}
