package org.csp.stream.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class StreamService {

    private static final Serde<String> STRING_SERDE = Serdes.String();
    @Value("${kafka.stream.inbound.topic}")
    String inboundTopic;
    @Value("${kafka.stream.outbound.topic}")
    String outboundTopic;

    @Autowired
    public void streamPipeline(StreamsBuilder streamsBuilder) {
        KStream<String, String> messageStream = streamsBuilder
                .stream(inboundTopic, Consumed.with(STRING_SERDE, STRING_SERDE))
                .peek((k, v) -> log.info("Event received with key=" + k + ", value=" + v));

        messageStream.to(outboundTopic, Produced.with(STRING_SERDE, STRING_SERDE));
    }

}
