package com.linkedin.camus.etl.kafka.coders;

import java.util.Properties;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.coders.MessageDecoder;

/**
 * MessageDecoder class that don't do any conversion,
 * This MessageDecoder returns a CamusWrapper that works with bytes[] payloads.
 * 
 * @author Marcelo Valle (mvalleavila@gmail.com https://github.com/mvalleavila)
 * 
 */


public class BytesMessageDecoder extends MessageDecoder<byte[], byte[]> {

	@Override
	public void init(Properties props, String topicName) {
		this.props     = props;
		this.topicName = topicName;
	}

	@Override
	public CamusWrapper<byte[]> decode(byte[] payload) {
		long       timestamp = 0;

		// Set the timestamp to current time.
		timestamp = System.currentTimeMillis();

		return new CamusWrapper<byte[]>(payload, timestamp);
	}
}
