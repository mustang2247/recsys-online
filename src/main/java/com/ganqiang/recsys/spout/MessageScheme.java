package com.ganqiang.recsys.spout;

import org.apache.storm.spout.Scheme;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.List;

@SuppressWarnings("serial")
public class MessageScheme implements Scheme {

	public List<Object> deserialize(byte[] ser) {
		try {
			String msg = new String(ser, "UTF-8");
			return new Values(msg);
		} catch (UnsupportedEncodingException e) {

		}
		return null;
	}

	@Override
	public List<Object> deserialize(ByteBuffer ser) {
		return null;
	}

	public Fields getOutputFields() {
		return new Fields("msg");
	}
}