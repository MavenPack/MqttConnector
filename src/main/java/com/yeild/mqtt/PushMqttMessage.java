package com.yeild.mqtt;

import java.io.UnsupportedEncodingException;

import org.eclipse.paho.client.mqttv3.MqttMessage;

public class PushMqttMessage extends MqttMessage {
	private String topic;
	private int retry_count = 0;
	private boolean isRpc;
	
	public PushMqttMessage() {
	}
	
	public PushMqttMessage(MqttMessage message) {
		this(null, message);
	}
	
	public PushMqttMessage(String topic, MqttMessage message) {
		this.topic = topic;
		if(message != null) {
			this.setId(message.getId());
			this.setQos(message.getQos());
			this.setRetained(message.isRetained());
			this.setPayload(message.getPayload());
		}
	}
	
	public void setPayload(String msg) {
		try {
			setPayload(msg.getBytes("UTF-8"));
		} catch (UnsupportedEncodingException e) {
			setPayload(msg.getBytes());
		}
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic.replaceAll("[^0-9a-zA-Z/_\\-+#]*", "");
	}
	
	public void setRetry_count(int retry_count) {
		this.retry_count = retry_count;
	}
	
	public int getRetry_count() {
		return retry_count;
	}
	
	public boolean isRpc() {
		return isRpc;
	}

	public void setRpc(boolean isRpc) {
		this.isRpc = isRpc;
	}
	
}
