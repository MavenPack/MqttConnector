package com.yeild.mqtt.listener;

import com.yeild.mqtt.PushMqttMessage;

public interface OnMqttMessageListener {
	void onMqttReceiveMessage(PushMqttMessage pmessage);
	void pushMessageResult(PushMqttMessage message, Error error);
}
