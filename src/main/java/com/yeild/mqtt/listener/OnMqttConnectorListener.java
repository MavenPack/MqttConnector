package com.yeild.mqtt.listener;

import com.yeild.mqtt.PushMqttMessage;

public interface OnMqttConnectorListener {
	void onMqttReceiveMessage(PushMqttMessage pmessage);
}
