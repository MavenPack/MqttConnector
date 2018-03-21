package com.yeild.mqtt;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallbackExtended;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.MqttPersistenceException;
import org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence;

import com.yeild.common.Utils.CommonUtils;

public class MqttServerTask extends Thread implements MqttCallbackExtended {
	Logger logger = Logger.getLogger(getClass().getSimpleName());
	private MqttClient mqttClient;
	private MqttConfig mqttConfig;
	private String mConfPath;
	private LinkedBlockingQueue<PushMqttMessage> pushMsgQueue = null;
	private boolean runningTask = true;
	private boolean mIsLogined = false;
	private Exception lastException;
	
	public interface OnMqttConnectorListener {public void onMqttReceiveMessage(PushMqttMessage pmessage);}
	
	private ArrayList<OnMqttConnectorListener> receiveMessageListeners = new ArrayList<OnMqttConnectorListener>(1);

	/**
	 * 
	 * @param confPath the path of mqtt config file
	 */
	public MqttServerTask(String confPath) {
		this.mConfPath = confPath;
	}
	
	public void addMqttConnectorListener(OnMqttConnectorListener pListener) {
		if(!this.receiveMessageListeners.contains(pListener)) {
			this.receiveMessageListeners.add(pListener);
		}
	}
	
	public void removeMqttConnectorListener(OnMqttConnectorListener pListener) {
		this.receiveMessageListeners.remove(pListener);
	}
	
	public String getRpcResponseName() {
		return mqttConfig.getRpcResponseName();
	}
	public String getRpcRequestName() {
		return mqttConfig.getRpcRequestName();
	}
	
	public String getRpcTopicPrefix() {
		return mqttConfig.getRpcTopicPrefix();
	}
	
	public String getNotifyTopicPre() {
		return mqttConfig.getNotifyTopicPre();
	}
	
	public boolean isLogined() {
		return mIsLogined;
	}
	
	public Exception getLastException() {
		return lastException;
	}
	
	public boolean pushMessage(PushMqttMessage message) {
		try {
			return this.pushMsgQueue.offer(message, 3, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			return false;
		}
	}
	
	public boolean pushMessageAsync(PushMqttMessage message) {
		try {
			mqttClient.publish(message.getTopic(), (MqttMessage)message);
			return true;
		} catch (MqttPersistenceException e) {
			logger.error(CommonUtils.getExceptionInfo(e));
		} catch (MqttException e) {
			logger.error(CommonUtils.getExceptionInfo(e));
		}
		return false;
	}
	
	/**
	 * wait the mqtt server login success, no wait time limited
	 * @return
	 */
	public boolean waitLoginComplete() {
		return waitLoginComplete(0);
	}
	
	/**
	 * wait the mqtt server login success
	 * @param timeout the time to wait, measured in milliseconds
	 * @return
	 */
	public boolean waitLoginComplete(int timeout) {
		long waitBegin = 0;
		while(!isLogined()) {
			if(timeout>0) {
				if(waitBegin < 1) {
					waitBegin = System.currentTimeMillis();
				} else if(System.currentTimeMillis() - waitBegin > timeout) {
					break;
				}
			}
			if(getLastException() != null) {
				logger.error(CommonUtils.getExceptionInfo(getLastException()));
				break;
			}
			try {
				Thread.sleep(1*1000);
			} catch (InterruptedException e) { }
		}
		return isLogined();
	}
	
	@Override
	public void run() {
		mIsLogined = false;
		try {
			mqttConfig.load(mConfPath);
		} catch (IOException e) {
			lastException = e;
			return;
		}
		pushMsgQueue = new LinkedBlockingQueue<PushMqttMessage>(mqttConfig.getMaxMessageQueue());
		int retryTimesLimit = 5;
		int retryTimes=0;
		while(true) {
			try {
				connnect();
				logger.info("the server login success");
				break;
			} catch (MqttException e) {
				logger.debug(CommonUtils.getExceptionInfo(e));
				if(retryTimes > retryTimesLimit) {
					lastException = e;
					return;
				} else {
					try {
						Thread.sleep(2*1000);
					} catch (InterruptedException e1) { }
					retryTimes ++;
				}
			}
		}
		while(runningTask) {
			PushMqttMessage pushMsg = null;
			try {
				pushMsg = pushMsgQueue.take();
				if(!mqttClient.isConnected()) {
					this.pushMessage(pushMsg);
					Thread.sleep(1*1000);
					continue;
				}
				mqttClient.publish(pushMsg.getTopic(), (MqttMessage)pushMsg);
			} catch (Exception e) {
				e.printStackTrace();
				if(pushMsg != null) {
					this.pushMessage(pushMsg);
				}
			}
		}
	}

	private void connnect() throws MqttException {
		MqttConnectOptions connectOptions = new MqttConnectOptions();
		connectOptions.setCleanSession(true);
		if(mqttConfig.getUrl() != null && mqttConfig.getSslSocketFac() != null) {
			connectOptions.setSocketFactory(mqttConfig.getSslSocketFac());
			logger.debug("mqtt server will login with ssl");
		} else {
			logger.debug("mqtt server will login with tcp");
		}
		connectOptions.setServerURIs(new String[]{mqttConfig.getUrl()});
		String username = mqttConfig.getUsername();
		if(username.length()>1) {
			connectOptions.setUserName(username);
			connectOptions.setPassword(mqttConfig.getPassword().toCharArray());
		}
		connectOptions.setConnectionTimeout(10);
		connectOptions.setKeepAliveInterval(mqttConfig.getKeepalive());
		connectOptions.setAutomaticReconnect(true);
		connectOptions.setWill(mqttConfig.getWillTopic(), mqttConfig.getWillMsg().getBytes(), 2, true);
		
		mqttClient = new MqttClient(mqttConfig.getUrl(), mqttConfig.getClientid(), new MqttDefaultFilePersistence(mConfPath+"/../mqtt"));
		mqttClient.setCallback(this);
		mqttClient.setTimeToWait(10*1000);
		
		mqttClient.connect(connectOptions);
	}

	@Override
	public void connectionLost(Throwable cause) {
		logger.info("the server lost connection, trying reconnect");
		logger.error(CommonUtils.getExceptionInfo(cause));
		mIsLogined = false;
	}

	@Override
	public void messageArrived(String topic, MqttMessage message) throws Exception {
		String msgcontent = new String(message.getPayload(), "UTF-8");
		logger.debug(topic+" received:"+msgcontent);
//		if(topic.startsWith(rpcTopicPrefix)) {
			PushMqttMessage mqttMessage = new PushMqttMessage(topic, message);
			for(OnMqttConnectorListener tListener : receiveMessageListeners) {
				try {
					tListener.onMqttReceiveMessage(mqttMessage);
				} catch (Exception e) {
					logger.error(CommonUtils.getExceptionInfo(e));
				}
			}
//		}
	}

	@Override
	public void deliveryComplete(IMqttDeliveryToken token) {
		if(token.getMessageId() == 0) {
			return;
		}
		if(!token.isComplete()) {
			logger.debug("message " + token.getMessageId()+" publish failed\n"+(token.getException()==null?"":CommonUtils.getExceptionInfo(token.getException())));
		}
	}

	@Override
	public void connectComplete(boolean reconnect, String serverURI) {
		if(reconnect) {
			logger.info("the server reconnect success");
		}
		try {
			mqttClient.subscribe(mqttConfig.getRpcTopicPrefix()+"#", 0);
			
			PushMqttMessage onlineMsg = new PushMqttMessage();
			onlineMsg.setTopic(mqttConfig.getWillTopic());
			onlineMsg.setPayload("1".getBytes());
			onlineMsg.setQos(0);
			onlineMsg.setRetained(true);
			if(!pushMessage(onlineMsg)) {
				logger.debug("online push failed");
			}
		} catch (MqttException e) {
			logger.error(CommonUtils.getExceptionInfo(e));
			lastException = e;
			return;
		}
		mIsLogined = true;
	}
}
