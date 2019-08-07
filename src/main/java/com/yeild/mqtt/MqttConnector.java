package com.yeild.mqtt;

import java.io.IOException;
import java.util.ArrayList;
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
import com.yeild.mqtt.listener.OnMqttMessageListener;

public class MqttConnector implements Runnable,MqttCallbackExtended {
	protected Logger logger = Logger.getLogger(getClass());
	protected MqttClient mqttClient;
	protected MqttConfig mqttConfig;
	protected String mConfPath;
	protected LinkedBlockingQueue<PushMqttMessage> pushMsgQueue = null;
	protected boolean runningTask = true;
	protected boolean mIsLogined = false;
	protected Exception lastException;
	
	protected ArrayList<OnMqttMessageListener> messageListeners = new ArrayList<OnMqttMessageListener>(1);

	/**
	 * 
	 * @param confPath the path of mqtt config file
	 */
	public MqttConnector(String confPath) {
		this.mConfPath = confPath;
	}

	public MqttConnector(MqttConfig config) {
		this.mqttConfig = config;
	}
	
	public MqttConfig getMqttConfig() {
		return mqttConfig;
	}
	
	public void addMqttMessageListener(OnMqttMessageListener pListener) {
		if(!this.messageListeners.contains(pListener)) {
			this.messageListeners.add(pListener);
		}
	}
	
	public void removeMqttMessageListener(OnMqttMessageListener pListener) {
		this.messageListeners.remove(pListener);
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
		if(mqttConfig == null) {
			mqttConfig = new MqttConfig();
			try {
				mqttConfig.load(mConfPath);
			} catch (IOException e) {
				lastException = e;
				return;
			}
		}
		try {
			mqttConfig.checkValid();
		} catch (Exception e2) {
			lastException = e2;
			logger.error(CommonUtils.getExceptionInfo(e2));
			return;
		}
		pushMsgQueue = new LinkedBlockingQueue<PushMqttMessage>(mqttConfig.getMaxMessageQueue());
		int retryTimesLimit = 5;
		int retryTimes=0;
		while(true) {
			try {
				connnect();
				logger.info("login success");
				break;
			} catch (MqttException e) {
				logger.debug(CommonUtils.getExceptionInfo(e));
				logger.debug(mqttConfig.getUsername()+"----"+mqttConfig.getPassword());
				if(retryTimes > retryTimesLimit) {
					lastException = e;
					return;
				} else {
					try {
						Thread.sleep(1*1000);
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
					Thread.sleep(2*1000);
					continue;
				}
			} catch (Exception e) {
				e.printStackTrace();
				if(pushMsg != null) {
					this.pushMessage(pushMsg);
				}
			}
			try {
				mqttClient.publish(pushMsg.getTopic(), pushMsg);
				callPushMessageResult(pushMsg, null);
			} catch (MqttException e) {
				logger.error(CommonUtils.getExceptionInfo(e));
				callPushMessageResult(pushMsg, new Error(e.getMessage()));
				try {
					Thread.sleep(1*1000);
				} catch (InterruptedException e1) { }
				if(pushMsg != null) {
					pushMsg.setRetry_count(pushMsg.getRetry_count()+1);
					this.pushMessage(pushMsg);
				}
			}
		}
	}

	@Override
	public void connectionLost(Throwable cause) {
		logger.info("mqtt lost connection, trying reconnect");
		logger.error(CommonUtils.getExceptionInfo(cause));
		mIsLogined = false;
	}

	@Override
	public void messageArrived(String topic, MqttMessage message) throws Exception {
		String msgcontent = new String(message.getPayload(), "UTF-8");
		logger.debug(topic+" received:"+msgcontent);
		PushMqttMessage mqttMessage = new PushMqttMessage(topic, message);
		mqttMessage.setRpc(topic.startsWith(mqttConfig.getRpcTopicPrefix()));
		callReceiveMessage(mqttMessage);
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
			logger.info("mqtt reconnect success");
		}
		initAfterConnect();
	}
	
	protected void initAfterConnect() {
		try {
			mqttClient.subscribeWithResponse(mqttConfig.getRpcTopicPrefix()+"#", 0).waitForCompletion();
			
			PushMqttMessage onlineMsg = new PushMqttMessage();
			onlineMsg.setTopic(mqttConfig.getWillTopic());
			onlineMsg.setPayload("1".getBytes());
			onlineMsg.setQos(1);
			onlineMsg.setRetained(true);
			if(!pushMessage(onlineMsg)) {
				logger.debug("online push failed");
			}
			mIsLogined = true;
		} catch (MqttException e) {
			logger.error(CommonUtils.getExceptionInfo(e));
			lastException = e;
		}
	}

	private void connnect() throws MqttException {
		MqttConnectOptions connectOptions = new MqttConnectOptions();
		connectOptions.setCleanSession(true);
		if(mqttConfig.getUri() != null && mqttConfig.getSslSocketFac() != null) {
			connectOptions.setSocketFactory(mqttConfig.getSslSocketFac());
			logger.debug("mqtt will login with ssl");
		} else {
			logger.debug("mqtt will login with tcp");
		}
		connectOptions.setServerURIs(new String[]{mqttConfig.getUri()});
		String username = mqttConfig.getUsername();
		if(username.length()>1) {
			connectOptions.setUserName(username);
			connectOptions.setPassword(mqttConfig.getPassword().toCharArray());
		}
		connectOptions.setConnectionTimeout(10);
		connectOptions.setKeepAliveInterval(mqttConfig.getKeepalive());
		connectOptions.setAutomaticReconnect(true);
		connectOptions.setWill(mqttConfig.getWillTopic(), mqttConfig.getWillMsg().getBytes(), 2, true);
		
		mqttClient = new MqttClient(mqttConfig.getUri(), mqttConfig.getClientid(), new MqttDefaultFilePersistence(mConfPath+"/../mqtt"));
		mqttClient.setCallback(this);
		mqttClient.setTimeToWait(10*1000);
		
		mqttClient.connect(connectOptions);
	}
	
	protected void callReceiveMessage(PushMqttMessage message) {
		for(OnMqttMessageListener tListener : messageListeners) {
			try {
				tListener.onMqttReceiveMessage(message);
			} catch (Exception e) {
				logger.error(CommonUtils.getExceptionInfo(e));
			}
		}
	}
	
	protected void callPushMessageResult(PushMqttMessage message, Error error) {
		for(OnMqttMessageListener tListener : messageListeners) {
			try {
				tListener.pushMessageResult(message, error);
			} catch (Exception e) {
				logger.error(CommonUtils.getExceptionInfo(e));
			}
		}
	}
}
