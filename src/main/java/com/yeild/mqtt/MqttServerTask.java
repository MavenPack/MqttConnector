package com.yeild.mqtt;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
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
import com.yeild.common.Utils.ConvertUtils;

public class MqttServerTask extends Thread implements MqttCallbackExtended {
	Logger logger = Logger.getLogger(getClass().getSimpleName());
	private MqttClient mqttClient;
	private String mConfPath;
	private Properties mProperties = null;
	private Properties mSSLProperties = null;
	private String mWillTopic = null;
	private LinkedBlockingQueue<PushMqttMessage> pushMsgQueue = null;
	private boolean runningTask = true;
	private String rpcTopicPrefix="/server/rpc/";
	private String rpcResponseName = null;
	private String rpcRequestName = null;
	private String mNotifyTopicPre = null;
	private boolean mIsLogined = false;
	private Exception lastException;
	
	public interface OnMqttConnectorListener {public void onMqttReceiveMessage(MqttMessage pmessage);}
	
	private ArrayList<OnMqttConnectorListener> receiveMessageListeners = new ArrayList<OnMqttConnectorListener>(1);

	/**
	 * 
	 * @param confPath mqtt config file
	 * @param sslConfPath mqtt ssl config file
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
		return rpcResponseName;
	}
	public String getRpcRequestName() {
		return rpcRequestName;
	}
	
	public String getNotifyTopicPre() {
		return mNotifyTopicPre;
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
			if(!loadConfig()) {
				return;
			}
		} catch (IOException e) {
			lastException = e;
			return;
		}
		mSSLProperties.setProperty("com.ibm.ssl.keyStore", mConfPath+mSSLProperties.getProperty("com.ibm.ssl.keyStore"));
		mSSLProperties.setProperty("com.ibm.ssl.trustStore", mConfPath+mSSLProperties.getProperty("com.ibm.ssl.trustStore"));
		if(mSSLProperties.getProperty("com.ibm.ssl.privateStore","").length()>0) {
			mSSLProperties.setProperty("com.ibm.ssl.privateStore", mConfPath+mSSLProperties.getProperty("com.ibm.ssl.privateStore"));
		}
		pushMsgQueue = new LinkedBlockingQueue<PushMqttMessage>(ConvertUtils.parseInt(mProperties.getProperty("mqtt.push.messagequeue"), -1));
		while(true) {
			try {
				connnect();
				logger.info("the server login success");
				break;
			} catch (MqttException e) {
				logger.debug(CommonUtils.getExceptionInfo(e));
				try {
					Thread.sleep(2*1000);
				} catch (InterruptedException e1) { }
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
		connectOptions.setCleanSession(false);
		try {
			String sslUrl = mProperties.getProperty("mqtt.uri.ssl");
			if(sslUrl == null) {
				throw new Exception("not found ssl url config");
			}
			connectOptions.setSocketFactory(MqttSSLCreator.getSSLSocktet(mSSLProperties.getProperty("com.ibm.ssl.trustStore")
					, mSSLProperties.getProperty("com.ibm.ssl.keyStore")
					, mSSLProperties.getProperty("com.ibm.ssl.privateStore")
					, mSSLProperties.getProperty("com.ibm.ssl.keyStorePassword")));
			connectOptions.setServerURIs(new String[]{sslUrl});
			logger.debug("mqtt server will login with ssl");
		} catch (Exception e) {
			logger.debug(CommonUtils.getExceptionInfo(e));
			connectOptions.setServerURIs(new String[]{mProperties.getProperty("mqtt.uri.tcp")});
			logger.debug("mqtt server will login with tcp");
		}
		String username = mProperties.getProperty("mqtt.username", "");
		if(username.length()>1) {
			connectOptions.setUserName(username);
			connectOptions.setPassword(mProperties.getProperty("mqtt.password", "").toCharArray());
		}
		connectOptions.setConnectionTimeout(10);
		connectOptions.setKeepAliveInterval(Integer.parseInt(mProperties.getProperty("mqtt.keepalive", "60")));
		connectOptions.setAutomaticReconnect(true);
		mWillTopic = mProperties.getProperty("mqtt.willtopic","/server/bus/status")+"/"+mProperties.getProperty("mqtt.clientid", "bus_server");
		connectOptions.setWill(mWillTopic, mProperties.getProperty("mqtt.willmsg","0").getBytes(), 2, true);
		
		mqttClient = new MqttClient(mProperties.getProperty("mqtt.uri", "tcp://127.0.0.1:1883"),
				mProperties.getProperty("mqtt.clientid", "bus_server"), new MqttDefaultFilePersistence(mConfPath+"/../mqtt"));
		mqttClient.setCallback(this);
		mqttClient.setTimeToWait(10*1000);
		
		rpcRequestName = mProperties.getProperty("mqtt.rpc.request.name", "/request/");
		rpcResponseName = mProperties.getProperty("mqtt.rpc.response.name", "/response/");
		mNotifyTopicPre = mProperties.getProperty("mqtt.topic.notify", "/server/notify")+"/"+mProperties.getProperty("mqtt.clientid", "bus_server")+"/";
		
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
		if(topic.startsWith(rpcTopicPrefix)) {
			PushMqttMessage mqttMessage = new PushMqttMessage(topic, message);
			for(OnMqttConnectorListener tListener : receiveMessageListeners) {
				try {
					tListener.onMqttReceiveMessage(mqttMessage);
				} catch (Exception e) {
					logger.error(CommonUtils.getExceptionInfo(e));
				}
			}
		}
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
			rpcTopicPrefix = mProperties.getProperty("mqtt.rpctopic","/server/rpc")+"/"
					+ mProperties.getProperty("mqtt.clientid", "bus_server")
					+ rpcRequestName;
			mqttClient.subscribe(rpcTopicPrefix+"#", 0);
			
			PushMqttMessage onlineMsg = new PushMqttMessage();
			onlineMsg.setTopic(mWillTopic);
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

	private boolean loadConfig() throws IOException {
		InputStream confInputStream = null;
		try {
			File confFile = new File(mConfPath + "mqtt.properties");
			confInputStream = new FileInputStream(confFile);
			mProperties = new Properties();
			mProperties.load(confInputStream);
			confInputStream.close();
			confInputStream = null;
			
			confFile = new File(mConfPath + "ssl.properties");
			confInputStream = new FileInputStream(confFile);
			mSSLProperties = new Properties();
			mSSLProperties.load(confInputStream);
		} catch (IOException e) {
			logger.info("the mqtt conf file does not exits");
			throw e;
		} finally {
			if (confInputStream != null) {
				try {
					confInputStream.close();
				} catch (IOException e) {
				}
			}
		}
		return true;
	}
}
