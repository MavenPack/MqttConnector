package com.yeild.mqtt;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLSocketFactory;

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
	private static String mMqttConfName = "mqtt.properties";
	private static String mSSLConfName = "ssl.properties";
	private MqttClient mqttClient;
	private String mConfPath;
	private Properties mDefProperties = null;
	private Properties mDefSSLProperties = null;
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
			if(!loadConfig() || !loadSSLConfig()) {
				return;
			}
		} catch (IOException e) {
			lastException = e;
			return;
		}
//		mSSLProperties.setProperty("com.ibm.ssl.keyStore", mConfPath+getSSLConfValue("com.ibm.ssl.keyStore", ""));
//		mSSLProperties.setProperty("com.ibm.ssl.trustStore", mConfPath+getSSLConfValue("com.ibm.ssl.trustStore", ""));
//		if(getSSLConfValue("com.ibm.ssl.privateStore","").length()>0) {
//			mSSLProperties.setProperty("com.ibm.ssl.privateStore", mConfPath+getSSLConfValue("com.ibm.ssl.privateStore", ""));
//		}
		pushMsgQueue = new LinkedBlockingQueue<PushMqttMessage>(ConvertUtils.parseInt(getConfValue("mqtt.push.messagequeue", "1000"), -1));
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
		String sslUrl = getConfValue("mqtt.uri.ssl");
		SSLSocketFactory sslSocketFac = null;
		if(sslUrl != null) {
			try {
				try {
					sslSocketFac = MqttSSLCreator.getSSLSocktet(mConfPath+getSSLConfValue("com.ibm.ssl.trustStore")
							, mConfPath+getSSLConfValue("com.ibm.ssl.keyStore")
							,mConfPath+ getSSLConfValue("com.ibm.ssl.privateStore")
							, getSSLConfValue("com.ibm.ssl.keyStorePassword"));
				}
				catch (FileNotFoundException e) {
					logger.debug(CommonUtils.getExceptionInfo(e));
		    		logger.debug("Could not find ssl file in path " + mConfPath);
		    		logger.debug("Try to use default ssl file.");
					ClassLoader classLoader = getClass().getClassLoader();
					sslSocketFac = MqttSSLCreator.getSSLSocktet(classLoader.getResourceAsStream(getSSLConfValue("com.ibm.ssl.trustStore"))
							, classLoader.getResourceAsStream(getSSLConfValue("com.ibm.ssl.keyStore"))
							, classLoader.getResourceAsStream(getSSLConfValue("com.ibm.ssl.privateStore"))
							, getSSLConfValue("com.ibm.ssl.keyStorePassword"));
				}
			}
			catch (Exception e) {
				logger.debug(CommonUtils.getExceptionInfo(e));
			}
		}
		if(sslUrl != null && sslSocketFac != null) {
			connectOptions.setSocketFactory(sslSocketFac);
			connectOptions.setServerURIs(new String[]{sslUrl});
			logger.debug("mqtt server will login with ssl");
		} else {
			connectOptions.setServerURIs(new String[]{getConfValue("mqtt.uri.tcp")});
			logger.debug("mqtt server will login with tcp");
		}
		String username = getConfValue("mqtt.username", "");
		if(username.length()>1) {
			connectOptions.setUserName(username);
			connectOptions.setPassword(getConfValue("mqtt.password", "").toCharArray());
		}
		connectOptions.setConnectionTimeout(10);
		connectOptions.setKeepAliveInterval(Integer.parseInt(getConfValue("mqtt.keepalive", "60")));
		connectOptions.setAutomaticReconnect(true);
		mWillTopic = getConfValue("mqtt.willtopic","/server/bus/status")+"/"+getConfValue("mqtt.clientid", "bus_server");
		connectOptions.setWill(mWillTopic, getConfValue("mqtt.willmsg","0").getBytes(), 2, true);
		
		mqttClient = new MqttClient(getConfValue("mqtt.uri", "tcp://127.0.0.1:1883"),
				getConfValue("mqtt.clientid", "bus_server"), new MqttDefaultFilePersistence(mConfPath+"/../mqtt"));
		mqttClient.setCallback(this);
		mqttClient.setTimeToWait(10*1000);
		
		rpcRequestName = getConfValue("mqtt.rpc.request.name", "/request/");
		rpcResponseName = getConfValue("mqtt.rpc.response.name", "/response/");
		mNotifyTopicPre = getConfValue("mqtt.topic.notify", "/server/notify")+"/"+getConfValue("mqtt.clientid", "bus_server")+"/";
		
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
			rpcTopicPrefix = getConfValue("mqtt.rpctopic","/server/rpc")+"/"
					+ getConfValue("mqtt.clientid", "bus_server")
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
    
    private String getConfValue(String key) {
    	return getConfValue(key, null);
    }
    
	private String getConfValue(String key, String defValue) {
    	return mProperties == null ? mDefProperties.getProperty(key, defValue) : mProperties.getProperty(key, mDefProperties.getProperty(key, defValue));
    }
    
	private String getSSLConfValue(String key) {
		return getSSLConfValue(key, null);
	}
	
    private String getSSLConfValue(String key, String defValue) {
    	return mSSLProperties == null ? mDefSSLProperties.getProperty(key, defValue) : mSSLProperties.getProperty(key, mDefSSLProperties.getProperty(key, defValue));
    }

	private boolean loadConfig() throws IOException {
		InputStream confInputStream = null;
		if(mDefProperties == null) {
			confInputStream = getClass().getClassLoader().getResourceAsStream(mMqttConfName);
    		if(confInputStream == null) {
    			throw new IOException("default mqtt config file not found.");
    		}
    		mDefProperties = new Properties();
    		mDefProperties.load(confInputStream);
    		confInputStream.close();
			confInputStream = null;
    	}
		try {
			mProperties = new Properties();
			
			File confFile = new File(mConfPath + mMqttConfName);
			confInputStream = new FileInputStream(confFile);
			mProperties.load(confInputStream);
			confInputStream.close();
			confInputStream = null;
		} catch (IOException e) {
    		logger.debug("Could not find mqtt config file for path " + mConfPath + mMqttConfName);
    		logger.debug("Use default mqtt config.");
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

	private boolean loadSSLConfig() throws IOException {
		InputStream confInputStream = null;
		if(mDefSSLProperties == null) {
			confInputStream = getClass().getClassLoader().getResourceAsStream(mSSLConfName);
    		if(confInputStream == null) {
    			throw new IOException("default mqtt ssl config file not found.");
    		}
    		mDefSSLProperties = new Properties();
    		mDefSSLProperties.load(confInputStream);
    		confInputStream.close();
			confInputStream = null;
    	}
		try {
			mSSLProperties = new Properties();
			File confFile = new File(mConfPath + mSSLConfName);
			confInputStream = new FileInputStream(confFile);
			mSSLProperties.load(confInputStream);
		} catch (IOException e) {
    		logger.debug("Could not find mqtt ssl config file for path " + mConfPath + mSSLConfName);
    		logger.debug("Use default mqtt ssl config.");
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
