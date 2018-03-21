package com.yeild.mqtt;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import javax.net.ssl.SSLSocketFactory;

import org.apache.log4j.Logger;

import com.yeild.common.Utils.CommonUtils;

public class MqttConfig {
	Logger logger = Logger.getLogger(getClass().getSimpleName());
	private static String mMqttConfName = "mqtt.properties";
	private static String mSSLConfName = "ssl.properties";
	private Properties mProperties = null;
	private Properties mSSLProperties = null;
	private String mConfPath;
	private String mUri;
	private String mUsername;
	private String mPassword;
	private String mClientid;
	private int keepalive = 60;
	private SSLSocketFactory sslSocketFac = null;
	private String mWillTopic = null;
	private String mWillMsg = "0";
	private String rpcTopicPrefix="/server/rpc/";
	private String rpcResponseName = null;
	private String rpcRequestName = null;
	private String mNotifyTopicPre = null;
	private int maxMessageQueue = 0;
	
	public MqttConfig() {
		try {
			load("");
		} catch (IOException e) {
		}
	}
	
	public void setRpcResponseName(String rpcResponseName) {
		this.rpcResponseName = rpcResponseName;
	}
	
	public String getRpcResponseName() {
		return rpcResponseName;
	}
	
	public void setRpcRequestName(String rpcRequestName) {
		this.rpcRequestName = rpcRequestName;
	}
	
	public String getRpcRequestName() {
		return rpcRequestName;
	}
	
	public void setRpcTopicPrefix(String rpcTopicPrefix) {
		this.rpcTopicPrefix = rpcTopicPrefix;
	}
	
	public String getRpcTopicPrefix() {
		return rpcTopicPrefix;
	}
	
	public void setNotifyTopicPre(String mNotifyTopicPre) {
		this.mNotifyTopicPre = mNotifyTopicPre;
	}
	
	public String getNotifyTopicPre() {
		return mNotifyTopicPre;
	}
	
	public void setUri(String mUri) {
		this.mUri = mUri;
	}
	
	public String getUri() {
		return mUri;
	}
	
	public void setUsername(String mUsername) {
		this.mUsername = mUsername;
	}
	
	public String getUsername() {
		return mUsername;
	}
	
	public void setPassword(String mPassword) {
		this.mPassword = mPassword;
	}
	
	public String getPassword() {
		return mPassword;
	}
	
	public void setSslSocketFac(SSLSocketFactory sslSocketFac) {
		this.sslSocketFac = sslSocketFac;
	}
	
	public SSLSocketFactory getSslSocketFac() {
		return sslSocketFac;
	}
	
	public void setKeepalive(int keepalive) {
		this.keepalive = keepalive;
	}
	
	public int getKeepalive() {
		return keepalive;
	}
	
	public void setWillTopic(String mWillTopic) {
		this.mWillTopic = mWillTopic;
	}
	
	public String getWillTopic() {
		return mWillTopic;
	}
	
	public void setmWillMsg(String mWillMsg) {
		this.mWillMsg = mWillMsg;
	}
	
	public String getWillMsg() {
		return mWillMsg;
	}
	
	public void setClientid(String mClientid) {
		this.mClientid = mClientid;
	}
	
	public String getClientid() {
		return mClientid;
	}
	
	public void setMaxMessageQueue(int maxMessageQueue) {
		this.maxMessageQueue = maxMessageQueue;
	}
    
    public int getMaxMessageQueue() {
		return maxMessageQueue;
	}
	
	public void load(String path) throws IOException {
		mConfPath = path;
		if(!loadConfig() || !loadSSLConfig()) {
			return;
		}
		maxMessageQueue = Integer.parseInt(getConfValue("mqtt.push.messagequeue", "1000"));
		mUri = getConfValue("mqtt.uri.ssl");
		if(mUri != null && mUri.length() > 0) {
			try {
//				try {
					sslSocketFac = MqttSSLCreator.getSSLSocktet(mConfPath+getSSLConfValue("com.ibm.ssl.trustStore")
							, mConfPath+getSSLConfValue("com.ibm.ssl.keyStore")
							,mConfPath+ getSSLConfValue("com.ibm.ssl.privateStore")
							, getSSLConfValue("com.ibm.ssl.keyStorePassword"));
//				}
//				catch (FileNotFoundException e) {
//					logger.debug(CommonUtils.getExceptionInfo(e));
//		    		logger.debug("Could not find ssl file in path " + mConfPath);
//		    		logger.debug("Try to use default ssl file.");
//					ClassLoader classLoader = getClass().getClassLoader();
//					sslSocketFac = MqttSSLCreator.getSSLSocktet(classLoader.getResourceAsStream(getSSLConfValue("com.ibm.ssl.trustStore"))
//							, classLoader.getResourceAsStream(getSSLConfValue("com.ibm.ssl.keyStore"))
//							, classLoader.getResourceAsStream(getSSLConfValue("com.ibm.ssl.privateStore"))
//							, getSSLConfValue("com.ibm.ssl.keyStorePassword"));
//				}
			}
			catch (Exception e) {
				logger.error(CommonUtils.getExceptionInfo(e));
			}
		}
		if(mUri == null || sslSocketFac == null) {
			mUri = getConfValue("mqtt.uri.tcp");
		}
		if(isEmpty(mUri)) {
			throw new IOException("uri not found");
		}
		mUsername = getConfValue("mqtt.username", "");
		mPassword = getConfValue("mqtt.password", "");
		mClientid = getConfValue("mqtt.clientid", "");
		if(isEmpty(mUsername) && isEmpty(mClientid)) {
			throw new IOException("No username and clientid");
		}
		keepalive = Integer.parseInt(getConfValue("mqtt.keepalive", "60"));
		mWillTopic = getConfValue("mqtt.willtopic","/server/bus/status")+"/"+getConfValue("mqtt.clientid", "bus_server");
		mWillMsg = getConfValue("mqtt.willmsg","0");
		
		rpcRequestName = getConfValue("mqtt.rpc.request.name", "/request/");
		rpcResponseName = getConfValue("mqtt.rpc.response.name", "/response/");
		mNotifyTopicPre = getConfValue("mqtt.topic.notify", "/server/notify")+"/"+getConfValue("mqtt.clientid", "bus_server")+"/";
		rpcTopicPrefix = getConfValue("mqtt.rpctopic","/server/rpc")+"/"
				+ getConfValue("mqtt.clientid", "bus_server")
				+ rpcRequestName;
	}
	
	private boolean isEmpty(String val) {
		return val == null || val.length() < 1;
	}
    
    private String getConfValue(String key) {
    	return getConfValue(key, null);
    }
    
	private String getConfValue(String key, String defValue) {
    	return mProperties.getProperty(key, defValue);
    }
    
	private String getSSLConfValue(String key) {
		return getSSLConfValue(key, null);
	}
	
    private String getSSLConfValue(String key, String defValue) {
    	return mSSLProperties.getProperty(key, defValue);
    }

	private boolean loadConfig() throws IOException {
		InputStream confInputStream = null;
		if(mProperties == null) {
			confInputStream = getClass().getClassLoader().getResourceAsStream(mMqttConfName);
			if(confInputStream == null) {
				throw new IOException("default mqtt config file not found.");
			}
			Properties defProperties = new Properties();
			defProperties.load(confInputStream);
			confInputStream.close();
			confInputStream = null;
			mProperties = new Properties(defProperties);
		}
		try {
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
		if(mSSLProperties == null) {
			confInputStream = getClass().getClassLoader().getResourceAsStream(mSSLConfName);
			if(confInputStream == null) {
				throw new IOException("default mqtt ssl config file not found.");
			}
			Properties defSSLProperties = new Properties();
			defSSLProperties.load(confInputStream);
			confInputStream.close();
			confInputStream = null;
			mSSLProperties = new Properties(defSSLProperties);
		}
		try {
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
