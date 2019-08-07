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
	protected Logger logger = Logger.getLogger(getClass().getSimpleName());
	private static String mMqttConfName = "mqtt.properties";
	private static String mSSLConfName = "ssl.properties";
	protected Properties mProperties = null;
	protected Properties mSSLProperties = null;
	protected String mConfPath;
	protected String mUri;
	protected String mUsername;
	protected String mPassword;
	protected String mClientid;
	protected int keepalive = 60;
	protected SSLSocketFactory sslSocketFac = null;
	protected String mWillTopic = null;
	protected String mWillMsg = "0";
	protected String rpcTopicPrefix="/server/rpc/";
	protected String rpcResponseName = null;
	protected String rpcRequestName = null;
	protected String mNotifyTopicPre = null;
	protected int maxMessageQueue = 0;
	
	public MqttConfig() {
		init();
	}
	
	private void init() {
		try {
			load(null);
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
	
	public void createSslSocketFac(String caPath, String crtPath, String keyPath, String password) throws Exception {
		sslSocketFac = MqttSSLCreator.getSSLSocktet(caPath, crtPath, keyPath, password);
	}
	
    /**
     * load config from file
     * @param path
     * @throws IOException
     */
	public void load(String path) throws IOException {
		mConfPath = path;
		if(!loadConfig() || !loadSSLConfig()) {
			return;
		}
		maxMessageQueue = Integer.parseInt(getConfValue("mqtt.push.messagequeue", "1000"));
		mUri = getConfValue("mqtt.uri.ssl");
		if(!isEmpty(mUri) && !isEmpty(mConfPath)) {
			try {
				createSslSocketFac(mConfPath+getSSLConfValue("com.ibm.ssl.trustStore")
						, mConfPath+getSSLConfValue("com.ibm.ssl.keyStore")
						,mConfPath+ getSSLConfValue("com.ibm.ssl.privateStore")
						, getSSLConfValue("com.ibm.ssl.keyStorePassword"));
			}
			catch (Exception e) {
				logger.error(CommonUtils.getExceptionInfo(e));
			}
		}
		if(isEmpty(mUri) || sslSocketFac == null) {
			mUri = getConfValue("mqtt.uri.tcp");
		}
		String rpcpre = getConfValue("mqtt.rpctopic","/rpc");
		mUsername = getConfValue("mqtt.username", "");
		mPassword = getConfValue("mqtt.password", "");
		mClientid = getConfValue("mqtt.clientid", "");
		if(isEmpty(mClientid)) {
			throw new IOException("the clientid not configured");
		}
		keepalive = Integer.parseInt(getConfValue("mqtt.keepalive", "60"));
		mWillTopic = rpcpre +"/"+mClientid+"/"+getConfValue("mqtt.willtopic", "status");
		mWillMsg = getConfValue("mqtt.willmsg","0");
		
		rpcRequestName = getConfValue("mqtt.rpc.request.name", "/req/");
		rpcResponseName = getConfValue("mqtt.rpc.response.name", "/resp/");
		mNotifyTopicPre = rpcpre +"/"+mClientid+"/"+getConfValue("mqtt.topic.notify", "notify");
		rpcTopicPrefix = rpcpre+"/" + mClientid + rpcRequestName;
	}
	
	public boolean checkValid() throws Exception {
		if(isEmpty(mUri)) {
			throw new Exception("uri not found");
		}
		if(isEmpty(mUsername) && isEmpty(mClientid)) {
			throw new Exception("No username and clientid");
		}
		return true;
	}
	
	protected boolean isEmpty(String val) {
		return val == null || val.length() < 1;
	}
    
    public String getConfValue(String key) {
    	return getConfValue(key, null);
    }
    
    public String getConfValue(String key, String defValue) {
    	return mProperties.getProperty(key, defValue);
    }
    
    public String getSSLConfValue(String key) {
		return getSSLConfValue(key, null);
	}
	
    public String getSSLConfValue(String key, String defValue) {
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
		if(!isEmpty(mConfPath)) {
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
		if(!isEmpty(mConfPath)) {
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
		}
		return true;
	}
}
