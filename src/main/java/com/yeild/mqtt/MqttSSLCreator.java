package com.yeild.mqtt;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.security.KeyFactory;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.interfaces.RSAPrivateKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManagerFactory;

import org.apache.commons.codec.binary.Base64;

public class MqttSSLCreator {
	public static SSLSocketFactory getSSLSocktet(String caPath, String crtPath, String keyPath, String password)
			throws Exception {
//		return new AppSocketFactory(new AppSocketFactory.SocketFactoryOptions()
//				.withCaBksInputStream(new FileInputStream(caPath), "yeildserver")
//				.withClientBksInputStream(new FileInputStream(crtPath), "yeildclient"));
		return getSSLSocktet(new FileInputStream(caPath), new FileInputStream(crtPath), new FileInputStream(keyPath), password);
	}
	
	public static SSLSocketFactory getSSLSocktet(InputStream caIn, InputStream crtIn, InputStream keyPath, String password)
			throws CertificateException, KeyStoreException, NoSuchAlgorithmException, IOException, UnrecoverableKeyException, KeyManagementException, InvalidKeySpecException {
		// CA certificate is used to authenticate server
		CertificateFactory cAf = CertificateFactory.getInstance("X.509");
		X509Certificate ca = (X509Certificate) cAf.generateCertificate(caIn);
		KeyStore caKs = KeyStore.getInstance("JKS");
		caKs.load(null, null);
		caKs.setCertificateEntry("ca-certificate", ca);
		TrustManagerFactory tmf = TrustManagerFactory.getInstance("SunX509");
		tmf.init(caKs);

		CertificateFactory cf = CertificateFactory.getInstance("X.509");
		X509Certificate caCert = (X509Certificate) cf.generateCertificate(crtIn);

		crtIn.close();
		KeyStore ks = KeyStore.getInstance("PKCS12");
		ks.load(null, null);
		ks.setCertificateEntry("certificate", caCert);
		ks.setKeyEntry("private-key", getPrivateKey(keyPath), password.toCharArray(),
				new java.security.cert.Certificate[] { caCert });
		KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509");
		kmf.init(ks, password.toCharArray());

		SSLContext context = SSLContext.getInstance("TLSv1.2");

		context.init(kmf.getKeyManagers(), tmf.getTrustManagers(), new SecureRandom());

		return context.getSocketFactory();
	}

	public static PrivateKey getPrivateKey(InputStream path) throws IOException, NoSuchAlgorithmException, InvalidKeySpecException {
		org.apache.commons.codec.binary.Base64 base64 = new Base64();
		byte[] buffer = base64.decode(getPem(path));
		PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(buffer);
		KeyFactory keyFactory = KeyFactory.getInstance("RSA");
		return (RSAPrivateKey) keyFactory.generatePrivate(keySpec);

	}

	private static String getPem(InputStream fin) throws IOException {
		BufferedReader br = new BufferedReader(new InputStreamReader(fin));
		String readLine = null;
		StringBuilder sb = new StringBuilder();
		while ((readLine = br.readLine()) != null) {
			if (readLine.length() > 0 && readLine.charAt(0) == '-') {
				continue;
			} else {
				sb.append(readLine);
				sb.append('\r');
			}
		}
		fin.close();
		return sb.toString();
	}
}
