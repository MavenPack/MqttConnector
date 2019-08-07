package com.yeild.mqtt;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.security.cert.CertificateException;

/**
 * <pre>
 *     author : docyp
 *     e-mail : xxx@xx
 *     time   : 2019/02/27
 *     desc   :
 *     version: 1.0
 * </pre>
 */
public class AppSocketFactory extends SSLSocketFactory {
    private static final String PROTOCOL_ARRAY[];

    static {
        PROTOCOL_ARRAY = new String[]{"SSLv3", "TLSv1", "TLSv1.1", "TLSv1.2"};
    }
    private javax.net.ssl.SSLSocketFactory delegate;
    private TrustManagerFactory tmf;

    public static class SocketFactoryOptions {

        private InputStream caCrtInputStream;
        private InputStream caBksInputStream;
        private InputStream caClientP12InputStream;
        private InputStream clientBksInputStream;
        private InputStream caClientKeyInputStream;
        private String caClientP12Password;
        private String caBksPassword;
        private String clientBksPassword;

        public SocketFactoryOptions withCaInputStream(InputStream stream) {
            this.caCrtInputStream = stream;
            return this;
        }
        public SocketFactoryOptions withClientP12InputStream(InputStream certStream, String password) {
            this.caClientP12InputStream = certStream;
            this.caClientP12Password = password;
            return this;
        }

        public SocketFactoryOptions withCaBksInputStream(InputStream caBksInputStream, String caBksPassword) {
            this.caBksInputStream = caBksInputStream;
            this.caBksPassword = caBksPassword;
            return this;
        }

        public SocketFactoryOptions withClientBksInputStream(InputStream clientBksInputStream, String clientBksPassword) {
            this.clientBksInputStream = clientBksInputStream;
            this.clientBksPassword = clientBksPassword;
            return this;
        }

        public boolean hasCaCrt() {
            return caCrtInputStream != null;
        }

        public boolean hasClientP12Crt() {
            return caClientP12InputStream != null;
        }

        public InputStream getCaCrtInputStream() {
            return caCrtInputStream;
        }

        public InputStream getCaClientP12InputStream() {
            return caClientP12InputStream;
        }

        public InputStream getCaClientKeyInputStream() {
            return caClientKeyInputStream;
        }

        public String getCaClientP12Password() {
            return caClientP12Password;
        }

        public boolean hasClientP12Password() {
            return (caClientP12Password != null) && !caClientP12Password.equals("");
        }

        public InputStream getCaBksInputStream() {
            return caBksInputStream;
        }

        public InputStream getClientBksInputStream() {
            return clientBksInputStream;
        }

        public String getCaBksPassword() {
            return caBksPassword;
        }

        public String getClientBksPassword() {
            return clientBksPassword;
        }

        public boolean hasCaBks() {
            return caBksInputStream != null;
        }

        public boolean hasCaBksPassword() {
            return (caBksPassword != null) && !caBksPassword.equals("");
        }

        public boolean hasClientBks() {
            return clientBksInputStream != null;
        }

        public boolean hasClientBksPassword() {
            return (caBksPassword != null) && !caBksPassword.equals("");
        }
    }
    public AppSocketFactory() throws CertificateException, KeyStoreException, NoSuchAlgorithmException, IOException, KeyManagementException, java.security.cert.CertificateException, UnrecoverableKeyException {
        this(new SocketFactoryOptions());
    }

    public AppSocketFactory(SSLSocketFactory factory) {
        this.delegate = factory;
    }

    public AppSocketFactory(SocketFactoryOptions options) throws KeyStoreException, NoSuchAlgorithmException, IOException, KeyManagementException, java.security.cert.CertificateException, UnrecoverableKeyException {
        tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509");

        if(options.hasCaBks()){
            KeyStore trustStore = KeyStore.getInstance("JKS");
            trustStore.load(options.getCaBksInputStream()
                    , options.hasCaBksPassword() ? options.getCaBksPassword().toCharArray() : new char[0]);
            tmf.init(trustStore);
        } else if(options.hasCaCrt()) {
            KeyStore caKeyStore = KeyStore.getInstance(KeyStore.getDefaultType());
            caKeyStore.load(null, null);

            CertificateFactory caCF = CertificateFactory.getInstance("X.509");
            X509Certificate ca = (X509Certificate) caCF.generateCertificate(options.getCaCrtInputStream());
            String alias = ca.getSubjectX500Principal().getName();
            caKeyStore.setCertificateEntry(alias, ca);
            tmf.init(caKeyStore);
        } else {
            KeyStore keyStore = KeyStore.getInstance("AndroidCAStore");
            keyStore.load(null);
            tmf.init(keyStore);
        }

        if(options.hasClientBks()){
            KeyStore keyStore = KeyStore.getInstance("JKS");
            keyStore.load(options.getClientBksInputStream()
                    , options.hasClientBksPassword() ? options.getClientBksPassword().toCharArray() : new char[0]);
            kmf.init(keyStore
                    , options.hasClientBksPassword() ? options.getClientBksPassword().toCharArray() : new char[0]);
        } else if (options.hasClientP12Crt()) {
            KeyStore clientKeyStore = KeyStore.getInstance("PKCS12");
            clientKeyStore.load(options.getCaClientP12InputStream(), options.hasClientP12Password() ? options.getCaClientP12Password().toCharArray() : new char[0]);
            kmf.init(clientKeyStore, options.hasClientP12Password() ? options.getCaClientP12Password().toCharArray() : new char[0]);
        } else {
            kmf.init(null,null);
        }

        SSLContext context = SSLContext.getInstance(PROTOCOL_ARRAY[PROTOCOL_ARRAY.length-1]);
        context.init(kmf.getKeyManagers(), getTrustManagers(), new SecureRandom());
        this.delegate = context.getSocketFactory();
    }

    /**
     * 如果是SSLSocket，启用所有协议。
     */
    private static void setSupportProtocolAndCipherSuites(Socket socket) {
        if (socket instanceof SSLSocket) {
            ((SSLSocket) socket).setEnabledProtocols(PROTOCOL_ARRAY);
        }
    }

    public TrustManager[] getTrustManagers() {
        return tmf.getTrustManagers();
    }

    @Override
    public String[] getDefaultCipherSuites() {
        return this.delegate.getDefaultCipherSuites();
    }

    @Override
    public String[] getSupportedCipherSuites() {
        return this.delegate.getSupportedCipherSuites();
    }

    @Override
    public Socket createSocket() throws IOException {
        SSLSocket r = (SSLSocket)this.delegate.createSocket();
        setSupportProtocolAndCipherSuites(r);
        return r;
    }

    @Override
    public Socket createSocket(Socket s, String host, int port, boolean autoClose) throws IOException {
        SSLSocket r = (SSLSocket)this.delegate.createSocket(s, host, port, autoClose);
        setSupportProtocolAndCipherSuites(r);
        return r;
    }

    @Override
    public Socket createSocket(String host, int port) throws IOException {
        SSLSocket r = (SSLSocket)this.delegate.createSocket(host, port);
        setSupportProtocolAndCipherSuites(r);
        return r;
    }

    @Override
    public Socket createSocket(String host, int port, InetAddress localHost, int localPort) throws IOException {
        SSLSocket r = (SSLSocket)this.delegate.createSocket(host, port, localHost, localPort);
        setSupportProtocolAndCipherSuites(r);
        return r;
    }

    @Override
    public Socket createSocket(InetAddress host, int port) throws IOException {
        SSLSocket r = (SSLSocket)this.delegate.createSocket(host, port);
        setSupportProtocolAndCipherSuites(r);
        return r;
    }

    @Override
    public Socket createSocket(InetAddress address, int port, InetAddress localAddress, int localPort) throws IOException {
        SSLSocket r = (SSLSocket)this.delegate.createSocket(address, port, localAddress,localPort);
        setSupportProtocolAndCipherSuites(r);
        return r;
    }
}
