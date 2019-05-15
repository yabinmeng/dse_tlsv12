package com.example;

import com.datastax.driver.core.RemoteEndpointAwareJdkSSLOptions;
import com.datastax.driver.core.RemoteEndpointAwareNettySSLOptions;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.SSLOptions;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.datastax.driver.dse.DseCluster;
import com.datastax.driver.dse.DseSession;
import io.netty.handler.ssl.SslContextBuilder;
import org.apache.commons.io.FileUtils;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import java.io.File;

import java.io.InputStream;
import java.security.KeyStore;
import java.util.HashSet;

public class dse_tlsv12 {

    private static SSLOptions getRemoteNettySSLOptions() {
        SSLOptions sslOptions = null;

        InputStream is = null;

        try {
            is = FileUtils.openInputStream(new File("<client_truststore_file_abs_path>"));
            char[] pwd = "cassandra".toCharArray();

            KeyStore ks = KeyStore.getInstance("JKS");
            ks.load(is, pwd);

            TrustManagerFactory tmf =
                TrustManagerFactory.getInstance("SunX509");
                //TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(ks);

            TrustManager[] tm = tmf.getTrustManagers();

            SslContextBuilder builder = SslContextBuilder.forClient().trustManager(tmf);
            // NOTE: ERROR with java.lang.IllegalArgumentException: TLS
            //       has to be TLSv1, TLSv1.1, TLSv1.2
            //builder.protocols("TLS");
            // NOTE: can be multiples. But,
            //       1) "TLSv1.2" by itself works fine with both DSE nodes
            //       2) "TLSv1" and "TLSv1.1" do NOT work with TLSv1.2 specific DSE node
            //builder.protocols("TLSv1", "TLSv1.1", "TLSv1.2");
            builder.protocols("TLSv1.2");

            HashSet<String> cipherSets = new HashSet<String>();
            cipherSets.add("TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384");
            builder.ciphers(cipherSets);
            
            sslOptions = new RemoteEndpointAwareNettySSLOptions(builder.build());

        } catch (Exception e) {
            e.printStackTrace();
        }

        return sslOptions;
    }


    private static SSLOptions getRemoteJDKSSLOptions() {
        SSLOptions sslOptions = null;

        InputStream is = null;

        try {
            is = FileUtils.openInputStream(new File("<client_truststore_file_abs_path>"));
            char[] pwd = "cassandra".toCharArray();

            KeyStore ks = KeyStore.getInstance("JKS");
            ks.load(is, pwd);

            TrustManagerFactory tmf =
                TrustManagerFactory.getInstance("SunX509");
                //TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(ks);

            TrustManager[] tm = tmf.getTrustManagers();

            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(null, tm, null);

            sslOptions = RemoteEndpointAwareJdkSSLOptions.builder()
                .withSSLContext(sslContext)
                .build();

        } catch (Exception e) {
            e.printStackTrace();
        }

        return sslOptions;
    }

    public static void main( String[] args ) {

        DseCluster dsecluster = null;

        try {
            DseCluster.Builder dseclusterBuilder =
                DseCluster.builder()
                    .addContactPoints("<DSE_Node_IP_with_explicit_TLSv1.2_cipher_suites>")      // explicit TLS v1.2
                    //.addContactPoints("<DSE_Node_IP_with_default_cipher_suites>")             // default TLS
                    .withLoadBalancingPolicy(
                        new TokenAwarePolicy(
                            DCAwareRoundRobinPolicy.builder()
                                .withLocalDc("DC1")
                                .build()));

            // TEST for using Netty SSL
            dseclusterBuilder = dseclusterBuilder.withSSL(getRemoteNettySSLOptions());
            
            // TEST for using JSSE SSL
            //dseclusterBuilder = dseclusterBuilder.withSSL(getRemoteJDKSSLOptions());

            dsecluster = dseclusterBuilder.build();

            DseSession session = dsecluster.connect();

            Row row = session.execute("select release_version from system.local").one();
            System.out.println(row.getString("release_version"));
        }
        finally {
            if (dsecluster != null) dsecluster.close();
    }

    }
}
