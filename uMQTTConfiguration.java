package re.usto.umqtt;

/**
 * Created by gabriel on 6/5/17.
 */

public class uMQTTConfiguration {

    private String brokerIp;
    private String clientId;
    private int brokerPort;

    // Callback is called when we receive a connack from the broker
    interface OnConnectionEstablishedListener {
        void onConnectionEstablished();

    }

    private OnConnectionEstablishedListener onConnectionEstablished;

    private uMQTTConfiguration(String clientId, String brokerIp, int brokerPort) {
        this.clientId = clientId;
        this.brokerIp = brokerIp;
        this.brokerPort = brokerPort;
    }

    public static class Builder {

        private uMQTTConfiguration config;

        public Builder(String clientId, String brokerIp, int brokerPort) {
            config = new uMQTTConfiguration(clientId, brokerIp, brokerPort);
        }

        public Builder setOnConnectionEstablishedListener(OnConnectionEstablishedListener
                                                          onConnectionEstablishedListener) {
            config.setOnConnectionEstablishedListener(onConnectionEstablishedListener);
            return this;
        }

        public uMQTTConfiguration build() {
            return config;
        }
    }

    private void setOnConnectionEstablishedListener(OnConnectionEstablishedListener
                                                    onConnectionEstablishedListener) {
        this.onConnectionEstablished = onConnectionEstablishedListener;
    }

    boolean hasConnectionCallback() {
        return onConnectionEstablished != null;
    }

    void connectionEstablished() {
        onConnectionEstablished.onConnectionEstablished();
    }

    String getClientId() {
        return clientId;
    }

    String getBrokerIp() {
        return brokerIp;
    }

    int getBrokerPort() {
        return brokerPort;
    }
}
