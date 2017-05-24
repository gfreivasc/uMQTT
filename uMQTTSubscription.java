package re.usto.umqtt;

/**
 * @author gabriel
 */

public class uMQTTSubscription {

    private String topic;
    private byte requestedQosLevel;
    private byte grantedQosLevel = -1;
    private OnReceivedPublish publishCallback;
    private short requestPacketId;

    uMQTTSubscription(String topic, byte qosLevel, OnReceivedPublish onReceivedPublish) {
        this.topic = topic;
        this.requestedQosLevel = qosLevel;
        publishCallback = onReceivedPublish;
    }

    /**
     * Defines a subscription and a callback for any received publish in that topic
     * @author gabriel
     */
    public interface OnReceivedPublish {
        void onReceivedPublish(String topic, String message);
    }

    void dispatchMessage(String message) {
        publishCallback.onReceivedPublish(topic, message);
    }

    void setRequestPacketId(short packetId) { requestPacketId = packetId; }
    void setGrantedQosLevel(byte qosLevel) { grantedQosLevel = qosLevel; }
    short getRequestPacketId() { return requestPacketId; }
    String getTopic() { return topic; }
    byte getRequestedQoSLevel() { return requestedQosLevel; }
    byte getGrantedQoSLevel() { return grantedQosLevel; }
}
