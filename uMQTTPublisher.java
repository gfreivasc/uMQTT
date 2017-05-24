package re.usto.umqtt;

import java.util.HashMap;

/**
 * Created by gabriel on 5/22/17.
 */

public abstract class uMQTTPublisher {

    protected byte qosLevel = 0b00;
    protected String topic;
    private HashMap<Short, uMQTTPublish> publishes;

    protected uMQTTPublisher(String topic, byte qosLevel) {
        this.topic = topic;
        this.qosLevel = qosLevel;
    }

    public short publish(String message) {
        uMQTTPublish publish = new uMQTTPublish(topic, message, qosLevel, this);
        uMQTTController.getInstance().addPublish(publish);
        if (publishes == null) publishes = new HashMap<>();
        publishes.put(publish.getPacketId(), publish);
        return publish.getPacketId();
    }

    protected uMQTTPublish getPublish(short packetId) {
        return publishes.get(packetId);
    }

    protected abstract void onPublishCompleted(short packetId);
}
