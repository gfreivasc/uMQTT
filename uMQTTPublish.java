package re.usto.maluhia.umqtt;

import android.support.annotation.IntDef;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

import timber.log.Timber;

/**
 * Created by gabriel on 5/16/17.
 */

public class uMQTTPublish {

    private String topic;
    private String message;
    private byte qosLevel;
    private uMQTTFrame frame;
    private byte[] packet = null;
    private short packetId = -1;
    private boolean inbound = false;
    private uMQTTPublisher publisher;

    static final int
            PUB_PUBLISHING = 0,
            PUB_PUBLISHED = 1,
            PUB_RECEIVED = 2,
            PUB_RELEASED = 3,
            PUB_COMPLETED = 4;

    @Retention(RetentionPolicy.SOURCE)
    @IntDef({PUB_PUBLISHING, PUB_PUBLISHED, PUB_RECEIVED, PUB_RELEASED, PUB_COMPLETED})
    @interface MQPubStatus {
    }

    private @MQPubStatus
    int pubState = PUB_PUBLISHING;

    uMQTTPublish(String topic, String message, byte qosLevel, uMQTTPublisher publisher) {
        this.topic = topic;
        this.message = message;
        this.qosLevel = qosLevel;

        try {
            frame = new uMQTTFrame.PublishBuilder()
                    .setTopic(topic)
                    .setPayload(message)
                    .setQosLevel(qosLevel)
                    .build();
        } catch (BrokenMQTTFrameException e) {
            Timber.e(e, "Packet missing information.");
        }

        this.publisher = publisher;
        pubState = PUB_PUBLISHING;
    }

    uMQTTPublish(byte[] packet) {
        if (((packet[0] >> 4) & 0xf) != uMQTTFrame.MQ_PUBLISH)
            throw new IllegalStateException("Not a PUBLISH packet");

        this.packet = packet;

        // Get QoS level
        this.qosLevel = (byte)((packet[0] >> 1) & 0b11);

        // Fetch packet full size (remaining + 2)
        int fullSize = 0;
        int i = 1;
        do {
            fullSize *= 0x80;
            fullSize += packet[i] & 0x7f;
            i++;
        } while ((packet[i] & 0x80) != 0);
        fullSize += 2;

        // Fetch topic. First, get topic size
        StringBuilder builder = new StringBuilder();
        int currentSize = packet[i] * 0xff + packet[i + 1];
        i += 2;
        for (int j = 0; j <  currentSize; ++j) {
            builder.append((char) packet[i + j]);
        }
        this.topic = builder.toString();

        // Packet ID
        i += this.topic.length();
        this.packetId = (short)uMQTTFrame.fetchBytes(packet[i], packet[i + 1]);
        i += 2;

        // And now the payload
        builder = new StringBuilder();
        for (; i < fullSize; ++i) {
            builder.append((char) packet[i]);
        }
        this.message = builder.toString();

        // This must be an incoming packet, and thus it's state is published
        this.inbound = true;
        this.pubState = PUB_PUBLISHED;
    }

    void transactionAdvance() {
        switch (pubState) {
            case PUB_PUBLISHING:
                if (qosLevel == 0) {
                    pubState = PUB_COMPLETED;
                    publisher.onPublishCompleted(packetId);
                } else pubState = PUB_PUBLISHED;
                break;
            case PUB_PUBLISHED:
                if (qosLevel == 1) {
                    pubState = PUB_COMPLETED;
                    if (!inbound) publisher.onPublishCompleted(packetId);
                    else uMQTTController.getInstance().publishCallback(topic, message);
                } else pubState = PUB_RECEIVED;
                break;
            case PUB_RECEIVED:
            case PUB_RELEASED:
                pubState = PUB_COMPLETED;
                if (!inbound) publisher.onPublishCompleted(packetId);
                else uMQTTController.getInstance().publishCallback(topic, message);
                break;
            case PUB_COMPLETED:
                throw new IllegalStateException("Publish already completed");
        }
    }

    String getTopic() {
        return topic;
    }

    public String getMessage() {
        return message;
    }

    byte getQosLevel() {
        return qosLevel;
    }

    short getPacketId() {
        if (packetId <= 0) packetId = frame.getPacketId();
        return packetId;
    }

    byte[] getPacket() {
        if (packet == null) packet = frame.getPacket();
        return packet;
    }

    int getState() {
        return pubState;
    }

    boolean publishCompleted() {
        return pubState == PUB_COMPLETED;
    }
}
