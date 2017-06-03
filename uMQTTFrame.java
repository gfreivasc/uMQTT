package re.usto.umqtt;

import android.support.annotation.IntDef;
import android.support.annotation.IntRange;

import java.io.UnsupportedEncodingException;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import timber.log.Timber;

/**
 * <p> This class represents an MQTT frame as it's built.
 * For more information, read MQTT documentation.
 *
 * @author gabriel
 */

public class uMQTTFrame {

    private byte type;
    private byte fixedHeader = 0b0;
    private byte[] variableHeader;
    private byte[] payload;
    private short packetId;

    private byte[] packet = null;

    private static final String PROTOCOL = "MQTT";
    private static final byte MQTT_VERSION = 0b100;
    private static final int CONNECT_VARIABLE_HEADER_BASE_SIZE = 6;
    private static AtomicInteger packetSequence = new AtomicInteger(1);

    @Retention(RetentionPolicy.SOURCE)
    @IntDef({
            MQ_RESERVED_BOT,
            MQ_CONNECT,
            MQ_CONNACK,
            MQ_PUBLISH,
            MQ_PUBACK,
            MQ_PUBREC,
            MQ_PUBREL,
            MQ_PUBCOMP,
            MQ_SUBSCRIBE,
            MQ_SUBACK,
            MQ_UNSUBSCRIBE,
            MQ_UNSUBACK,
            MQ_PINGREQ,
            MQ_PINGRESP,
            MQ_DISCONNECT,
            MQ_RESERVED_TOP
    })
    public @interface MQPacketType { }

    public static final byte MQ_RESERVED_BOT = 0,
            MQ_CONNECT = 1,
            MQ_CONNACK = 2,
            MQ_PUBLISH = 3,
            MQ_PUBACK = 4,
            MQ_PUBREC = 5,
            MQ_PUBREL = 6,
            MQ_PUBCOMP = 7,
            MQ_SUBSCRIBE = 8,
            MQ_SUBACK = 9,
            MQ_UNSUBSCRIBE = 10,
            MQ_UNSUBACK = 11,
            MQ_PINGREQ = 12,
            MQ_PINGRESP = 13,
            MQ_DISCONNECT = 14,
            MQ_RESERVED_TOP = 15;

    private uMQTTFrame() { }

    public static class Builder {

        uMQTTFrame frame;
        boolean mQosSet = false;

        Builder(@MQPacketType int packetType) {
            frame = new uMQTTFrame();
            frame.type = (byte) packetType;
            frame.fixedHeader = (byte) (packetType << 4);
        }

        public Builder setPacketId(short packetId) {
            frame.variableHeader = new byte[2];
            frame.variableHeader[0] = (byte)((packetId >> 8) & 0xff);
            frame.variableHeader[1] = (byte)(packetId & 0xff);
            return this;
        }

        public uMQTTFrame build() throws BrokenMQTTFrameException {
            return frame;
        }
    }

    public static class PublishBuilder extends Builder {

        private boolean mPayloadSet = false;
        private byte mQoSLevel;
        private byte[] mTopic = null;

        public PublishBuilder() {
            super(MQ_PUBLISH);
        }

        /**
         * PUBLISH packets specific data. Sets the topic the frame will be published to.
         * @param topic the MQTT topic
         */
        public PublishBuilder setTopic(String topic) {
            mTopic = frame.encodeString(topic);
            return this;
        }

        /**
         * PUBLISH packets specific data. The payload to be published.
         * @param payload the string corresponding to the payload
         */
        public PublishBuilder setPayload(String payload) {
            return setPayload(payload.getBytes());
        }

        /**
         * PUBLISH packets specific data. The payload to be published.
         * @param payload the UTF encoded byte corresponding to the payload
         */
        public PublishBuilder setPayload(byte[] payload) {
            frame.payload = payload;
            mPayloadSet = true;
            return this;
        }

        public PublishBuilder setRetain() {
            frame.fixedHeader |= 1;
            return this;
        }

        public PublishBuilder setQosLevel(@IntRange(from=0b00, to=0b10) int level) {
            frame.fixedHeader |= level << 1;
            mQoSLevel = (byte) level;
            mQosSet = true;
            return this;
        }

        public PublishBuilder setDup() {
            frame.fixedHeader |= 1 << 3;
            return this;
        }

        @Override
        public uMQTTFrame build() throws BrokenMQTTFrameException {
            if (mTopic == null || !mPayloadSet) {
                throw new BrokenMQTTFrameException(
                        "Packet is malformed. Did you forger information?");
            }
            if (!mQosSet) {
                Timber.w("QoS not set. Setting default (QoS 0).");
            }

            if (mQoSLevel > 0) {
                frame.variableHeader = new byte[mTopic.length + 2];
                for (int i = 0; i < mTopic.length; ++i) {
                    frame.variableHeader[i] = mTopic[i];

                }
                frame.setPacketId();
                frame.variableHeader[frame.variableHeader.length - 2] =
                        (byte)((frame.getPacketId() >> 8) & 0xff);
                frame.variableHeader[frame.variableHeader.length - 1] =
                        (byte)(frame.getPacketId() & 0xff);
            }
            else {
                frame.setPacketId();
                frame.variableHeader = mTopic;
            }

            return frame;
        }
    }

    public static class SubscribeBuilder extends Builder {

        private String[] topics;
        private byte[] qosLevels;
        private boolean unsubscribe = false;

        public SubscribeBuilder() {
            super(MQ_SUBSCRIBE);
        }

        /**
         * SUBSCRIBE specific data. The topics to addSubscription to.
         * @param topics The string corresponding to the list of topics
         */
        public SubscribeBuilder setTopics(String[] topics) {
            this.topics = topics;
            return this;
        }

        public SubscribeBuilder setQoS(byte[] qosLevels) {
            this.qosLevels = qosLevels;
            return this;
        }

        public SubscribeBuilder setDup() {
            frame.fixedHeader |= 1 << 3;
            return this;
        }

        public SubscribeBuilder setUnsubscribe() {
            frame.fixedHeader &= 0xf;
            frame.fixedHeader |= (MQ_UNSUBSCRIBE << 4) & 0xf0;
            unsubscribe = true;
            return this;
        }

        @Override
        public uMQTTFrame build() throws BrokenMQTTFrameException {
            if (topics == null || qosLevels == null || topics.length != qosLevels.length) {
                throw new BrokenMQTTFrameException(
                        "Packet is malformed. Did you forget information?");
            }

            // Set QoS to 1 (mandatory)
            frame.fixedHeader |= 0b01 << 1;

            ArrayList<Byte> buildPayload = new ArrayList<>();
            for (int i = 0; i < topics.length; i++) {
                byte[] topic = frame.encodeString(topics[i]);
                for (byte _byte : topic) {
                    buildPayload.add(_byte);
                }
                if (!unsubscribe) buildPayload.add(qosLevels[i]);
            }

            frame.payload = new byte[buildPayload.size()];
            for (int i = 0; i < buildPayload.size(); ++i) {
                frame.payload[i] = buildPayload.get(i);
            }

            frame.setPacketId();
            frame.variableHeader = new byte[] {
                    (byte)((frame.getPacketId() >> 8) & 0xff),
                    (byte)(frame.getPacketId() & 0xff)
            };
            return frame;
        }
    }

    public static class ConnectBuilder extends Builder {

        private int i = 0;
        private static final short DEFAULT_KEEP_ALIVE = 180;
        private boolean mKeepAliveSet = false;


        private byte[] clientId;
        private byte[] willTopic;
        private byte[] willMessage;
        private byte[] username;
        private byte[] password;

        public ConnectBuilder() {
            super(MQ_CONNECT);

            short protocolNameSize = (short) PROTOCOL.length();
            byte[] encodedProtocolName = frame.encodeString(PROTOCOL);
            frame.variableHeader = new byte[protocolNameSize + CONNECT_VARIABLE_HEADER_BASE_SIZE];
            for (byte _byte : encodedProtocolName) {
                frame.variableHeader[i++] = _byte;
            }
            frame.variableHeader[i++] = MQTT_VERSION;
        }

        public ConnectBuilder setUsernameFlag() {
            frame.variableHeader[i] |= (1 << 7);
            return this;
        }

        public ConnectBuilder setPasswordFlag() {
            frame.variableHeader[i] |= (1 << 6);
            return this;
        }

        public ConnectBuilder setWillRetain() {
            frame.variableHeader[i] |= (1 << 5);
            return this;
        }

        public ConnectBuilder setWillQoS(@IntRange(from = 0b00, to = 0b10) int qosLevel) {
            frame.variableHeader[i] |= (qosLevel << 3);
            return this;
        }

        public ConnectBuilder setWillFlag() {
            frame.variableHeader[i] |= (1 << 2);
            return this;
        }

        public ConnectBuilder setCleanSession() {
            frame.variableHeader[i] |= (1 << 1);
            return this;
        }

        public ConnectBuilder setKeepAlive(short keepAlive) {
            frame.variableHeader[i + 1] = (byte)((keepAlive >> 8) & 0xff);
            frame.variableHeader[i + 2] = (byte)(keepAlive & 0xff);
            return this;
        }

        public ConnectBuilder setClientId(String clientId) {
            this.clientId = frame.encodeString(clientId);
            return this;
        }

        public ConnectBuilder setWillTopic(String willTopic) {
            this.willTopic = frame.encodeString(willTopic);
            return this;
        }

        public ConnectBuilder setWillMessage(String willMessage) {
            this.willMessage = frame.encodeString(willMessage);
            return this;
        }

        public ConnectBuilder setUsername(String username) {
            this.username = frame.encodeString(username);
            return this;
        }

        public ConnectBuilder setPassword(String password) {
            this.password = frame.encodeString(password);
            return this;
        }

        @Override
        public uMQTTFrame build() throws BrokenMQTTFrameException {
            if (clientId == null)
                throw new BrokenMQTTFrameException("Connect packet must have a client id");

            if (!mKeepAliveSet) setKeepAlive(DEFAULT_KEEP_ALIVE);

            ArrayList<Byte> payload = new ArrayList<>();
            for (byte _byte : clientId) {
                payload.add(_byte);
            }

            for (byte _byte : willTopic != null ? willTopic : new byte[]{}) {
                payload.add(_byte);
            }

            for (byte _byte : willMessage != null ? willMessage : new byte[]{}) {
                payload.add(_byte);
            }

            for (byte _byte : username != null ? username : new byte[]{}) {
                payload.add(_byte);
            }

            for (byte _byte : password != null ? password : new byte[]{}) {
                payload.add(_byte);
            }

            frame.payload = new byte[payload.size()];
            for (int i = 0; i < payload.size(); ++i) {
                frame.payload[i] = payload.get(i);
            }
            return super.build();
        }
    }

    /**
     * Gets string entry and encodes to byte through UTF to MQTT frame.
     * @param s string to be encoded.
     * @return Array of bytes corresponding to encoded string.
     */
    private byte[] encodeString(String s) {
        byte[] encodedString = new byte[s.length() + 2];
        encodedString[0] = (byte)((s.length() >> 8) & 0xff);
        encodedString[1] = (byte)(s.length() & 0xff);
        try {
            byte[] string = s.getBytes("UTF-8");
            int j = 2;
            for (byte _byte : string) {
                encodedString[j++] = _byte;
            }
        }
        catch (UnsupportedEncodingException e) {
            throw new UnsupportedOperationException(
                    "UTF-8 support not found. Needed for MQTT strings");
        }

        return encodedString;
    }

    private byte[] encodedRemainingSize() {
        int size = variableHeader != null ? variableHeader.length : 0;
        size += payload != null ? payload.length : 0;
        ArrayList<Byte> bytes = new ArrayList<>();

        do {
            byte digit = (byte)(size % 0x80);
            size /= 0x80;
            if (size > 0) {
                digit |= 0x80;
            }
            bytes.add(digit);
        } while (size > 0);
        byte[] encodedSize = new byte[bytes.size()];
        int i = 0;
        for (Byte digit : bytes) encodedSize[i++] = digit;
        return encodedSize;
    }

    private void setPacketId() {
        if (packetSequence.get() >= Short.MAX_VALUE)
            packetSequence.set(packetId = 1);

        packetId = (short) packetSequence.getAndIncrement();
    }

    public short getPacketId() { return packetId; }

    public byte[] getPacket() {
        if ((this.packet != null) && ((this.packet[0] != 0x00) && (this.packet[1] != 0x00))) {
            return this.packet;
        }

        ArrayList<Byte> packet = new ArrayList<>();
        packet.add(fixedHeader);
        for(byte _byte : encodedRemainingSize()) {
            packet.add(_byte);
        }

        if (variableHeader != null) {
            for (byte _byte : variableHeader) {
                packet.add(_byte);
            }
        }

        if (payload != null) {
            for (byte _byte : payload) {
                packet.add(_byte);
            }
        }

        this.packet = new byte[packet.size()];
        for (int i = 0; i < packet.size(); ++i) {
            this.packet[i] = packet.get(i);
        }

        return this.packet;
    }

    @MQPacketType
    static int readPacketType(byte firstByte) {
        return (firstByte >> 4);
    }

    static int fetchBytes(byte msb, byte lsb) {
        return ((msb << 8) + lsb) & 0xffff;
    }
}
