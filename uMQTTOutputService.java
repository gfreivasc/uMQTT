package re.usto.umqtt;

import android.app.IntentService;
import android.content.Intent;
import android.support.annotation.Nullable;

import java.io.IOException;
import java.net.Socket;

import timber.log.Timber;

/**
 * @author gabriel
 */

public class uMQTTOutputService extends IntentService {

    private uMQTTController mController = uMQTTController.getInstance();

    public uMQTTOutputService() {
        super(uMQTTOutputService.class.getSimpleName());
    }

    @Override
    protected void onHandleIntent(@Nullable Intent intent) {
        if (intent == null || intent.getAction() == null) {
            Timber.w("Invalid intent received");
            return;
        }

        switch (intent.getAction()){
            case uMQTTController.ACTION_OPEN_MQTT:
                if (!intent.hasExtra(uMQTTController.EXTRA_SERVER_ADDRESS)
                        || !intent.hasExtra(uMQTTController.EXTRA_SERVER_PORT))
                    throw new UnsupportedOperationException("Connection information missing");
                openSocket(intent.getStringExtra(uMQTTController.EXTRA_SERVER_ADDRESS),
                        intent.getIntExtra(uMQTTController.EXTRA_SERVER_PORT, 1883));
                break;
            case uMQTTController.ACTION_CONNECT:
                if (!intent.hasExtra(uMQTTController.EXTRA_CLIENT_ID))
                    throw new UnsupportedOperationException("Missing client ID");
                connect(intent.getStringExtra(uMQTTController.EXTRA_CLIENT_ID));
                break;
            case uMQTTController.ACTION_SUBSCRIBE:
                if ((!intent.hasExtra(uMQTTController.EXTRA_TOPIC)
                        || !intent.hasExtra(uMQTTController.EXTRA_TOPIC_QOS))
                        && (!intent.hasExtra(uMQTTController.EXTRA_TOPICS)
                        || !intent.hasExtra(uMQTTController.EXTRA_TOPICS_QOS)))
                    throw new UnsupportedOperationException("Missing subscription info");

                if (intent.hasExtra(uMQTTController.EXTRA_TOPIC))
                    subscribe(intent.getStringExtra(uMQTTController.EXTRA_TOPIC),
                            intent.getByteExtra(uMQTTController.EXTRA_TOPIC_QOS, (byte) 0b00),
                            false);
                else
                    subscribe(intent.getStringArrayExtra(uMQTTController.EXTRA_TOPICS),
                            intent.getByteArrayExtra(uMQTTController.EXTRA_TOPICS_QOS),
                            false);
                break;
            case uMQTTController.ACTION_UNSUBSCRIBE:
                if (!intent.hasExtra(uMQTTController.EXTRA_TOPIC)
                        && !intent.hasExtra(uMQTTController.EXTRA_TOPICS))
                    throw new UnsupportedOperationException("Missing unsubscription topics");

                if (intent.hasExtra(uMQTTController.EXTRA_TOPIC))
                    subscribe(intent.getStringExtra(uMQTTController.EXTRA_TOPIC), (byte) 0b01,
                            true);
                else {
                    String[] topics = intent.getStringArrayExtra(uMQTTController.EXTRA_TOPICS);
                    byte[] placeHolder = new byte[topics.length];
                    subscribe(topics, placeHolder, true);
                }
                break;
            case uMQTTController.ACTION_PING:
                pingreq();
                break;
            case uMQTTController.ACTION_PUBLISH:
                publish(intent.getShortExtra(uMQTTController.EXTRA_PACKET_ID, (short) -1));
                break;
            case uMQTTController.ACTION_FORWARD_PUBLISH:
                handlePublishTransaction(
                        intent.getByteExtra(uMQTTController.EXTRA_FRAME_TYPE, (byte) -1),
                        intent.getShortExtra(uMQTTController.EXTRA_PACKET_ID, (short) -1)
                );
        }
    }

    private void openSocket(String serverAddress, int port) {
        Socket socket;
        try {
            socket = new Socket(serverAddress, port);
            mController.setSocket(socket);
            mController.startInputListener(socket.getInputStream());
        }
        catch (IOException e) {
            Timber.e("Could not open socket to broker", e);
        }
    }

    private void publish(short packetId) {
        if (packetId <= 0) {
            throw new IllegalArgumentException("Problem handling packet ID");
        }

        try {
            byte[] packet = mController.getPacket(packetId);
            if (((packet[0] >> 1) & 0b11) == 0b00)
                mController.sentQoS0Packet(packetId);
            mController.getSocket().getOutputStream().write(packet);
        }
        catch (IOException e) {
            Timber.e(e, "Could not send publish to broker.");
        }
    }

    private void handlePublishTransaction(@uMQTTFrame.MQPacketType int type, short packetId) {
        if (type <= 0 || packetId <= 0)
            throw new IllegalArgumentException("Problem handling publish transaction");

        uMQTTFrame frame;
        try {
            try {
                frame = new uMQTTFrame.Builder(type).setPacketId(packetId).build();
            } catch (BrokenMQTTFrameException e) {
                Timber.e(e);
                return;
            }
            mController.getSocket().getOutputStream().write(frame.getPacket());
        }
        catch (IOException e) {
            Timber.e(e, "Could not handle publish transaction with broker");
        }
    }

    private void connect(String clientId) {
        uMQTTFrame frame;
        try {
            try {
                frame = new uMQTTFrame.ConnectBuilder()
                        .setClientId("543badf00d4da0074daf00ba")
                        .setWillFlag()
                        .setWillMessage("Disconnect")
                        .setWillTopic("inbox/control")
                        .setWillQoS(0b01)
                        .setKeepAlive((short)uMQTTController.DEFAULT_KEEP_ALIVE)
                        .build();
            }
            catch (BrokenMQTTFrameException e) {
                Timber.wtf(e);
                return;
            }

            mController.getSocket().getOutputStream().write(frame.getPacket());
            Timber.v("Sent connect packet to broker.");
        }
        catch (IOException e) {
            Timber.e(e, "Failed to send packet to broker.");
        }
    }

    private void subscribe(String topic, byte qosLevel, boolean unsubscribe) {
        uMQTTFrame frame;
        try {
            try {
                uMQTTFrame.SubscribeBuilder builder = new uMQTTFrame.SubscribeBuilder()
                        .setTopics(new String[]{topic})
                        .setQoS(new byte[]{qosLevel});

                if (unsubscribe) builder.setUnsubscribe();
                frame = builder.build();
            }
            catch (BrokenMQTTFrameException e) {
                Timber.wtf(e);
                return;
            }

            if (unsubscribe)
                mController.addToUnhandledUnsubscriptions(frame.getPacketId(), new String[]{topic});
            mController.getSocket().getOutputStream().write(frame.getPacket());
            mController.setSubscriptionsAsAwaiting(frame.getPacketId(), new String[]{topic});
            Timber.v("%subscribing to topic %s", unsubscribe ? "Un" : "S", topic);
        }
        catch (IOException e) {
            Timber.e(e, "Could not send subscription packet for topic %s", topic);
        }
    }

    private void subscribe(String[] topics, byte[] qosLevels, boolean unsubscribe) {
        uMQTTFrame frame;
        try {
            try {
                uMQTTFrame.SubscribeBuilder builder = new uMQTTFrame.SubscribeBuilder()
                        .setTopics(topics)
                        .setQoS(qosLevels);

                if (unsubscribe) builder.setUnsubscribe();
                frame = builder.build();
            }
            catch (BrokenMQTTFrameException e) {
                Timber.wtf(e);
                return;
            }

            if (unsubscribe)
                mController.addToUnhandledUnsubscriptions(frame.getPacketId(), topics);
            mController.getSocket().getOutputStream().write(frame.getPacket());
            mController.setSubscriptionsAsAwaiting(frame.getPacketId(), topics);
            Timber.v("Subscribing to %d topics", topics.length);
        }
        catch (IOException e) {
            Timber.e(e, "Could not send subscription packets for %d topics", topics.length);
        }
    }

    private void pingreq() {
        uMQTTFrame frame;
        try {
            try {
                frame = new uMQTTFrame.Builder(uMQTTFrame.MQ_PINGREQ).build();
            }
            catch (BrokenMQTTFrameException e) {
                Timber.wtf(e, "Missing what?");
                return;
            }

            mController.getSocket().getOutputStream().write(frame.getPacket());
            Timber.v("Sending PINGREQ");
        }
        catch (IOException e) {
            Timber.e(e, "Could not send Ping to server");
        }
    }
}
