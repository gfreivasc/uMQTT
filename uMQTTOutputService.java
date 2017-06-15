package re.usto.umqtt;

import android.app.IntentService;
import android.content.Intent;
import android.support.annotation.Nullable;

import timber.log.Timber;

/**
 * @author gabriel
 */

public class uMQTTOutputService extends IntentService {

    private uMQTT mController = uMQTT.getInstance();

    public uMQTTOutputService() {
        super(uMQTTOutputService.class.getSimpleName());
    }

    @Override
    protected void onHandleIntent(@Nullable Intent intent) {
        if (intent == null || intent.getAction() == null) {
            Timber.w("Invalid intent received");
            return;
        }

        switch (intent.getAction()) {
            case uMQTT.ACTION_CONNECT:
                if (!intent.hasExtra(uMQTT.EXTRA_CLIENT_ID))
                    throw new UnsupportedOperationException("Missing client ID");
                String username = intent.hasExtra(uMQTT.EXTRA_USERNAME) ?
                        intent.getStringExtra(uMQTT.EXTRA_USERNAME) : null;
                String password = intent.hasExtra(uMQTT.EXTRA_PASSWORD) ?
                        intent.getStringExtra(uMQTT.EXTRA_PASSWORD) : null;
                connect(intent.getStringExtra(uMQTT.EXTRA_CLIENT_ID),
                        username,
                        password);
                break;
            case uMQTT.ACTION_DISCONNECT:
                disconnect();
                break;
            case uMQTT.ACTION_SUBSCRIBE:
                if ((!intent.hasExtra(uMQTT.EXTRA_TOPIC)
                        || !intent.hasExtra(uMQTT.EXTRA_TOPIC_QOS))
                        && (!intent.hasExtra(uMQTT.EXTRA_TOPICS)
                        || !intent.hasExtra(uMQTT.EXTRA_TOPICS_QOS)))
                    throw new UnsupportedOperationException("Missing subscription info");

                if (intent.hasExtra(uMQTT.EXTRA_TOPIC))
                    subscribe(intent.getStringExtra(uMQTT.EXTRA_TOPIC),
                            intent.getByteExtra(uMQTT.EXTRA_TOPIC_QOS, (byte) 0b00),
                            false);
                else
                    subscribe(intent.getStringArrayExtra(uMQTT.EXTRA_TOPICS),
                            intent.getByteArrayExtra(uMQTT.EXTRA_TOPICS_QOS),
                            false);
                break;
            case uMQTT.ACTION_UNSUBSCRIBE:
                if (!intent.hasExtra(uMQTT.EXTRA_TOPIC)
                        && !intent.hasExtra(uMQTT.EXTRA_TOPICS))
                    throw new UnsupportedOperationException("Missing unsubscription topics");

                if (intent.hasExtra(uMQTT.EXTRA_TOPIC))
                    subscribe(intent.getStringExtra(uMQTT.EXTRA_TOPIC), (byte) 0b01,
                            true);
                else {
                    String[] topics = intent.getStringArrayExtra(uMQTT.EXTRA_TOPICS);
                    byte[] placeHolder = new byte[topics.length];
                    subscribe(topics, placeHolder, true);
                }
                break;
            case uMQTT.ACTION_PING:
                pingreq();
                break;
            case uMQTT.ACTION_PUBLISH:
                publish(intent.getShortExtra(uMQTT.EXTRA_PACKET_ID, (short) 0));
                break;
            case uMQTT.ACTION_FORWARD_PUBLISH:
                handlePublishTransaction(
                        intent.getByteExtra(uMQTT.EXTRA_FRAME_TYPE, (byte) 0),
                        intent.getShortExtra(uMQTT.EXTRA_PACKET_ID, (short) 0)
                );
        }
    }

    private void publish(short packetId) {
        //if (packetId == 0) {
        //    throw new IllegalArgumentException("Problem handling packet ID");
        //}

        try {
            uMQTTPublish publish = mController.getPublish(packetId);
            if(publish != null) {
                byte[] packet = publish.getPacket();
                mController.getSocket().getOutputStream().write(packet);
                if (((packet[0] >> 1) & 0b11) == 0b00)
                    mController.sentQoS0Packet(packetId);
                else
                    mController.sentPacket(packetId);
            }
        }
        catch (Throwable e) {
            Timber.e(e, "Could not send publish to broker.");
            mController.close();
            mController.open();
        }
    }

    private void handlePublishTransaction(@uMQTTFrame.MQPacketType int type, short packetId) {
        //if (type == 0 || packetId == 0)
            //throw new IllegalArgumentException("Problem handling publish transaction");

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
        catch (Throwable e) {
            Timber.e(e, "Could not handle publish transaction with broker");
            mController.close();
            mController.open();
        }
    }

    private void connect(String clientId, String username, String password) {
        uMQTTFrame frame;
        try {
            try {
                uMQTTFrame.ConnectBuilder builder = new uMQTTFrame.ConnectBuilder()
                        .setClientId(clientId)
                        .setWillFlag()
                        .setWillMessage("Disconnect")
                        .setWillTopic("a/b")
                        .setWillQoS(0b01)
                        .setKeepAlive((short) uMQTT.DEFAULT_KEEP_ALIVE);

                if (username != null) builder.setUsername(username);
                if (password != null) builder.setPassword(password);
                frame = builder.build();
            }
            catch (BrokenMQTTFrameException e) {
                Timber.wtf(e);
                return;
            }

            Timber.d("Frame: ", frame.getPacket().toString());
            mController.getSocket().getOutputStream().write(frame.getPacket());
            Timber.v("Sent connect packet to broker.");
        }
        catch (Throwable e) {
            Timber.e(e, "Failed to send packet to broker.");
            mController.close();
            mController.open();
        }
    }

    private void disconnect() {
        uMQTTFrame frame;
        try {
            frame = new uMQTTFrame.Builder(uMQTTFrame.MQ_DISCONNECT)
                    .build();
        }
        catch (BrokenMQTTFrameException e) {
            Timber.e(e);
            return;
        }
        try {
            mController.getSocket().getOutputStream().write(frame.getPacket());
            mController.getSocket().close();
        }
        catch (Throwable e) {
            mController.close();
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
            else
                mController.setSubscriptionsAsAwaiting(frame.getPacketId(), new String[]{topic});

            if(mController.getSocket().isConnected()) {
                mController.getSocket().getOutputStream().write(frame.getPacket());
                Timber.v("%subscribing to topic %s", unsubscribe ? "Un" : "S", topic);
            }
        }
        catch (Throwable e) {
            Timber.e(e, "Could not send subscription packet for topic %s", topic);
            mController.close();
            mController.open();
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
            else
                mController.setSubscriptionsAsAwaiting(frame.getPacketId(), topics);
            mController.getSocket().getOutputStream().write(frame.getPacket());
            Timber.v("Subscribing to %d topics", topics.length);
        }
        catch (Throwable e) {
            Timber.e(e, "Could not send subscription packets for %d topics", topics.length);
            mController.close();
            mController.open();
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

            if(mController != null && mController.getSocket() != null && mController.getSocket().isConnected()) {
                mController.getSocket().getOutputStream().write(frame.getPacket());
                Timber.v("Sending PINGREQ");
            }else{
                Timber.v("NOT Sending PINGREQ");
            }
        }
        catch (Throwable e) {
            Timber.e(e, "Could not send Ping to server");
            mController.close();
            mController.open();
        }
    }
}
