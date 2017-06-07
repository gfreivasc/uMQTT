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
                connect(intent.getStringExtra(uMQTT.EXTRA_CLIENT_ID));
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
            byte[] packet = mController.getPacket(packetId);
            if(packet != null) {
                if (((packet[0] >> 1) & 0b11) == 0b00)
                    mController.sentQoS0Packet(packetId);
                mController.getSocket().getOutputStream().write(packet);
            }
        }
        catch (IOException e) {
            Timber.e(e, "Could not send publish to broker.");
            try {
                mController.getSocket().close();
                mController.scheduleSocketOpening();
            }
            catch (IOException ex) {
                Timber.wtf(ex);
            }
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
        catch (IOException e) {
            Timber.e(e, "Could not handle publish transaction with broker");
            try {
                mController.getSocket().close();
                mController.scheduleSocketOpening();
            }
            catch (IOException ex) {
                Timber.wtf(ex);
            }
        }
    }

    private void connect(String clientId) {
        uMQTTFrame frame;
        try {
            try {
                frame = new uMQTTFrame.ConnectBuilder()
                        .setClientId(clientId)
                        .setWillFlag()
                        .setWillMessage("Disconnect")
                        .setWillTopic("a/b")
                        .setWillQoS(0b01)
                        .setKeepAlive((short) uMQTT.DEFAULT_KEEP_ALIVE)
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
            try {
                mController.getSocket().close();
                mController.scheduleSocketOpening();
            }
            catch (IOException ex) {
                Timber.wtf(ex);
            }
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
        catch (IOException e) {
            try {
                mController.getSocket().close();
            }
            catch (IOException ex) {
                Timber.wtf(ex);
            }
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
        catch (IOException e) {
            Timber.e(e, "Could not send subscription packet for topic %s", topic);
            try {
                mController.getSocket().close();
                mController.scheduleSocketOpening();
            }
            catch (IOException ex) {
                Timber.wtf(ex);
            }
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
        catch (IOException e) {
            Timber.e(e, "Could not send subscription packets for %d topics", topics.length);
            try {
                mController.getSocket().close();
                mController.scheduleSocketOpening();
            }
            catch (IOException ex) {
                Timber.wtf(ex);
            }
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

            if(mController != null && mController.getSocket().isConnected()) {
                mController.getSocket().getOutputStream().write(frame.getPacket());
                Timber.v("Sending PINGREQ");
            }else{
                Timber.v("NOT Sending PINGREQ");
            }
        }
        catch (IOException e) {
            Timber.e(e, "Could not send Ping to server");
            try {
                mController.getSocket().close();
                mController.scheduleSocketOpening();
            }
            catch (IOException ex) {
                Timber.wtf(ex);
            }
        }
        catch (NullPointerException e) {
            Timber.w("Trying to send Ping before connection is established");
            mController.close();
            mController.open();
        }
    }
}
