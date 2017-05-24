package re.usto.umqtt;

import android.content.Context;
import android.content.Intent;
import android.net.ConnectivityManager;
import android.net.NetworkInfo;
import android.os.Bundle;

import com.firebase.jobdispatcher.Constraint;
import com.firebase.jobdispatcher.FirebaseJobDispatcher;
import com.firebase.jobdispatcher.GooglePlayDriver;
import com.firebase.jobdispatcher.Job;
import com.firebase.jobdispatcher.Lifetime;
import com.firebase.jobdispatcher.RetryStrategy;
import com.firebase.jobdispatcher.Trigger;

import java.io.InputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import re.usto.umqtt.utils.NetworkJobService;
import re.usto.umqtt.utils.PingService;
import timber.log.Timber;

/**
 * @author gabriel
 */

public class uMQTTController {

    private static uMQTTController mInstance;
    private uMQTTInputService mInputService;
    private Context mApplicationContext;
    private Socket mSocket;
    private FirebaseJobDispatcher mJobDispatcher;
    private SocketStatusTask mRunWhenSocketOpen;
    private ConnectivityManager mConnectivityManager;
    private HashMap<String, uMQTTSubscription> mSubscriptions;
    private HashMap<Short, String[]> mUnhandledUnsubscriptions;
    private HashMap<Short, uMQTTPublish> mUnsentPublishes;
    private HashMap<Short, uMQTTPublish> mUnhandledPublishes;
    private ArrayList<uMQTTSubscription> mSubscriptionsAwaitingResponse;
    private ArrayList<uMQTTSubscription> mUnsentSubscriptions;
    private boolean mConnectedToBroker = false;
    private String mClientId;
    private String mServerAddress;
    private int mServerPort;


    public static abstract class SocketStatusTask implements Runnable {

        private boolean connected = false;

        public void setConnected(boolean connected) {
            this.connected = connected;
        }

        @Override
        public void run() {
            onSocketOpened(connected);
        }

        public abstract void onSocketOpened(boolean success);

    }
    private static final String JS_NETWORK_OPEN_SOCKET_JOB = "openSocketJob";
    private static final String JS_PING_JOB = "pingJob";

    static final String ACTION_CONNECT =
            "re.usto.maluhia.CONNECT";

    static final String ACTION_OPEN_MQTT =
            "re.usto.maluhia.OPEN_MQTT";

    static final String ACTION_SUBSCRIBE =
            "re.usto.maluhia.SUBSCRIBE";

    static final String ACTION_UNSUBSCRIBE =
            "re.usto.maluhia.UNSUBSCRIBE";

    static final String ACTION_PING =
            "re.usto.maluhia.PING";

    static final String ACTION_PUBLISH =
            "re.usto.maluhia.PUBLISH";

    static final String ACTION_FORWARD_PUBLISH =
            "re.usto.maluhia.FORWARD_PUBLISH";

    static final String EXTRA_CLIENT_ID = "extraClientId";
    static final String EXTRA_SERVER_ADDRESS = "extraServerAddres";
    static final String EXTRA_SERVER_PORT = "extraServerPort";
    static final String EXTRA_TOPIC = "extraTopic";
    static final String EXTRA_TOPICS = "extraTopics";
    static final String EXTRA_TOPIC_QOS = "extraTopicQoS";
    static final String EXTRA_TOPICS_QOS = "extraTopicsQoS";
    static final String EXTRA_PACKET_ID = "extraPacketId";
    static final String EXTRA_FRAME_TYPE = "extraPacketType";

    static final int DEFAULT_KEEP_ALIVE = 180;

    private uMQTTController(Context context, String client, String serverAddress, int port) {
        mApplicationContext = context.getApplicationContext();
        mConnectivityManager = (ConnectivityManager)
                mApplicationContext.getSystemService(Context.CONNECTIVITY_SERVICE);
        mJobDispatcher = new FirebaseJobDispatcher(new GooglePlayDriver(context));
        mClientId = client;
        mServerAddress = serverAddress;
        mServerPort = port;
        scheduleSocketOpening();
    }

    public static uMQTTController getInstance() {
        if (mInstance == null) {
            throw new IllegalStateException(
                    "Calling getInstance before initializing controller!");
        }
        return mInstance;
    }

    public static void init(Context context, String client, String serverAddress, int port) {
        if (mInstance != null) {
            throw new IllegalStateException(
                    "Cannot initialize controller twice!");
        }
        mInstance = new uMQTTController(context, client, serverAddress, port);
    }

    public void scheduleSocketOpening() {
        Job openSocketJob = mJobDispatcher.newJobBuilder()
                .setService(NetworkJobService.class)
                .setTag(JS_NETWORK_OPEN_SOCKET_JOB)
                .setConstraints(Constraint.ON_ANY_NETWORK)
                .setTrigger(Trigger.NOW)
                .build();

        mJobDispatcher.mustSchedule(openSocketJob);
    }

    public void openSocket(SocketStatusTask task) {
        Intent i = new Intent(mApplicationContext, uMQTTOutputService.class);
        i.setAction(ACTION_OPEN_MQTT);
        i.putExtra(EXTRA_SERVER_ADDRESS, mServerAddress);
        i.putExtra(EXTRA_SERVER_PORT, mServerPort);
        mApplicationContext.startService(i);
        mRunWhenSocketOpen = task;
    }

    void setSocket(Socket socket) {
        mSocket = socket;
        if (mSocket != null) {
            mRunWhenSocketOpen.setConnected(mSocket.isConnected());
            mRunWhenSocketOpen.run();
        }
    }

    Socket getSocket() {
        return mSocket;
    }

    void establishConnection() {
        if (!isConnected()) return;

        Intent i = new Intent(mApplicationContext, uMQTTOutputService.class);
        i.setAction(ACTION_CONNECT);
        i.putExtra(EXTRA_CLIENT_ID, mClientId);
        mApplicationContext.startService(i);
    }

    void connectionEstablished() {
        mConnectedToBroker = true;
        startKeepAliveMechanism();

        if (mUnsentSubscriptions != null) {
            String[] topics = new String[mUnsentSubscriptions.size()];
            byte[] qosLevels = new byte[mUnsentSubscriptions.size()];

            for (int j = 0; j < mUnsentSubscriptions.size(); ++j) {
                topics[j] = mUnsentSubscriptions.get(j).getTopic();
                qosLevels[j] = mUnsentSubscriptions.get(j).getRequestedQoSLevel();
            }

            mUnsentSubscriptions = null;

            Intent i = new Intent(mApplicationContext, uMQTTOutputService.class);
            i.setAction(ACTION_SUBSCRIBE);
            i.putExtra(EXTRA_TOPICS, topics);
            i.putExtra(EXTRA_TOPICS_QOS, qosLevels);
            mApplicationContext.startService(i);
        }
    }

    void startInputListener(InputStream inputStream) {
        if (!isConnected()) return;

        mInputService = uMQTTInputService.getInstance();
        mInputService.start(inputStream);
    }

    public void stopInputListener() {
        mInputService.stop();
    }

    public boolean isConnected() {
        NetworkInfo activeNetwork = mConnectivityManager.getActiveNetworkInfo();
        boolean connected = activeNetwork != null && activeNetwork.isConnected()
                && mSocket != null;
        if (!connected) {
            scheduleSocketOpening();
            mConnectedToBroker = false;
        }
        return connected;
    }

    public void addSubscription(String topic, byte qosLevel,
                                uMQTTSubscription.OnReceivedPublish onReceivedPublish) {
        if (mSubscriptions == null) mSubscriptions = new HashMap<>();
        uMQTTSubscription subscription = new uMQTTSubscription(topic, qosLevel, onReceivedPublish);
        mSubscriptions.put(topic, subscription);
        if (mConnectedToBroker && isConnected()) {
            Intent i = new Intent(mApplicationContext, uMQTTOutputService.class);
            i.setAction(ACTION_SUBSCRIBE);
            i.putExtra(EXTRA_TOPIC, topic);
            i.putExtra(EXTRA_TOPIC_QOS, qosLevel);
            mApplicationContext.startService(i);
        }
        else {
            if (mUnsentSubscriptions == null) mUnsentSubscriptions = new ArrayList<>();
            mUnsentSubscriptions.add(subscription);
        }
    }

    public void addSubscriptions(String[] topics, byte[] qosLevels,
                                 uMQTTSubscription.OnReceivedPublish onReceivedPublish) {
        if (mSubscriptions == null) mSubscriptions = new HashMap<>();
        if (topics.length != qosLevels.length)
            throw new UnsupportedOperationException("Number of topics and QoSLevels differ");

        uMQTTSubscription[] subscriptions = new uMQTTSubscription[topics.length];
        for (int i = 0; i < topics.length; ++i) {
            subscriptions[i] = new uMQTTSubscription(topics[i], qosLevels[i], onReceivedPublish);
            mSubscriptions.put(topics[i], subscriptions[i]);
        }
        if (mConnectedToBroker && isConnected()) {
            Intent i = new Intent(mApplicationContext, uMQTTOutputService.class);
            i.setAction(ACTION_SUBSCRIBE);
            i.putExtra(EXTRA_TOPICS, topics);
            i.putExtra(EXTRA_TOPICS_QOS, qosLevels);
            mApplicationContext.startService(i);
        }
        else {
            if (mUnsentSubscriptions == null) mUnsentSubscriptions = new ArrayList<>();
            for (uMQTTSubscription subscription : subscriptions)
                mUnsentSubscriptions.add(subscription);
        }
    }

    void setSubscriptionsAsAwaiting(short packetId, String[] topics) {
        if (mSubscriptionsAwaitingResponse == null)
            mSubscriptionsAwaitingResponse = new ArrayList<>();

        for (String topic : topics) {
            uMQTTSubscription subscription = mSubscriptions.get(topic);
            subscription.setRequestPacketId(packetId);
            mSubscriptionsAwaitingResponse.add(subscription);
        }
    }

    void setResponseToAwaitingSubscriptions(short packetId, byte[] grantedQoSLevels) {
        int i = 0;
        for (Iterator<uMQTTSubscription> it = mSubscriptionsAwaitingResponse.iterator();
                it.hasNext();) {
            uMQTTSubscription subscription = it.next();
            if (subscription.getRequestPacketId() == packetId) {
                subscription.setGrantedQosLevel(grantedQoSLevels[i++]);
                Timber.v("Confirmed subscription to topic %s with QoS %d",
                        subscription.getTopic(), subscription.getGrantedQoSLevel());
                it.remove();
            }
            else if (i != 0) break;
        }
    }

    private void startKeepAliveMechanism() {
        Job pingJob = mJobDispatcher.newJobBuilder()
                .setService(PingService.class)
                .setTag(JS_PING_JOB)
                .addConstraint(Constraint.ON_ANY_NETWORK)
                .setRetryStrategy(RetryStrategy.DEFAULT_EXPONENTIAL)
                .setLifetime(Lifetime.FOREVER)
                .setRecurring(true)
                .setReplaceCurrent(true)
                .setTrigger(Trigger.executionWindow(
                        DEFAULT_KEEP_ALIVE - 10, DEFAULT_KEEP_ALIVE))
                .build();

        mJobDispatcher.mustSchedule(pingJob);
    }

    public void sendPing() {
        if (!isConnected()) return;

        Intent i = new Intent(mApplicationContext, uMQTTOutputService.class);
        i.setAction(ACTION_PING);
        mApplicationContext.startService(i);
    }

    void addPublish(uMQTTPublish publish) {
        if (mConnectedToBroker && isConnected()) {
            Intent i = new Intent(mApplicationContext, uMQTTOutputService.class);
            i.setAction(ACTION_PUBLISH);
            i.putExtra(EXTRA_PACKET_ID, publish.getPacketId());
            mApplicationContext.startService(i);
        }
        publish.transactionAdvance();
        if (publish.getQosLevel() > 0) {
            if (mUnsentPublishes == null) mUnsentPublishes = new HashMap<>();
            mUnsentPublishes.put(publish.getPacketId(), publish);
        }
        Timber.v("Sent PUBLISH packet to %s (packet id: %d)",
                publish.getTopic(), publish.getPacketId());
    }

    byte[] getPacket(short packetId) {
        return mUnsentPublishes.get(packetId).getPacket();
    }

    void advanceOutboundTransaction(short packetId) {
        uMQTTPublish publish = mUnsentPublishes.get(packetId);
        publish.transactionAdvance();
        if (publish.getState() == uMQTTPublish.PUB_RECEIVED) {
            Intent i = new Intent(mApplicationContext, uMQTTOutputService.class);
            i.setAction(ACTION_FORWARD_PUBLISH);
            i.putExtra(EXTRA_PACKET_ID, packetId);
            i.putExtra(EXTRA_FRAME_TYPE, uMQTTFrame.MQ_PUBREL);
            mApplicationContext.startService(i);
        }
        else mUnsentPublishes.remove(packetId);
    }

    void advanceInboundTransaction(uMQTTPublish publish) {
        publish.transactionAdvance();
        Intent i = new Intent(mApplicationContext, uMQTTOutputService.class);
        i.setAction(ACTION_FORWARD_PUBLISH);
        i.putExtra(EXTRA_PACKET_ID, publish.getPacketId());
        if (publish.getQosLevel() == 0b01) {
            i.putExtra(EXTRA_FRAME_TYPE, uMQTTFrame.MQ_PUBACK);
            Timber.v("Sending PUBACK for packet id %d", publish.getPacketId());
        }
        else if (publish.getQosLevel() == 0b10) {
            i.putExtra(EXTRA_FRAME_TYPE, uMQTTFrame.MQ_PUBREC);
            Timber.v("Sending PUBREC for packet id %d", publish.getPacketId());
        }
        else return;

        mApplicationContext.startService(i);
        if (mUnhandledPublishes == null) mUnhandledPublishes = new HashMap<>();
        mUnhandledPublishes.put(publish.getPacketId(), publish);
    }

    void advanceInboundTransaction(short packetId) {
        uMQTTPublish publish = mUnhandledPublishes.get(packetId);
        publish.transactionAdvance();
        Intent i = new Intent(mApplicationContext, uMQTTOutputService.class);
        i.setAction(ACTION_FORWARD_PUBLISH);
        i.putExtra(EXTRA_PACKET_ID, packetId);
        if (publish.getState() == uMQTTPublish.PUB_COMPLETED) {
            i.putExtra(EXTRA_FRAME_TYPE, uMQTTFrame.MQ_PUBCOMP);
        }
        else return;

        Timber.v("Sending PUBCOMP for packet id %d", packetId);
        mApplicationContext.startService(i);
    }

    public void unsubscribeFromTopic(String topic) {
        if (!mSubscriptions.containsKey(topic)) {
            Timber.w("There's no subscription to topic %s", topic);
            return;
        }
        else if (mUnsentSubscriptions != null
                && mUnsentSubscriptions.contains(mSubscriptions.get(topic))) {
            Timber.d("Subscription for topic %s has not been sent"
                    + " and was removed from queue", topic);
            mUnsentSubscriptions.remove(mSubscriptions.get(topic));
            return;
        }

        Intent i = new Intent(mApplicationContext, uMQTTOutputService.class);
        i.setAction(ACTION_UNSUBSCRIBE);
        i.putExtra(EXTRA_TOPIC, topic);
        mApplicationContext.startService(i);
    }

    public void unsubscribeFromTopics(String[] topics) {
        boolean send = false;
        for (String topic : topics) {
            if (!mSubscriptions.containsKey(topic)) {
                Timber.w("There's no valid subscription to topic %s", topic);
                return;
            }
            else if (mUnsentSubscriptions != null
                    && mUnsentSubscriptions.contains(mSubscriptions.get(topic))) {
                Timber.d("Subscription for topic %s has not been sent"
                        + " and was removed from queue", topic);
                mUnsentSubscriptions.remove(mSubscriptions.get(topic));
            }
            else send = true;
        }

        if (send) {
            Intent i = new Intent(mApplicationContext, uMQTTOutputService.class);
            i.setAction(ACTION_UNSUBSCRIBE);
            i.putExtra(EXTRA_TOPICS, topics);
            mApplicationContext.startService(i);
        }
    }

    void addToUnhandledUnsubscriptions(short packetId, String[] topics) {
        if (mUnhandledUnsubscriptions == null) mUnhandledUnsubscriptions = new HashMap<>();
        mUnhandledUnsubscriptions.put(packetId, topics);
    }

    void removeSubscriptions(short packetId) {
        String[] topics = mUnhandledUnsubscriptions.get(packetId);

        for (String topic : topics)
            mSubscriptions.remove(topic);

        mUnhandledUnsubscriptions.remove(packetId);
    }

    void publishCallback(String topic, String message) {
        try {
            mSubscriptions.get(topic).dispatchMessage(message);
        }
        catch (NullPointerException e) {
            Timber.e(e, "No subscription found tor topic %s", topic);
        }
    }
}
