package re.usto.umqtt;

import android.content.Context;
import android.content.Intent;
import android.content.SharedPreferences;
import android.support.annotation.NonNull;

import com.birbit.android.jobqueue.JobManager;
import com.birbit.android.jobqueue.config.Configuration;
import com.firebase.jobdispatcher.Constraint;
import com.firebase.jobdispatcher.FirebaseJobDispatcher;
import com.firebase.jobdispatcher.GooglePlayDriver;
import com.firebase.jobdispatcher.Job;
import com.firebase.jobdispatcher.Lifetime;
import com.firebase.jobdispatcher.RetryStrategy;
import com.firebase.jobdispatcher.Trigger;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

import re.usto.umqtt.utils.NetworkJobService;
import re.usto.umqtt.utils.PingService;
import timber.log.Timber;

/**
 * @author gabriel
 */

public class uMQTT {

    private static uMQTT mInstance;
    private static Context mApplicationContext;
    private FirebaseJobDispatcher mJobDispatcher;
    private JobManager mJobManager;
    private Socket mSocket;
    private HashMap<String, uMQTTSubscription> mSubscriptions;
    private HashMap<Short, String[]> mUnhandledUnsubscriptions;
    private HashMap<Short, uMQTTPublish> mUnsentPublishes;
    private HashMap<Short, uMQTTPublish> mUnhandledPublishes;
    private List<uMQTTSubscription> mSubscriptionsAwaitingResponse;
    private ArrayList<uMQTTSubscription> mSubscriptionFrames;
    private boolean mConnectedToBroker = false;
    private uMQTTConfiguration mConfiguration;
    private Lock mPublishLock;

    private static final String JS_PING_JOB = "pingJob";

    static final String ACTION_CONNECT =
            "re.usto.umqtt.CONNECT";

    static final String ACTION_DISCONNECT =
            "re.usto.umqtt.DISCONNECT";

    static final String ACTION_SUBSCRIBE =
            "re.usto.umqtt.SUBSCRIBE";

    static final String ACTION_UNSUBSCRIBE =
            "re.usto.umqtt.UNSUBSCRIBE";

    static final String ACTION_PING =
            "re.usto.umqtt.PING";

    static final String ACTION_PUBLISH =
            "re.usto.umqtt.PUBLISH";

    static final String ACTION_FORWARD_PUBLISH =
            "re.usto.umqtt.FORWARD_PUBLISH";

    static final String EXTRA_CLIENT_ID = "extraClientId";
    static final String EXTRA_USERNAME = "extraUsername";
    static final String EXTRA_PASSWORD = "extraPassword";
    static final String EXTRA_TOPIC = "extraTopic";
    static final String EXTRA_TOPICS = "extraTopics";
    static final String EXTRA_TOPIC_QOS = "extraTopicQoS";
    static final String EXTRA_TOPICS_QOS = "extraTopicsQoS";
    static final String EXTRA_PACKET_ID = "extraPacketId";
    static final String EXTRA_FRAME_TYPE = "extraPacketType";

    private static final String PREFS_FILE = "re.usto.umqtt.PREFS";
    private static final String PREF_PACKET_ID = "re.usto.umqtt.PACKET_ID";

    static final int DEFAULT_KEEP_ALIVE = 180;

    private uMQTT(Context context, uMQTTConfiguration configuration) {
        mApplicationContext = context.getApplicationContext();
        mJobDispatcher = new FirebaseJobDispatcher(new GooglePlayDriver(context));
        mJobManager = new JobManager(
                new Configuration.Builder(context).build()
        );
        mConfiguration = configuration;

        scheduleSocketOpening();
    }

    public static uMQTT getInstance() {
        if (mInstance == null) {
            throw new IllegalStateException(
                    "Calling getInstance before initializing controller!");
        }
        return mInstance;
    }

    public static void init(@NonNull Context context,
                            @NonNull uMQTTConfiguration configuration) {
        if (mInstance != null) {
            throw new IllegalStateException(
                    "Cannot initialize controller twice!");
        }
        mInstance = new uMQTT(context, configuration);
    }

    Context getApplicationContext() {
        return mApplicationContext;
    }

    void scheduleSocketOpening() {
        try {
            if (mSocket != null && !mSocket.isClosed()) mSocket.close();
        }
        catch (IOException e) {
            Timber.e(e, "Could not close socket!");
        }

        mJobManager.start();
        mJobManager.addJobInBackground(new NetworkJobService());
    }

    public void openSocket() throws IOException {
        mConnectedToBroker = false;
        mSocket = new Socket(mConfiguration.getBrokerIp(), mConfiguration.getBrokerPort());
        if (mSocket.isConnected())
            startInputListener(mSocket.getInputStream());
        else throw new IOException("Could not connect to broker");
    }

    Socket getSocket() {
        return mSocket;
    }

    void establishConnection() {
        Intent i = new Intent(mApplicationContext, uMQTTOutputService.class);
        i.setAction(ACTION_CONNECT);
        i.putExtra(EXTRA_CLIENT_ID, mConfiguration.getClientId());
        if (mConfiguration.getUsername() != null) {
            i.putExtra(EXTRA_USERNAME, mConfiguration.getUsername());
        }
        if (mConfiguration.getPassword() != null) {
            i.putExtra(EXTRA_PASSWORD, mConfiguration.getPassword());
        }
        mApplicationContext.startService(i);
    }

    void connectionEstablished() {
        mConnectedToBroker = true;
        startKeepAliveMechanism();

        if (mSubscriptionFrames != null) {
            String[] topics = new String[mSubscriptionFrames.size()];
            byte[] qosLevels = new byte[mSubscriptionFrames.size()];

            for (int j = 0; j < mSubscriptionFrames.size(); ++j) {
                topics[j] = mSubscriptionFrames.get(j).getTopic();
                qosLevels[j] = mSubscriptionFrames.get(j).getRequestedQoSLevel();
            }

            Intent i = new Intent(mApplicationContext, uMQTTOutputService.class);
            i.setAction(ACTION_SUBSCRIBE);
            i.putExtra(EXTRA_TOPICS, topics);
            i.putExtra(EXTRA_TOPICS_QOS, qosLevels);
            mApplicationContext.startService(i);
        }

        if (mUnsentPublishes != null) {
            Collection<uMQTTPublish> publishes = mUnsentPublishes.values();
            for (uMQTTPublish publish : publishes) {
                sendPublish(publish);
            }
        }

        if (mConfiguration.hasConnectionCallback())
            mConfiguration.connectionEstablished();
    }

    private void startInputListener(InputStream inputStream) {
        uMQTTInputService.getInstance().start(inputStream);
    }

    public void stopInputListener() {
        uMQTTInputService.getInstance().stop();
    }

    public void sendDisconnectAndCloseSocket() {
        Intent i = new Intent(mApplicationContext, uMQTTOutputService.class);
        i.setAction(ACTION_DISCONNECT);
        mApplicationContext.startService(i);
    }

    public synchronized void addSubscription(String topic, byte qosLevel,
                                uMQTTSubscription.OnReceivedPublish onReceivedPublish) {
        if (mSubscriptions == null) mSubscriptions = new HashMap<>();
        uMQTTSubscription subscription = new uMQTTSubscription(topic, qosLevel, onReceivedPublish);
        mSubscriptions.put(topic, subscription);
        if (mConnectedToBroker) {
            Intent i = new Intent(mApplicationContext, uMQTTOutputService.class);
            i.setAction(ACTION_SUBSCRIBE);
            i.putExtra(EXTRA_TOPIC, topic);
            i.putExtra(EXTRA_TOPIC_QOS, qosLevel);
            mApplicationContext.startService(i);
        }
        else {
            if (mSubscriptionFrames == null) mSubscriptionFrames = new ArrayList<>();
            mSubscriptionFrames.add(subscription);
        }
    }

    public synchronized void addSubscriptions(String[] topics, byte[] qosLevels,
                                 uMQTTSubscription.OnReceivedPublish onReceivedPublish) {
        if (mSubscriptions == null) mSubscriptions = new HashMap<>();
        if (topics.length != qosLevels.length)
            throw new UnsupportedOperationException("Number of topics and QoSLevels differ");

        uMQTTSubscription[] subscriptions = new uMQTTSubscription[topics.length];
        for (int i = 0; i < topics.length; ++i) {
            subscriptions[i] = new uMQTTSubscription(topics[i], qosLevels[i], onReceivedPublish);
            mSubscriptions.put(topics[i], subscriptions[i]);
        }
        if (mConnectedToBroker) {
            Intent i = new Intent(mApplicationContext, uMQTTOutputService.class);
            i.setAction(ACTION_SUBSCRIBE);
            i.putExtra(EXTRA_TOPICS, topics);
            i.putExtra(EXTRA_TOPICS_QOS, qosLevels);
            mApplicationContext.startService(i);
        }
        else {
            if (mSubscriptionFrames == null) mSubscriptionFrames = new ArrayList<>();
            for (uMQTTSubscription subscription : subscriptions)
                mSubscriptionFrames.add(subscription);
        }
    }

    void setSubscriptionsAsAwaiting(short packetId, String[] topics) {
        if (mSubscriptionsAwaitingResponse == null)
            mSubscriptionsAwaitingResponse = new CopyOnWriteArrayList<>();

        ArrayList<uMQTTSubscription> awaitingSubscriptions = new ArrayList<>();
        for (String topic : topics) {
            uMQTTSubscription subscription = mSubscriptions.get(topic);
            subscription.setRequestPacketId(packetId);
            awaitingSubscriptions.add(subscription);
        }

        mSubscriptionsAwaitingResponse.addAll(awaitingSubscriptions);
    }

    synchronized void setResponseToAwaitingSubscriptions(short packetId, byte[] grantedQoSLevels) {
        int i = 0;

        for (Iterator<uMQTTSubscription> it = mSubscriptionsAwaitingResponse.iterator();
             it.hasNext();) {
            uMQTTSubscription subscription = it.next();
            if (subscription.getRequestPacketId() == packetId && i < grantedQoSLevels.length) {
                subscription.setGrantedQosLevel(grantedQoSLevels[i++]);
                Timber.v("Confirmed subscription to topic %s with QoS %d",
                        subscription.getTopic(), subscription.getGrantedQoSLevel());
                mSubscriptionsAwaitingResponse.remove(subscription);
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
                        DEFAULT_KEEP_ALIVE - DEFAULT_KEEP_ALIVE/10,
                        DEFAULT_KEEP_ALIVE - DEFAULT_KEEP_ALIVE/20))
                .build();

        mJobDispatcher.mustSchedule(pingJob);
    }

    public void sendPing() {
        Intent i = new Intent(mApplicationContext, uMQTTOutputService.class);
        i.setAction(ACTION_PING);
        mApplicationContext.startService(i);
    }

    void addPublish(uMQTTPublish publish) {
        if (publish.getQosLevel() == 0 && !mConnectedToBroker)
            return;

        if (mUnsentPublishes == null) {
            mUnsentPublishes = new HashMap<>();
        }
        mUnsentPublishes.put(publish.getPacketId(), publish);
        if (mConnectedToBroker) {
            sendPublish(publish);
        }
        publish.transactionAdvance();
    }

    void sendPublish(uMQTTPublish publish) {
        Timber.v("Sending PUBLISH packet to %s: %s (packet id: %d)",
                publish.getTopic(),
                publish.isMessageTooLong() ? "--Constricted long message--" : publish.getMessage(),
                publish.getPacketId());
        Intent i = new Intent(mApplicationContext, uMQTTOutputService.class);
        i.setAction(ACTION_PUBLISH);
        i.putExtra(EXTRA_PACKET_ID, publish.getPacketId());
        mApplicationContext.startService(i);
    }

    void sentQoS0Packet(short packetId) {
        if (mUnsentPublishes != null)
            mUnsentPublishes.remove(packetId);
    }

    uMQTTPublish getPublish(short packetId) {
        if (mUnsentPublishes.get(packetId) != null){
            return mUnsentPublishes.get(packetId);
        }else{
            return null;
        }
    }

    void advanceOutboundTransaction(short packetId) {
        Timber.v("Acknowledged publish id #%d", packetId);
        uMQTTPublish publish = mUnsentPublishes.get(packetId);
        if (publish == null) return;
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
        if (publish.getQosLevel() == 0) return;

        Intent i = new Intent(mApplicationContext, uMQTTOutputService.class);
        if (mUnhandledPublishes == null) mUnhandledPublishes = new HashMap<>();
        try {
            mUnhandledPublishes.put(publish.getPacketId(), publish);
        }
        catch (NullPointerException e) {
            Timber.v("Dropping...");
            return;
        }
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
    }

    void advanceInboundTransaction(short packetId) {
        uMQTTPublish publish = mUnhandledPublishes.get(packetId);
        if(publish != null) {
            publish.transactionAdvance();
            Intent i = new Intent(mApplicationContext, uMQTTOutputService.class);
            i.setAction(ACTION_FORWARD_PUBLISH);
            i.putExtra(EXTRA_PACKET_ID, packetId);
            if (publish.getState() == uMQTTPublish.PUB_COMPLETED) {
                i.putExtra(EXTRA_FRAME_TYPE, uMQTTFrame.MQ_PUBCOMP);
            } else return;

            Timber.v("Sending PUBCOMP for packet id %d", packetId);
            mApplicationContext.startService(i);
        }else{
            Timber.v("Publish is null");
        }
    }

    public interface OnUnsubscribeListener {
        void onUnsubscribeSuccessful(String[] topics);
    }
    private OnUnsubscribeListener mOnUnsubscribeListener;

    public void setOnUnsubscribeListener(OnUnsubscribeListener onUnsubscribeListener) {
        mOnUnsubscribeListener = onUnsubscribeListener;
    }

    public void unsubscribeFromTopic(String topic) {
        if (!mSubscriptions.containsKey(topic)) {
            Timber.w("There's no subscription to topic %s", topic);
            return;
        }
        else if (mSubscriptionFrames != null
                && mSubscriptionFrames.contains(mSubscriptions.get(topic))) {
            Timber.d("Subscription for topic %s has not been sent"
                    + " and was removed from queue", topic);
            mSubscriptionFrames.remove(mSubscriptions.get(topic));
            return;
        }

        Intent i = new Intent(mApplicationContext, uMQTTOutputService.class);
        i.setAction(ACTION_UNSUBSCRIBE);
        i.putExtra(EXTRA_TOPIC, topic);
        mApplicationContext.startService(i);
    }

    private void forceUnsubscribeFromTopic(String topic) {
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
            else if (mSubscriptionFrames != null
                    && mSubscriptionFrames.contains(mSubscriptions.get(topic))) {
                Timber.d("Subscription for topic %s has not been sent"
                        + " and was removed from queue", topic);
                mSubscriptionFrames.remove(mSubscriptions.get(topic));
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
        if(mUnhandledUnsubscriptions != null) {
            String[] topics = mUnhandledUnsubscriptions.get(packetId);

            if (topics != null) {
                for (String topic : topics) mSubscriptions.remove(topic);
                mOnUnsubscribeListener.onUnsubscribeSuccessful(topics);
            }

            mUnhandledUnsubscriptions.remove(packetId);
        }
    }

    void publishCallback(String topic, String message) {
        try {
            mSubscriptions.get(topic).dispatchMessage(message);
        }
        catch (NullPointerException e) {
            Timber.w(e, "No subscription found tor topic %s", topic);
        }
    }

    public void open() {
        if (mSocket != null && !mSocket.isClosed()
                && mSocket.isConnected() && mConnectedToBroker) {
            Timber.w("uMQTT service already open");
            return;
        }

        scheduleSocketOpening();
    }

    public void close() {
        Timber.i("Closing MQTT connection.");
        mJobDispatcher.cancelAll();
        mJobManager.stop();
        stopInputListener();
        if (mSocket != null && !mSocket.isClosed()) {
            sendDisconnectAndCloseSocket();
        }
        mConnectedToBroker = false;
    }

    public boolean isConnected() {
        return mConnectedToBroker;
    }

    short getTopPacketId() {
        SharedPreferences sp = mApplicationContext.getSharedPreferences(PREFS_FILE,
                Context.MODE_PRIVATE);
        return (short)(sp.getInt(PREF_PACKET_ID, 1) & 0xffff);
    }

    void updateTopPacketId(short packetId) {
        SharedPreferences sp = mApplicationContext.getSharedPreferences(PREFS_FILE,
                Context.MODE_PRIVATE);
        SharedPreferences.Editor editor = sp.edit();
        editor.putInt(PREF_PACKET_ID, packetId & 0xffff);
        editor.commit();
    }

    void sendPuback(short packetId) {
        Intent i = new Intent(mApplicationContext, uMQTTOutputService.class);
        i.setAction(ACTION_FORWARD_PUBLISH);
        i.putExtra(EXTRA_PACKET_ID, packetId);
        i.putExtra(EXTRA_FRAME_TYPE, uMQTTFrame.MQ_PUBACK);
        mApplicationContext.startService(i);
    }

    void sentPacket(short packetId) {
        mUnsentPublishes.get(packetId).setDuplicate();
    }
}
