package re.usto.umqtt;

import android.content.Context;
import android.content.Intent;
import android.net.ConnectivityManager;
import android.net.NetworkInfo;
import android.support.annotation.NonNull;
import android.util.Log;

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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import io.realm.Realm;
import io.realm.RealmResults;
import re.usto.message.controller.MessageController;
import re.usto.net.UMQTTController;
import re.usto.umqtt.utils.NetworkJobService;
import re.usto.umqtt.utils.PingService;
import re.usto.utils.Group;
import re.usto.utils.MaluhiaApplication;
import timber.log.Timber;

/**
 * @author gabriel
 */

public class uMQTTController {

    private static uMQTTController mInstance;
    private static Context mApplicationContext;
    private uMQTTInputService mInputService;
    private FirebaseJobDispatcher mJobDispatcher;
    private JobManager mJobManager;
    private Socket mSocket;
    private HashMap<String, uMQTTSubscription> mSubscriptions;
    private HashMap<Short, String[]> mUnhandledUnsubscriptions;
    private HashMap<Short, uMQTTPublish> mUnsentPublishes;
    private HashMap<Short, uMQTTPublish> mUnhandledPublishes;
    private ArrayList<uMQTTSubscription> mSubscriptionsAwaitingResponse;
    private ArrayList<uMQTTSubscription> mSubscriptionFrames;
    private boolean mConnectedToBroker = false;
    private String mClientId;
    private String mServerAddress;
    private int mServerPort;

    private static final String JS_PING_JOB = "pingJob";

    static final String ACTION_CONNECT =
            "re.usto.maluhia.CONNECT";

    static final String ACTION_DISCONNECT =
            "re.usto.maluhia.DISCONNECT";

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
    static final String EXTRA_TOPIC = "extraTopic";
    static final String EXTRA_TOPICS = "extraTopics";
    static final String EXTRA_TOPIC_QOS = "extraTopicQoS";
    static final String EXTRA_TOPICS_QOS = "extraTopicsQoS";
    static final String EXTRA_PACKET_ID = "extraPacketId";
    static final String EXTRA_FRAME_TYPE = "extraPacketType";

    static final int DEFAULT_KEEP_ALIVE = 180;

    private uMQTTController(Context context, String client, String serverAddress, int port) {
        mApplicationContext = context.getApplicationContext();
        mJobDispatcher = new FirebaseJobDispatcher(new GooglePlayDriver(context));
        mJobManager = new JobManager(
                new Configuration.Builder(context).build()
        );
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

    public static void init(@NonNull Context context,
                            String client, String serverAddress, int port) {
        if (mInstance != null) {
            throw new IllegalStateException(
                    "Cannot initialize controller twice!");
        }
        mInstance = new uMQTTController(context, client, serverAddress, port);
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
        mSocket = new Socket(mServerAddress, mServerPort);
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
        i.putExtra(EXTRA_CLIENT_ID, mClientId);
        mApplicationContext.startService(i);
    }

    void connectionEstablished() {
        mConnectedToBroker = true;
        startKeepAliveMechanism();

        MessageController.getInstance().publishing = false;

        UMQTTController.getInstance(MaluhiaApplication.getContext()).addSubscription("inbox/"+mClientId);
        UMQTTController.getInstance(MaluhiaApplication.getContext()).addSubscription("inbox/control");

        subscribeToGroups();

        if (mUnsentPublishes != null) {
            Iterator<Map.Entry<Short, uMQTTPublish>> iterator =
                    mUnsentPublishes.entrySet().iterator();
            while (iterator.hasNext()) {
                addPublish(iterator.next().getValue());
            }
        }
    }

    private void subscribeToGroups(){
        Realm realm = Realm.getDefaultInstance();
        realm.executeTransaction(new Realm.Transaction() {
            @Override
            public void execute(Realm realm) {
                RealmResults<Group> result = realm.where(Group.class).findAll();
                if(!result.isEmpty()) {
                    for(Group group : result) {
                        Log.d("USTORE", "SUBSCRIBE GROUP " + group.gid);
                        UMQTTController.getInstance(MaluhiaApplication.getContext()).addSubscription("inbox/group/"+group.gid);
                    }
                }
            }
        });
        realm.close();
    }


    private void startInputListener(InputStream inputStream) {
        mInputService = uMQTTInputService.getInstance();
        mInputService.start(inputStream);
    }

    public void stopInputListener() {
        mInputService.stop();
    }

    public void sendDisconnect() {
        Intent i = new Intent(mApplicationContext, uMQTTOutputService.class);
        i.setAction(ACTION_DISCONNECT);
        mApplicationContext.startService(i);
    }

    public void addSubscription(String topic, byte qosLevel,
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
            if (subscription.getRequestPacketId() == packetId && i < grantedQoSLevels.length) {
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
        Intent i = new Intent(mApplicationContext, uMQTTOutputService.class);
        i.setAction(ACTION_PING);
        mApplicationContext.startService(i);
    }

    void addPublish(uMQTTPublish publish) {
        if (mUnsentPublishes == null) {
            mUnsentPublishes = new HashMap<>();
        }
        mUnsentPublishes.put(publish.getPacketId(), publish);
        if (mConnectedToBroker) {
            Intent i = new Intent(mApplicationContext, uMQTTOutputService.class);
            i.setAction(ACTION_PUBLISH);
            i.putExtra(EXTRA_PACKET_ID, publish.getPacketId());
            mApplicationContext.startService(i);
        }
        publish.transactionAdvance();
        Timber.v("Sending PUBLISH packet to %s: %s (packet id: %d)",
                publish.getTopic(), publish.getMessage(), publish.getPacketId());
    }

    void sentQoS0Packet(short packetId) {
        if (mUnsentPublishes != null)
            mUnsentPublishes.remove(packetId);
    }

    byte[] getPacket(short packetId) {
        if (mUnsentPublishes.get(packetId) != null){
            return mUnsentPublishes.get(packetId).getPacket();
        }else{
            return null;
        }
    }

    void advanceOutboundTransaction(short packetId) {
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
        mUnhandledPublishes.put(publish.getPacketId(), publish);
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
        String[] topics = mUnhandledUnsubscriptions.get(packetId);

        if (topics != null)
            for (String topic : topics)
                mSubscriptions.remove(topic);

        mUnhandledUnsubscriptions.remove(packetId);
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
        sendDisconnect();
        mConnectedToBroker = false;
    }

    public boolean isConnected() {
        return mConnectedToBroker;
    }
}
