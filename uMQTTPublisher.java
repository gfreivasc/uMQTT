package re.usto.umqtt;

import android.support.annotation.NonNull;
import android.support.annotation.Nullable;

import com.birbit.android.jobqueue.Job;
import com.birbit.android.jobqueue.JobManager;
import com.birbit.android.jobqueue.Params;
import com.birbit.android.jobqueue.RetryConstraint;
import com.birbit.android.jobqueue.TagConstraint;
import com.birbit.android.jobqueue.config.Configuration;

import java.util.ArrayList;
import java.util.HashMap;

import timber.log.Timber;

/**
 * @author gabriel 
 */

public abstract class uMQTTPublisher {

    protected byte qosLevel = 0b00;
    protected String topic;
    private JobManager mPublishManager;
    private ArrayList<String> mPublishJobs;
    private HashMap<Short, uMQTTPublish> mPublishes;

    private static final String JOB_PUBLISH_PACKET_ID = "pubPacketId";
    private static final String JOB_PUBLISH = "pubJob";

    private String buildJobTag(short packetId) {
        return JOB_PUBLISH + packetId;
    }

    protected uMQTTPublisher(String topic, byte qosLevel) {
        this.topic = topic;
        this.qosLevel = qosLevel;
        mPublishManager = new JobManager(
                new Configuration.Builder(uMQTT.getInstance().getApplicationContext())
                        .minConsumerCount(1)
                        .maxConsumerCount(1)
                        .loadFactor(5)
                        .build());
        mPublishJobs = new ArrayList<>();
        mPublishes = new HashMap<>();
    }

    public short preparePublish(String message){
        uMQTTPublish publish = new uMQTTPublish(topic, message, qosLevel, this);
        return publish.getPacketId();
    }

    public short publish(String message) {
        uMQTTPublish publish = new uMQTTPublish(topic, message, qosLevel, this);

        short packetId;

        if (publish.getQosLevel() > 0) {
            packetId = publish.getPacketId();
            mPublishes.put(publish.getPacketId(), publish);
            mPublishJobs.add(buildJobTag(packetId));
            mPublishManager.addJobInBackground(new PublishJob(packetId));
        }
        else {
            packetId = publish.getPacketId();
            mPublishJobs.add(buildJobTag(packetId));
            mPublishManager.addJobInBackground(new PublishJob(packetId));
            //uMQTT.getInstance().addPublish(publish);
        }
        return packetId;
    }

    public class PublishJob extends Job {

        short mPacketId;
        private static final int PRIORITY = 1;
        public PublishJob(short packetId) {
            super(new Params(PRIORITY).requireNetwork()
                    .singleInstanceBy(buildJobTag(packetId))
                    .addTags(buildJobTag(packetId)));

            mPacketId = packetId;
        }

        @Override
        public void onAdded() {
            Timber.v("Adding publish id #%d", mPacketId);
        }

        @Override
        public void onRun() throws Throwable {
            uMQTTPublish publish = mPublishes.get(mPacketId);
            if (publish == null) return;
            uMQTT.getInstance().addPublish(publish);
        }

        @Override
        protected RetryConstraint shouldReRunOnThrowable(
                @NonNull Throwable throwable, int runCount, int maxRunCount) {
            return RetryConstraint.CANCEL;
        }

        @Override
        protected void onCancel(int cancelReason, @Nullable Throwable throwable) {
            if (mPublishJobs.contains(buildJobTag(mPacketId)));
        }
    }

    protected uMQTTPublish getPublish(short packetId) {
        return mPublishes.get(packetId);
    }

    void completePublish(short packetId) {
        String jobTag = buildJobTag(packetId);
        if (!mPublishJobs.contains(jobTag))
            return;
        mPublishJobs.remove(jobTag);
        mPublishManager.cancelJobsInBackground(null, TagConstraint.ANY, jobTag);
        onPublishCompleted(packetId);
    }

    protected abstract void onPublishCompleted(short packetId);
}
