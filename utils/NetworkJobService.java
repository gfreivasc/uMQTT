package re.usto.umqtt.utils;

import android.support.annotation.NonNull;
import android.support.annotation.Nullable;

import com.birbit.android.jobqueue.Job;
import com.birbit.android.jobqueue.Params;
import com.birbit.android.jobqueue.RetryConstraint;

import re.usto.umqtt.uMQTT;
import timber.log.Timber;

/**
 * This job is responsible for reopening socket if we lose internet connectivity
 */
public class NetworkJobService extends Job {

    private static final int PRIORITY = 1;
    private static final int INITIAL_BACKOFF = 500;

    public NetworkJobService() {
        super(new Params(PRIORITY).requireNetwork().singleInstanceBy("OPEN_SOCKET_JOB"));
    }

    @Override
    public void onAdded() {
        Timber.v("Open socket job added");
    }

    @Override
    public void onRun() throws Throwable {
        uMQTT.getInstance().openSocket();
    }

    @Override
    protected RetryConstraint shouldReRunOnThrowable(
            @NonNull Throwable throwable, int runCount, int maxRunCount) {
        return RetryConstraint.createExponentialBackoff(runCount, INITIAL_BACKOFF);
    }

    @Override
    protected void onCancel(int cancelReason, @Nullable Throwable throwable) {
        Timber.v("Could not connect.");
    }
}

