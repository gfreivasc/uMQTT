package re.usto.umqtt.utils;

import com.firebase.jobdispatcher.JobParameters;
import com.firebase.jobdispatcher.JobService;
import re.usto.umqtt.uMQTTController;

/**
 * This job is responsible for reopening socket if we lose internet connectivity
 */

public class NetworkJobService extends JobService {

    @Override
    public boolean onStartJob(final JobParameters params) {
        // We automatically start the input listener and send Connect packet through this method.
        uMQTTController.getInstance().openSocket(new uMQTTController.SocketStatusTask() {
            @Override
            public void onSocketOpened(boolean success) {
                jobFinished(params, !success);
            }
        });
        return false;
    }

    @Override
    public boolean onStopJob(JobParameters params) {
        return false;
    }
}
