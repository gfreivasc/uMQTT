package re.usto.umqtt.utils;

import com.firebase.jobdispatcher.JobParameters;
import com.firebase.jobdispatcher.JobService;
import re.usto.umqtt.uMQTT;

/**
 * @author gabriel
 */

public class PingService extends JobService {

    @Override
    public boolean onStartJob(JobParameters jobParameters) {
        uMQTT.getInstance().sendPing();
        return false;
    }

    @Override
    public boolean onStopJob(JobParameters jobParameters) {
        return false;
    }
}
