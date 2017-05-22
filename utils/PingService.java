package re.usto.maluhia.umqtt.utils;

import com.firebase.jobdispatcher.JobParameters;
import com.firebase.jobdispatcher.JobService;
import com.gabrielfv.maluhia.mqtt.uMQTTController;

/**
 * @author gabriel
 */

public class PingService extends JobService {

    @Override
    public boolean onStartJob(JobParameters jobParameters) {
        uMQTTController.getInstance().sendPing();
        return false;
    }

    @Override
    public boolean onStopJob(JobParameters jobParameters) {
        return false;
    }
}
