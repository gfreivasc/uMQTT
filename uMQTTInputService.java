package re.usto.umqtt;

import android.support.annotation.IntDef;
import android.support.annotation.WorkerThread;

import java.io.IOException;
import java.io.InputStream;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.util.Arrays;

import timber.log.Timber;

/**
 * @author gabriel
 */

public class uMQTTInputService {

    private Thread mTCPListenerThread;
    private boolean mRun = true;
    private InputStream mInputStream;
    private static uMQTTInputService mInstance;

    private static final int BYTES_FIXED_HEADER = 2;
    private static final int BYTES_SINGLE_STEP = 1;
    private static int readStepSize = BYTES_FIXED_HEADER;
    private static int remainingSize = 0;
    private static int readOffset = 0;
    private static int bytesRead = 0;

    private static final int
            RS_WAITING = 0,
            RS_FETCHING_REMAINING_SIZE = 1,
            RS_FETCHING_FULL_PACKET = 2;

    @Retention(RetentionPolicy.SOURCE)
    @IntDef({RS_WAITING, RS_FETCHING_REMAINING_SIZE, RS_FETCHING_FULL_PACKET})
    private @interface MQReadState {
    }

    private @MQReadState
    int mReadState;

    // Size = 16MB
    private static final int INPUT_BUFFER_LENGTH = 16 * 1024 * 1024;
    private static byte[] buffer = new byte[INPUT_BUFFER_LENGTH];

    private uMQTTInputService() {
    }

    static uMQTTInputService getInstance() {
        if (mInstance == null) mInstance = new uMQTTInputService();
        return mInstance;
    }

    private Runnable mListener = new Runnable() {
        @Override
        public void run() {
            try {
                while (mRun) {
                    int readSize = mInputStream.read(buffer, readOffset, readStepSize);
                    if (readSize > 0)
                        dispatchMessage(buffer);
                }
            } catch (IOException e) {
                Timber.e(e);
                stop();
            }
        }
    };

    /**
     * Socket input instance is necessary here, that's where our service
     * will read from.
     *
     * @param mqttSocketInput The InputStream from the socket.
     */
    void start(InputStream mqttSocketInput) {
        mRun = true;
        mInputStream = mqttSocketInput;
        mTCPListenerThread = new Thread(mListener);
        mTCPListenerThread.start();
        uMQTTController.getInstance().establishConnection();
    }

    void stop() {
        mRun = false;
        mInputStream = null;
    }

    @WorkerThread
    private synchronized void dispatchMessage(byte[] excerpt) {
        switch (mReadState) {
            case RS_WAITING:
                remainingSize = (excerpt[1] & 0x7f);
                if ((excerpt[1] & 0x80) != 0) {
                    mReadState = RS_FETCHING_REMAINING_SIZE;
                    readStepSize = BYTES_SINGLE_STEP;
                } else if (remainingSize > 0) {
                    mReadState = RS_FETCHING_FULL_PACKET;
                    readStepSize = remainingSize;
                } else {
                    onMessageReceived(excerpt, readOffset);
                    return;
                }
                readOffset += 2;
                break;
            case RS_FETCHING_REMAINING_SIZE:
                remainingSize *= 0x80;
                remainingSize += excerpt[readOffset];
                if ((excerpt[readOffset] & 0x80) == 0) {
                    mReadState = RS_FETCHING_FULL_PACKET;
                    readStepSize = remainingSize;
                }
                readOffset++;
                break;
            case RS_FETCHING_FULL_PACKET:
                onMessageReceived(excerpt, readOffset + readStepSize);
                mReadState = RS_WAITING;
                readStepSize = BYTES_FIXED_HEADER;
                readOffset = 0;
                remainingSize = 0;
                break;
        }
    }

    private void onMessageReceived(byte[] message, int size) {
        @uMQTTFrame.MQPacketType int type = (message[0] >> 4) & 0xf;
        switch (type) {
            case uMQTTFrame.MQ_CONNACK:
                handleConnack(message[3]);
                break;
            case uMQTTFrame.MQ_SUBACK:
                handleSuback(message, size);
                break;
            case uMQTTFrame.MQ_UNSUBACK:
                handleUnsuback(message[2], message[3]);
            case uMQTTFrame.MQ_PINGRESP:
                handlePingresp();
                break;
            case uMQTTFrame.MQ_PUBACK:
            case uMQTTFrame.MQ_PUBREC:
            case uMQTTFrame.MQ_PUBCOMP:
                handleOutboundQoS(type, message);
                break;
            case uMQTTFrame.MQ_PUBLISH:
            case uMQTTFrame.MQ_PUBREL:
                handleInboundQoS(type, message);
                break;
            default:
                break;
        }
    }

    private void handleUnsuback(byte msb, byte lsb) {
        short packetId = (short)(msb * 0xff + lsb);
        uMQTTController.getInstance().removeSubscriptions(packetId);
    }

    private void handleOutboundQoS(int type, byte[] message) {
        short packetId = (short)(message[2] * 0xff + message[3]);
        uMQTTController.getInstance().advanceOutboundTransaction(packetId);
    }

    private void handleInboundQoS(int type, byte[] message) {
        if (type == uMQTTFrame.MQ_PUBLISH) {
            uMQTTPublish publish = new uMQTTPublish(message);

            uMQTTController.getInstance().advanceInboundTransaction(
                    publish
            );
        }
        else {
            short packetId = (short)(message[2] * 0xff + message[3]);
            uMQTTController.getInstance().advanceInboundTransaction(packetId);
        }
    }

    private void handlePingresp() {
        Timber.v("Received PINGRESP");
    }

    private void handleConnack(byte returnCode) {
        switch (returnCode) {
            case 0:
                Timber.d("Connection accepted by broker");
                uMQTTController.getInstance().connectionEstablished();
                break;
            case 1:
                Timber.d("Connection refused: unacceptable protocol version");
                break;
            case 2:
                Timber.d("Connection refused: identifier rejected");
                break;
            case 3:
                Timber.d("Connection refused: server unavailable");
                break;
            case 4:
                Timber.d("Connection refused: bad username or password");
                break;
            case 5:
                Timber.d("Connection refused: unauthorized");
                break;
        }
    }

    private void handleSuback(byte[] message, int size) {
        short packetId = (short)uMQTTFrame.fetchBytes(message[2], message[3]);

        uMQTTController.getInstance()
                .setResponseToAwaitingSubscriptions(
                        packetId,
                        Arrays.copyOfRange(message, 4, size));
    }
}
