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
    private static int readStepSize = BYTES_SINGLE_STEP;
    private static int remainingSize = 0;
    private static int readOffset = 0;
    private static int multiplier = 1;
    private boolean mWaitingConnack = true;

    private static final int
            RS_AWAITING = 0,
            RS_FETCHING_REMAINING_SIZE = 1,
            RS_FETCHING_FULL_PACKET = 2;

    @Retention(RetentionPolicy.SOURCE)
    @IntDef({RS_AWAITING, RS_FETCHING_REMAINING_SIZE, RS_FETCHING_FULL_PACKET})
    private @interface MQReadState {
    }

    private @MQReadState
    int mReadState;

    // Size = 16MB
    private static final int INPUT_BUFFER_LENGTH = 16 * 1024 * 1024;
    private static byte[] buffer = new byte[INPUT_BUFFER_LENGTH];

    private uMQTTInputService() {
        mWaitingConnack = true;
        setListenerAwaiting();
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
                        parseSocketInput(buffer, readSize);
                    else if (readSize == -1 && mRun) {
                        throw new IOException("Connection closed by broker.");
                    }
                }
            } catch (IOException e) {
                if (mRun) {
                    stop();
                    uMQTT.getInstance().close();
                    uMQTT.getInstance().open();
                }
                else stop();
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
        mWaitingConnack = true;
        mTCPListenerThread = new Thread(mListener);
        mTCPListenerThread.start();
        setListenerAwaiting();
        uMQTT.getInstance().establishConnection();
    }

    void stop() {
        mRun = false;
        mInputStream = null;
    }

    @WorkerThread
    private synchronized void setListenerAwaiting() {
        mReadState = RS_AWAITING;
        readOffset = 0;
        remainingSize = 0;
        multiplier = 1;
        readStepSize = BYTES_SINGLE_STEP;
    }

    @WorkerThread
    private synchronized void setListenerFetchingRemainingSize() {
        mReadState = RS_FETCHING_REMAINING_SIZE;
        readOffset += BYTES_SINGLE_STEP;
        remainingSize = 0;
    }

    private synchronized void setListenerFetchingFullPacket() {
        mReadState = RS_FETCHING_FULL_PACKET;
        readStepSize = remainingSize;
    }

    @WorkerThread
    private synchronized void parseSocketInput(byte[] excerpt, int readSize) {
        switch (mReadState) {
            case RS_AWAITING:
                @uMQTTFrame.MQPacketType int type = (excerpt[0] >> 4) & 0xf;
                if (mWaitingConnack && type != uMQTTFrame.MQ_CONNACK) {
                    break;
                }
                else if (mWaitingConnack) mWaitingConnack = false;
                setListenerFetchingRemainingSize();
                break;
            case RS_FETCHING_REMAINING_SIZE:
                int digit = excerpt[readOffset];
                remainingSize += (digit & 0x7f) * multiplier;
                multiplier *= 0x80;
                readOffset += BYTES_SINGLE_STEP;
                if ((digit & 0x80) == 0) {
                    if (remainingSize == 0) {
                        onMessageReceived(excerpt, BYTES_FIXED_HEADER);
                        setListenerAwaiting();
                    }
                    else setListenerFetchingFullPacket();
                }
                break;
            case RS_FETCHING_FULL_PACKET:
                if (readSize < readStepSize) {
                    readStepSize -= readSize;
                    readOffset += readSize;
                }
                else {
                    if (readSize == readStepSize) {
                        onMessageReceived(excerpt, readOffset + readStepSize);
                    }
                    setListenerAwaiting();
                }
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
                handleOutboundQoS(message);
                break;
            case uMQTTFrame.MQ_PUBLISH:
            case uMQTTFrame.MQ_PUBREL:
                handleInboundQoS(type, message);
                break;
            default:
                Timber.wtf("Unexpected packet type (#%d) received", type);
                break;
        }
    }

    private void handleUnsuback(byte msb, byte lsb) {
        short packetId = uMQTTFrame.fetchBytes(msb, lsb);
        uMQTT.getInstance().removeSubscriptions(packetId);
    }

    private void handleOutboundQoS(byte[] message) {
        short packetId = uMQTTFrame.fetchBytes(message[2], message[3]);
        uMQTT.getInstance().advanceOutboundTransaction(packetId);
    }

    private void handleInboundQoS(int type, byte[] message) {
        if (type == uMQTTFrame.MQ_PUBLISH) {
            uMQTTPublish publish = new uMQTTPublish(message);

            uMQTT.getInstance().advanceInboundTransaction(
                    publish
            );
        }
        else {
            short packetId = uMQTTFrame.fetchBytes(message[2], message[3]);
            uMQTT.getInstance().advanceInboundTransaction(packetId);
        }
    }

    private void handlePingresp() {
        Timber.v("Received PINGRESP");
    }

    private void handleConnack(byte returnCode) {
        switch (returnCode) {
            case 0:
                Timber.d("Connection accepted by broker");
                uMQTT.getInstance().connectionEstablished();
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
            default:
                Timber.w("Unsupported connection response. Ignoring");
                mWaitingConnack = true;
                break;
        }
    }

    private void handleSuback(byte[] message, int size) {
        short packetId = (short)uMQTTFrame.fetchBytes(message[2], message[3]);

        uMQTT.getInstance()
                .setResponseToAwaitingSubscriptions(
                        packetId,
                        Arrays.copyOfRange(message, 4, size));
    }
}
