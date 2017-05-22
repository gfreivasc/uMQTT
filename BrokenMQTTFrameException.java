package re.usto.umqtt;

/**
 * Malformed packets (missing information).
 */

class BrokenMQTTFrameException extends Exception {
    BrokenMQTTFrameException(String message) {
        super(message);
    }
}
