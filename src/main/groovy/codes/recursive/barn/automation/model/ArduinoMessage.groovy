package codes.recursive.barn.automation.model

class ArduinoMessage {

    public static int MOTOR_O = 0
    public static int MOTOR_1 = 1
    public static int DOOR_0 = 10
    public static int DOOR_1 = 11
    public static int RELAY_0 = 20
    public static int WATER_0 = 30
    public static int CAMERA_0 = 40
    public static int DOOR_LED_0 = 50
    public static int DOOR_LED_1 = 50
    public static int OPEN = 100
    public static int CLOSE = 101
    public static int ON = 200
    public static int OFF = 201

    int type
    String message

    ArduinoMessage(type, message) {
        this.type = type
        this.message = message
    }

    @Override
    String toString() {
        return "[type: ${type}, message: ${message}]"
    }

}
