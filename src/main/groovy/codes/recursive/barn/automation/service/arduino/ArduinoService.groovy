package codes.recursive.barn.automation.service.arduino

import codes.recursive.barn.automation.model.ArduinoMessage
import codes.recursive.barn.automation.service.streaming.MessageProducerService
import com.fazecast.jSerialComm.SerialPort
import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import groovy.util.logging.Log4j

@Log4j
class ArduinoService {

    MessageProducerService messageProducerService
    SerialPort comPort
    Boolean debug = false

    ArduinoService(messageProducerService, debug) {
        this.messageProducerService = messageProducerService
        SerialPort.getCommPorts().each {
            if( it.systemPortName == 'ttyACM0' ) {
                comPort = it
            }
        }
        if( !comPort ) {
            throw new Exception("Serial port error!  Did not find a suitable serial port.")
        }
        comPort.openPort()
        this.debug = debug
    }

    def listen() {
        try {
            while (true) {
                while (comPort.bytesAvailable() <= 0) {
                    sleep(500)
                }
                byte[] readBuffer = new byte[comPort.bytesAvailable()]
                int numRead = comPort.readBytes(readBuffer, readBuffer.length)
                def incomingMessage = new String(readBuffer)
                if (debug) {
                    log.info("Received from Arduino: ${incomingMessage}")
                }
                try {
                    /*
                        we want to make sure we're sending a valid JSON object
                        because the serial read may have missed some bits
                        so, first parse it, then stringify it
                    */
                    def parsed = new JsonSlurper().parseText(incomingMessage)
                    if (debug) {
                        println parsed
                    }
                    if (parsed.status == "OK") {
                        parsed.messages.each {
                            messageProducerService.send(JsonOutput.toJson(it))
                            log.info("Publishing new message...")
                        }
                    }
                }
                catch (e) {
                    log.warn("Could not parse incoming message...Not publishing anything")
                }
            }
        }
        catch (Exception e) {
            log.error("Exception", e)
        }
    }

    def send(ArduinoMessage message) {
        def msg = JsonOutput.toJson(message)
        log.info("Sending message to Arduino: ${message}")
        return comPort.writeBytes(msg.bytes, msg.bytes.length)
    }

    def close() {
        log.info("Shutting down serial port connection...")
        comPort.closePort()
    }

}
