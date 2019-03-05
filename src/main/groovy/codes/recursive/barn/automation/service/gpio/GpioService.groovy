package codes.recursive.barn.automation.service.gpio

import com.pi4j.io.gpio.*
import com.pi4j.wiringpi.GpioUtil
import groovy.util.logging.Log4j

@Log4j
class GpioService {

    GpioController gpio
    GpioPinDigitalInput motionSensor
    Boolean isEnabled

    GpioService(enabled) {
        this.isEnabled = enabled
        if( this.isEnabled ) {
            GpioUtil.enableNonPrivilegedAccess()
            gpio = GpioFactory.getInstance()
            /* provision pins */
            motionSensor = gpio.provisionDigitalInputPin(RaspiPin.GPIO_15)
            motionSensor.setShutdownOptions(true)
        }
    }

    def close() {
        if( isEnabled ) {
            log.info("Shutting down GPIO service...")
            gpio.shutdown()
        }
    }

    def start() {
        while(true) {
            Thread.sleep(500)
        }
    }

}