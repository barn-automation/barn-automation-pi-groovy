package codes.recursive.barn.automation

import codes.recursive.barn.automation.camera.CameraService
import codes.recursive.barn.automation.service.arduino.ArduinoService
import codes.recursive.barn.automation.service.gpio.GpioService
import codes.recursive.barn.automation.service.streaming.MessageConsumerService
import codes.recursive.barn.automation.service.streaming.MessageProducerService
import com.amazonaws.auth.SystemPropertiesCredentialsProvider
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.pi4j.io.gpio.PinState
import com.pi4j.io.gpio.event.GpioPinDigitalStateChangeEvent
import com.pi4j.io.gpio.event.GpioPinListenerDigital
import groovy.util.logging.Log4j
import org.apache.log4j.PropertyConfigurator

@Log4j
class Main {

    static void main( String[] args ) {
        /* configure logging*/
        def properties = new Properties()
        def props = Main.class.getClassLoader().getResourceAsStream( 'log/log4j.properties' )
        properties.load(props)
        PropertyConfigurator.configure(properties)

        /* get properties */
        def gpioEnabled = System.getProperty("gpioEnabled", "true").toBoolean()
        def debugArduinoSerial = System.getProperty("debugArduinoSerial", "false").toBoolean()
        def inTopicName = System.getProperty("inTopicName", "incoming")
        def outTopicName = System.getProperty("outTopicName", "outgoing")
        def kafkaOutgoingBootstrapServer = System.getProperty("kafkaOutgoingBootstrapServer", "129.146.79.59:6667")
        def kafkaIncomingBootstrapServer = System.getProperty("kafkaIncomingBootstrapServer", "129.146.79.59:6667")
        def accessToken = System.getProperty("aws.accessKeyId", null)
        def secretKey = System.getProperty("aws.secretKey", null)
        def storageRegion = System.getProperty("storageRegion", "us-phoenix-1")
        def storageTenancy = System.getProperty("storageTenancy", "toddrsharp")
        def storageUrl = System.getProperty("storageUrl", "${storageTenancy}.compat.objectstorage.${storageRegion}.oraclecloud.com")
        def storageBucket = System.getProperty("storageBucket", "barn-captures")

        def incomingStreamId = System.getProperty("incomingStreamId", "")
        def outgoingStreamId = System.getProperty("outgoingStreamId", "")
        def ociConfigPath = System.getProperty("ociConfigPath", "")

        def storageConfig = [
                accessToken: accessToken,
                secretKey: secretKey,
                storageRegion: storageRegion,
                storageTenancy: storageTenancy,
                storageUrl: storageUrl,
                storageBucket: storageBucket,
        ]

        if( !accessToken || !secretKey ) {
            throw new Exception("You must set an 'aws.accessKeyId' and 'aws.secretKey' as System properties!")
        }

        /* setup services */
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                .withCredentials(new SystemPropertiesCredentialsProvider())
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(storageUrl, storageRegion))
                .withPathStyleAccessEnabled(true)
                .build()

        MessageProducerService messageProducerService = new MessageProducerService(ociConfigPath, outgoingStreamId)
        ArduinoService arduinoService = new ArduinoService(messageProducerService, debugArduinoSerial)
        CameraService cameraService = new CameraService(messageProducerService, s3Client, storageConfig)
        MessageConsumerService messageConsumeService = new MessageConsumerService(ociConfigPath, incomingStreamId, arduinoService, cameraService)
        GpioService gpioService = new GpioService(gpioEnabled)


        if( gpioEnabled ) {
            Thread.start {
                log.info("Adding listener...")
                println gpioService.motionSensor
                gpioService.motionSensor.addListener( new GpioPinListenerDigital() {
                    @Override
                    void handleGpioPinDigitalStateChangeEvent(GpioPinDigitalStateChangeEvent event) {
                        def prevMillis = 0
                        if( event.state == PinState.HIGH ) {
                            log.info("Motion detected, take and store snapshot...")
                            cameraService.snapStoreBroadcast()
                        }
                    }
                })
                log.info("Listener added")
                gpioService.start()
            }
        }

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            void run() {
                log.info("Shutting down...")
                arduinoService.close()
                messageConsumeService.close()
                messageProducerService.close()
                gpioService.close()
            }
        })

        Thread.start {
            messageConsumeService.start()
        }

        Thread.start {
            arduinoService.listen()
        }
        def art = """
                             +&-
                           _.-^-._    .--.
                        .-'   _   '-. |__|
                       /     |_|     \\|  |
                      /               \\  |
                     /|     _____     |\\ |
                      |    |==|==|    |  |
  |---|---|---|---|---|    |--|--|    |  |
  |---|---|---|---|---|    |==|==|    |  |
 ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
 """
        println art
        log.info("Started...")

    }

}