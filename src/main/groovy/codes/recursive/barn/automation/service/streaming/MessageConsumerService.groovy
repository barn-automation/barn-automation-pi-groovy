package codes.recursive.barn.automation.service.streaming

import codes.recursive.barn.automation.camera.CameraService
import codes.recursive.barn.automation.model.ArduinoMessage
import codes.recursive.barn.automation.service.arduino.ArduinoService
import com.oracle.bmc.auth.ConfigFileAuthenticationDetailsProvider
import com.oracle.bmc.streaming.StreamClient
import com.oracle.bmc.streaming.model.CreateGroupCursorDetails
import com.oracle.bmc.streaming.model.Message
import com.oracle.bmc.streaming.requests.CreateGroupCursorRequest
import com.oracle.bmc.streaming.requests.GetMessagesRequest
import groovy.json.JsonException
import groovy.json.JsonSlurper
import groovy.util.logging.Log4j

import java.util.concurrent.atomic.AtomicBoolean

@Log4j
class MessageConsumerService {
    CameraService cameraService
    ArduinoService arduinoService
    String configFilePath
    String streamId
    String groupName = 'group-0'
    StreamClient client
    private final AtomicBoolean closed = new AtomicBoolean(false)

    MessageConsumerService(configFilePath, streamId, arduinoService, cameraService) {
        this.configFilePath = configFilePath
        this.streamId = streamId
        def provider =  new ConfigFileAuthenticationDetailsProvider(this.configFilePath, 'DEFAULT')
        def client = new StreamClient(provider)
        client.setRegion('us-phoenix-1')
        this.client = client
        this.arduinoService = arduinoService
        this.cameraService = cameraService
    }

    def start() {

        def cursorDetails = CreateGroupCursorDetails.builder()
                .type(CreateGroupCursorDetails.Type.TrimHorizon)
                .commitOnGet(true)
                .groupName(this.groupName)
                .build()
        def groupCursorRequest = CreateGroupCursorRequest.builder()
                .streamId(streamId)
                .createGroupCursorDetails(cursorDetails)
                .build()

        def cursorResponse = this.client.createGroupCursor(groupCursorRequest)

        def getRequest = GetMessagesRequest.builder()
                .cursor(cursorResponse.cursor.value)
                .streamId(this.streamId)
                .build()

        while(!closed.get()) {
            def getResult = this.client.getMessages(getRequest)
            getResult.items.each { Message record ->
                def msg
                try {
                    msg = new JsonSlurper().parseText( new String(record.value, "UTF-8") )
                    arduinoService.send( new ArduinoMessage(msg?.type, msg?.message) )
                    // do we need to take any other actions?

                    switch( msg?.type ) {
                        case ArduinoMessage.CAMERA_0:
                            if( msg?.message == ArduinoMessage.ON ) {
                                cameraService.snapStoreBroadcast()
                            }
                            break
                    }
                }
                catch (JsonException e) {
                    log.error("Error parsing JSON from ${record.value}")
                    e.printStackTrace()
                }
                catch (Exception e) {
                    log.error("Error:")
                    e.printStackTrace()
                }
            }
            getRequest.cursor = getResult.opcNextCursor
            sleep(500)
        }
    }

    def close() {
        log.info("Closing cursor...")
        closed.set(true)
    }
}
