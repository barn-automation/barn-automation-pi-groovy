package codes.recursive.barn.automation.camera

import codes.recursive.barn.automation.service.gpio.GpioService
import codes.recursive.barn.automation.service.kafka.MessageProducerService
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.s3.model.PutObjectRequest
import com.amazonaws.util.IOUtils
import groovy.json.JsonOutput
import groovy.util.logging.Log4j

@Log4j
class CameraService {

    String captureMethod
    MessageProducerService messageProducerService
    AmazonS3 s3Client
    Map storageConfig
    Map scripts = [
            raspistill: "raspistill -ts -vf -hf -o -", //slower, better quality
            uv4l: "dd if=/dev/video0 bs=11M count=1", //faster, medium quality
    ]

    CameraService(
            MessageProducerService messageProducerService,
            AmazonS3 s3Client,
            Map storageConfig,
            String captureMethod = "raspistill"
    ){
        this.captureMethod = captureMethod
        this.messageProducerService = messageProducerService
        this.s3Client = s3Client
        this.storageConfig = storageConfig

    }
    
    def snapshotToFile() {
        def p = scripts[captureMethod].execute()
        def bis = new BufferedInputStream(p.getInputStream())
        def imagePath = "/tmp/${new Date().time}.jpg"
        def fos = new FileOutputStream(imagePath)

        int read = bis.read()
        fos.write(read)

        while (read != -1)
        {
            read = bis.read()
            fos.write(read)
        }
        bis.close()
        fos.close()

        return imagePath
    }

    InputStream snapshot() {
        def p = scripts[captureMethod].execute()
        return p.getInputStream() as InputStream
    }

    Map store(InputStream imageStream, String metaTitle=null) {
        def key = new Date().time
        def bytes = IOUtils.toByteArray(imageStream)
        ObjectMetadata metadata = new ObjectMetadata()
        metadata.setContentLength(bytes.length)
        metadata.setContentType("image/jpeg")
        if( metaTitle ) metadata.addUserMetadata("opc-meta-title", metaTitle)
        PutObjectRequest request = new PutObjectRequest(storageConfig.storageBucket, key.toString(), imageStream, metadata)
        def result = s3Client.putObject(request)
        return [key: key, result: result]
    }

    Map store(File file, String metaTitle=null) {
        def key = new Date().time
        ObjectMetadata metadata = new ObjectMetadata()
        metadata.setContentLength(file.size())
        metadata.setContentType("image/jpeg")
        if( metaTitle ) metadata.addUserMetadata("opc-meta-title", metaTitle)
        PutObjectRequest request = new PutObjectRequest(storageConfig.storageBucket, key.toString(), file)
        request.metadata = metadata
        def result = s3Client.putObject(request)
        return [key: key, result: result]
    }

    void snapStoreBroadcast() {
        log.info("Taking picture and storing it...")
        Thread.start {
            File pic = new File( snapshotToFile() )
            Map storeResult = store( pic )
            //log.info("Picture stored...")
            messageProducerService.send( JsonOutput.toJson([
                    type: "CAMERA_0",
                    data: [
                            takenAt: new Date(),
                            key: storeResult.key
                    ]
            ]) )
            pic.delete()
            //log.info("Message broadcast...")
        }
    }

}
