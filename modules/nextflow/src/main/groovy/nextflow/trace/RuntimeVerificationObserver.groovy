package nextflow.trace

import groovy.util.logging.Slf4j
import nextflow.Global
import nextflow.Session
import nextflow.processor.TaskProcessor

import java.nio.file.Path
import java.time.LocalDateTime
import java.time.ZoneOffset

@Slf4j
class RuntimeVerificationObserver implements TraceObserver{
    void onFilePublish(Path destination){
        if (Global.session instanceof Session){
            String suffix
            if (destination.fileName.toString().endsWith(".preEmit.state")){
                suffix = "Pre"
            } else if (destination.fileName.toString().endsWith(".postEmit.state")){
                suffix = "Post"
            } else {
                return
            }
            def name = destination.baseName.split("\\.")[1]
            ((Session) Global.session).runtimeVerifier.addEvents(name + suffix, destination.toFile().getText().toCharArray(), LocalDateTime.ofEpochSecond((long) (destination.lastModified() / 1000), 0, ZoneOffset.UTC))
        }
    }
}
