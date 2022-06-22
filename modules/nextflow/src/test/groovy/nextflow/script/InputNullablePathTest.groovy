package nextflow.script

import nextflow.Channel
import nextflow.Session
import nextflow.exception.*
import nextflow.util.NullablePath
import spock.lang.IgnoreIf
import spock.lang.Timeout
import test.BaseSpec
import test.Dsl2Spec
import test.MockScriptRunner

/**
 *
 * @author Jorge Aguilera <jorge.aguilera@seqera.io>
 */
@IgnoreIf({System.getenv('NXF_INPUTNULLABLE')})
class InputNullablePathTest extends Dsl2Spec {

    def 'should fails if nullable is allowed as output but expected as input'() {
        given:
        def session = new Session( executor: 'nope' ) {
            @Override
            void abort(Throwable cause) {
                forceTermination()
            }
        }

        def runner = new TestScriptRunner(session)
        and:
        def script = '''
            nextflow.enable.dsl=2
            process test_process1 {
              input:
                val id
              output:
                path("output.txt", nullable:true)
              exec:
                println id
            }

            process test_process2 {
              input:
                path(file)
              output:
                val file
              exec:
                sleep 1000L
                println file                 
            }            

            workflow {                 
                channel.of('foo') | test_process1 | test_process2 |  view()
            }

        '''
        when:
        runner.setScript(script).execute()

        then:
        session.fault.error instanceof ProcessUnrecoverableException
        session.fault.error.message =~ /Path value cannot be null.*/
    }

}
