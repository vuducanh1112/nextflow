package nextflow.script


import nextflow.Session
import spock.lang.IgnoreIf
import test.Dsl2Spec

/**
 *
 * @author Jorge Aguilera <jorge.aguilera@seqera.io>
 */
@IgnoreIf({System.getenv('NXF_INPUTNULLABLE')})
class OutputNullablePathTest extends Dsl2Spec {

    def 'should fails if allowNull output is not set'() {
        given:
        def error = false
        def session = new Session( executor: 'nope' ) {
            @Override
            void abort(Throwable cause) {
                forceTermination()
                error = true
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
                path("output.txt")
              exec:
                println 'hi'        
            }            
            workflow {
                test_process1('foo').out          
            }
        '''
        when:
        runner.setScript(script).execute()

        then:
        error
    }

}
