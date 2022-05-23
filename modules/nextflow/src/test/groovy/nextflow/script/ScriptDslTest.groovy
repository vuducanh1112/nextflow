package nextflow.script

import groovyx.gpars.dataflow.operator.PoisonPill
import nextflow.Channel
import nextflow.exception.AbortRunException
import nextflow.exception.MissingProcessException
import nextflow.exception.ScriptCompilationException
import nextflow.exception.ScriptRuntimeException
import nextflow.util.NullablePath
import spock.lang.Ignore
import test.Dsl2Spec
import test.MockScriptRunner
/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
class ScriptDslTest extends Dsl2Spec {


    def 'should define a process with output alias' () {
        given:
        def SCRIPT = '''
         
        process foo {
          output: val x, emit: 'ch1'
          output: val y, emit: 'ch2' 
          exec: x = 'Hello'; y = 'world'
        }
       
        workflow {
            main: 
                foo()
            emit: 
                foo.out.ch1
                foo.out.ch2
        }
        '''

        when:
        def runner = new MockScriptRunner()
        def result = runner.setScript(SCRIPT).execute()
        then:
        result[0].val == 'Hello'
        result[1].val == 'world'
    }

    def 'should execute basic workflow' () {
        when:
        def result = dsl_eval '''
   
        workflow {
            emit: result 
            main:
            result = 'Hello world'
        }
        '''

        then:
        result.val == 'Hello world'
    }

    def 'should execute emit' () {
        when:
        def result = dsl_eval '''
   
        workflow {
            emit:
            result = 'Hello world'
        }
        '''

        then:
        result.val == 'Hello world'
    }

    def 'should emit expression' () {
        when:
        def result = dsl_eval '''
         
        def foo() { 'Hello world' } 
       
        workflow {
            emit:
            foo().toUpperCase()
        }
        '''

        then:
        result.val == 'HELLO WORLD'
    }

    def 'should emit process out' () {
        when:
        def result = dsl_eval '''
         
        process foo {
          output: val x 
          exec: x = 'Hello'
        }
       
        workflow {
            main: foo()
            emit: foo.out
        }
        '''

        then:
        result.val == 'Hello'
    }


    def 'should define processes and workflow' () {
        when:
        def result = dsl_eval '''
        process foo {
          input: val data 
          output: val result
          exec:
            result = "$data mundo"
        }     
        
        process bar {
            input: val data 
            output: val result
            exec: 
              result = data.toUpperCase()
        }   
        
        workflow alpha {
            take: 
                data
            
            main:
                foo(data)
                bar(foo.out)
                
            emit: 
                x = bar.out 
            
        }
   
        workflow {
            main: alpha('Hello') 
            emit: x = alpha.out 
        }
        '''

        then:
        result.val == 'HELLO MUNDO'
    }


    def 'should access nextflow enabling property' () {
        when:
        def result = dsl_eval '''
        return nextflow.enable.dsl 
        '''

        then:
        result == 2
    }


    def 'should not allow function with reserved identifier' () {

        when:
        dsl_eval """ 
            def main() { println 'ciao' }
        """

        then:
        def err = thrown(ScriptCompilationException)
        err.message.contains('Identifier `main` is reserved for internal use')
    }

    def 'should not allow process with reserved identifier' () {

        when:
        dsl_eval """ 
            process main {
              /echo ciao/
            }
        """

        then:
        def err = thrown(ScriptCompilationException)
        err.message.contains('Identifier `main` is reserved for internal use')
    }

    def 'should not allow workflow with reserved identifier' () {

        when:
        dsl_eval """ 
            workflow main {
              /echo ciao/
            }
        """
        then:
        def err = thrown(ScriptCompilationException)
        err.message.contains('Identifier `main` is reserved for internal use')
    }

    def 'should not allow duplicate workflow keyword' () {
        when:
        dsl_eval(
                """ 
                workflow {
                  /echo ciao/
                }
                
                workflow {
                  /echo miao/
                }
            """
        )
        then:
        def err = thrown(ScriptCompilationException)
        err.message.contains('Duplicate entry workflow definition')
    }

    def 'should apply operator to process result' () {
        when:
        def result = dsl_eval(/
            process hello {
              output: val result
              exec:
                result = "Hello"
            }     
            
            workflow {
               main: hello()
               emit: hello.out.map { it.toUpperCase()  }
            }
        /)
        then:
        result.val == 'HELLO'
    }

    def 'should branch and view' () {

        when:
        def result = dsl_eval(/
            Channel
                .from(1,2,3,40,50)
                .branch { 
                    small: it < 10 
                    large: it > 10  
                }
                .set { result }
                
             ch1 = result.small.map { it }
             ch2 = result.large.map { it }  

             [ch1, ch2]
        /)
        then:
        result[0].val == 1
        result[0].val == 2
        result[0].val == 3
        and:
        result[1].val == 40
        result[1].val == 50
    }


    def 'should allow pipe process and operator' () {
        when:
        def result = dsl_eval('''
        process foo {
          output: val result
          exec: result = "hello"
        }     
 
        process bar {
          output: val result
          exec: result = "world"
        } 
        
        workflow {
           emit: (foo & bar) | concat      
        }
        ''')

        then:
        result.val == 'hello'
        result.val == 'world'
        result.val == Channel.STOP
    }

    def 'should allow process and operator composition' () {
        when:
        def result = dsl_eval('''
        process foo {
          output: val result
          exec: result = "hello"
        }     
 
        process bar {
          output: val result
          exec: result = "world"
        } 
        
        workflow {
           main: foo(); bar()
           emit: foo.out.concat(bar.out)      
        }
        ''')


        then:
        result.val == 'hello'
        result.val == 'world'
        result.val == Channel.STOP
    }

    def 'should run entry flow' () {
        when:
        def result = dsl_eval('TEST_FLOW', '''
        process foo {
          output: val result
          exec: result = "hello"
        }     
 
        process bar {
          output: val result
          exec: result = "world"
        } 
        
        workflow {
           main: foo()
           emit: foo.out  
        }
    
        workflow TEST_FLOW {
           main: bar()
           emit: bar.out  
        }
        ''')


        then:
        result.val == 'world'
        
    }

    def 'should not allow composition' () {
        when:
        dsl_eval('''
        process foo {
          /echo foo/
        }
        
        process bar {
          input: val x 
          /echo bar $x/
        }
        
        workflow {
          bar(foo())
        }
        ''')


        then:
        def err = thrown(ScriptRuntimeException)
        err.message == 'Process `bar` declares 1 input channel but 0 were specified'
    }

    def 'should report error accessing undefined out/a' () {
        when:
        dsl_eval('''
        process foo {
          /echo foo/
        }
        
        process bar {
          input: val x 
          /echo bar $x/
        }
        
        workflow {
          bar(foo.out)
        }
        ''')

        then:
        def err = thrown(ScriptRuntimeException)
        err.message == "Access to 'foo.out' is undefined since the process 'foo' has not been invoked before accessing the output attribute"
    }

    def 'should report error accessing undefined out/b' () {
        when:
        dsl_eval('''
        process foo {
          /echo foo/
        }
        
        process bar {
          input: val x 
          /echo bar $x/
        }
        
        workflow {
          bar(foo.out)
        }
        ''')

        then:
        def err = thrown(ScriptRuntimeException)
        err.message == "Access to 'foo.out' is undefined since the process 'foo' has not been invoked before accessing the output attribute"
    }

    def 'should report error accessing undefined out/c' () {
        when:
        dsl_eval('''
        process foo {
          /echo foo/
        }
        
        workflow flow1 {
            foo()
        }
        
        workflow {
          flow1()
          flow1.out.view()
        }
        ''')

        then:
        def err = thrown(ScriptRuntimeException)
        err.message == "Access to 'flow1.out' is undefined since the workflow 'flow1' doesn't declare any output"
    }

    def 'should report error accessing undefined out/d' () {
        when:
        dsl_eval('''
        process foo {
          /echo foo/
        }
        
        process bar {
          input: val x 
          /echo bar $x/
        }
        
        workflow flow1 {
            foo()
        }
        
        workflow {
          flow1 | bar
        }
        ''')

        then:
        def err = thrown(ScriptRuntimeException)
        err.message == "Process `bar` declares 1 input channel but 0 were specified"
    }

    def 'should report error accessing undefined out/e' () {
        when:
        dsl_eval('''
        process foo {
          /echo foo/
        }
        
        workflow flow1 {
            foo()
        }
        
        workflow {
          flow1.out.view()
        }
        ''')

        then:
        def err = thrown(ScriptRuntimeException)
        err.message == "Access to 'flow1.out' is undefined since the workflow 'flow1' has not been invoked before accessing the output attribute"
    }

    def 'should report unsupported error' () {
        when:
        dsl_eval('''
        process foo {
          /echo foo/
        }
        
        workflow {
          get: 
            x
          main: 
          flow()
        }
        ''')

        then:
        def err = thrown(ScriptCompilationException)
        err.message.contains "Workflow 'get' is not supported anymore use 'take' instead"
    }

    def 'should fail with wrong scope'() {
        when:
        dsl_eval('''\
        process foo {
          /echo foo/
        }
        
        workflow {
          main: 
          flow()
          emmit:
          flow.out
        }
        ''')

        then:
        def err = thrown(ScriptCompilationException)
        err.message.contains "Unknown execution scope 'emmit:' -- Did you mean 'emit'"
    }


    def 'should fail because process is not defined'() {
        when:
        dsl_eval(
        '''
        process sleeper {
            exec:
            """
            sleep 5
            """
        }
        
        workflow {
            main:
                sleeper()
                hello()      
        }
        
        ''')

        then:
        def err = thrown(MissingProcessException)
        err.message == "Missing process or function with name 'hello'"
    }


    def 'should fail because is not defined /2' () {
        when:
        dsl_eval('''
        process sleeper {
            exec:
            """
            sleep 5
            """
        }
        
        workflow nested {
            main:
                sleeper()
                sleeper_2()      
        }
        
        workflow{
            nested()
        }
        ''')

        then:
        def err = thrown(MissingProcessException)
        err.message == "Missing process or function with name 'sleeper_2' -- Did you mean 'sleeper' instead?"
    }

    def 'should fail because is not defined /3' () {
        when:
        dsl_eval('''
        process sleeper1 {
            /echo 1/
        }
        
        process sleeper2 {
            /echo 3/
        }

        
        workflow nested {
            main:
                sleeper1()
                sleeper3()      
        }
        
        workflow{
            nested()
        }
        ''')

        then:
        def err = thrown(MissingProcessException)
        err.message ==  '''\
                        Missing process or function with name 'sleeper3'
                        
                        Did you mean any of these instead?
                          sleeper1
                          sleeper2
                        '''.stripIndent()
    }

    def 'should not conflict with private meta attribute' () {
        when:
        def result = dsl_eval '''
         
        process foo {
          input: val x
          output: val y 
          exec: y = x
        }
       
        workflow {
            main: 
              meta = channel.of('Hello')
              foo(meta)
            emit: 
              foo.out
        }
        '''

        then:
        result.val == 'Hello'
    }

    def 'should go fine if allowNull is allowed as output and input'() {

        when:
        def result = dsl_eval '''
            nextflow.enable.dsl=2
            process test_process1 {
              input:
                val id
              output:
                path("output.txt", allowNull:true)
              script:
                "echo hi"
            }

            process test_process2 {
              input:
                path(file, allowNull:true)
              output:
                val 'done'
              script:
                "echo hello"                 
            }            

            workflow {  
                main:               
                    channel.of('foo') | test_process1 | test_process2
                emit:
                    test_process2.out
            }

        '''
        then:
        result.val == 'done'
    }

    def 'should go fine if allowNull output is allowed'() {

        when:
        def result = dsl_eval '''
            nextflow.enable.dsl=2
            
            process test_process1 {
              input:
                val id
              output:
                path("output.txt", allowNull:true)
              """
                echo hi
              """
            }            
            workflow {
                main: 
                    test_process1('foo')
                emit:
                    test_process1.out          
            }
        '''
        then:
        result.val instanceof NullablePath
    }

    @Ignore
    def 'should fails if allowNull output is not set'() {

        when:
        dsl_eval '''
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
        then:
        thrown(AbortRunException)
    }

    @Ignore
    def 'should fails if allowNull is allowed as output but expected as input'() {

        when:
        def result = dsl_eval '''
            nextflow.enable.dsl=2
            process test_process1 {
              input:
                val id
              output:
                path("output.txt", allowNull:true)
              exec:
                println id
            }

            process test_process2 {
              input:
                path(file)
              output:
                val file
              exec:
                println file                 
            }            

            workflow {                 
                channel.of('foo') | test_process1 | test_process2 |  view()
            }

        '''
        then:
        thrown(AbortRunException)
    }

}
