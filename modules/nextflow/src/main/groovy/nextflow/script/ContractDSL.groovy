package nextflow.script

import groovy.util.logging.Slf4j

@Slf4j
class ContractDSL {
    private class Conditional {
        String command

        Conditional(String command){
            this.command = command
        }
    }

    private class Numeric {
        String command
        private boolean isLiteral

        Numeric(String command) {
            this.command = command
        }

        Numeric(String command, boolean isLiteral){
            this.command = command
            this.isLiteral = isLiteral
        }

        String render(){
            return command.isNumber() || isLiteral ? command : "`$command`"
        }
    }

    private class Iterable {
        String command

        Iterable(String command){
            this.command = command
        }
    }

    Conditional EMPTY_FILE( String file ) {
    	return new Conditional("! test -s $file")
    }
    
    String IF_THEN(Conditional condition, String body){
        return "if ${condition.command}; then ${body}; fi"
    }

    Iterable ITER(String command){
        return new Iterable(command)
    }

    Iterable RANGE(long min, long max){
        return new Iterable("{${min}..${max}}")
    }

    String FOR_ALL(String iterator, Iterable toIterate, Closure<String> body){
        return "for $iterator in ${toIterate.command}; do ${body.call('$' + iterator)}; done"
    }
    
    String IF_THEN_ELSE(Conditional condition, String then_body, String else_body){
        return "if ${condition.command}; then ${then_body}; else ${else_body}; fi"
    }

    String RETURN(Conditional condition){
        return "if ${condition.command}; then exit 0; else exit 1; fi"
    }

    Conditional NOT(Conditional condition) {
        return new Conditional("! ${condition.command}")
    }

    Conditional AND(Conditional a, Conditional b) {
        return new Conditional("${a.command} && ${b.command}")
    }

    Conditional OR(Conditional a, Conditional b) {
        return new Conditional("${a.command} || ${b.command}")
    }

    Conditional INPUT_NOT_CHANGED(String file) {
        return new Conditional("diff -r $file backup$file > /dev/null 2> /dev/null")
    }

    Numeric CPUS(){
        return new Numeric("grep -c ^processor /proc/cpuinfo")
    }

    Numeric NUM(String number){
        return new Numeric(number, true);
    }

    Numeric NUM(long number){
        return new Numeric(number as String);
    }

    Numeric COUNT_PATTERN(String file, String pattern){
        return new Numeric("grep -cE \"$pattern\" $file")
    }

    Conditional GREATER_THAN(Numeric a, Numeric b){
        return new Conditional("[ ${a.render()} -gt ${b.render()} ]")
    }

    Conditional LESS_THAN(Numeric a, Numeric b){
        return new Conditional("[ ${a.render()} -lt ${b.render()} ]")
    }

    Conditional GREATER_EQUAL(Numeric a, Numeric b){
        return new Conditional("[ ${a.render()} -ge ${b.render()} ]")
    }

    Conditional LESS_EQUAL(Numeric a, Numeric b){
        return new Conditional("[ ${a.render()} -le ${b.render()} ]")
    }

    Conditional EQUAL(Numeric a, Numeric b){
        return new Conditional("[ ${a.render()} -eq ${b.render()} ]")
    }

    Conditional COND(String command) {
        return new Conditional(command)
    }

    Conditional TRUE = new Conditional("true")
    
    Conditional FALSE = new Conditional("false")

    String ALL_FILES_NON_EMPTY(String files){
        return FOR_ALL("f", ITER(files), { String f -> RETURN(EMPTY_FILE(f))})
    }

    String COMMAND_LOGGED_NO_ERROR = RETURN(EQUAL(COUNT_CASE_INSENSITIVE_PATTERN(".commmand.err", "error"), NUM(0)))
}
