package nextflow.script

class ContractDSL {
    private class Conditional {
        String command

        Conditional(String command){
            this.command = command
        }
    }

    private class Numeric {
        String command

        Numeric(String command) {
            this.command = command
        }

        String render(){
            return command.isNumber() ? command : "`$command`"
        }
    }

    Conditional EMPTY_FILE( String file ) {
    	return new Conditional("! test -s $file")
    }
    
    String IF_THEN(Conditional condition, String body){
        return "if ${condition.command}; then ${body}; fi"
    }
    
    String FOR_ALL(String iterator, String toIterate, Closure<String> body){
        return "for $iterator in ${toIterate}; do ${body.call('$' + iterator)}; done"
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

    Numeric NUM(long number){
        return new Numeric("$number");
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

}
