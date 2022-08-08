package nextflow.script

class ContractDSL {
    private class Conditional {
        String command

        Conditional(String command){
            this.command = command
        }
    }

    Conditional EMPTY_FILE( String file ) {
    	return new Conditional("! test -s $file")
    }
    
    String IF_THEN(Conditional condition, String body){
        return "if ${condition.command}; then ${body}; fi"
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

    Conditional TRUE = new Conditional("true")
    
    Conditional FALSE = new Conditional("false")

}
