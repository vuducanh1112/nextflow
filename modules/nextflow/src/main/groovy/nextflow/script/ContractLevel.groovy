package nextflow.script

import nextflow.Global
import nextflow.Session

enum ContractLevel {
    never, always, debug;

    static ContractLevel getContractLevel(String toTest){
        try {
            return valueOf(toTest.strip().toLowerCase());
        } catch (IllegalArgumentException e) {
            return null
        }
    }

    boolean shouldCheck(){
        if (!Global.session instanceof Session){
            return false
        }
        return this <= ((Session) Global.session).contractLevel
    }
}