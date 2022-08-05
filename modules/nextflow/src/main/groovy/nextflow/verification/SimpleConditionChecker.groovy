package nextflow.verification

import nextflow.dag.RuntimeVerifier

class SimpleConditionChecker implements ModelChecker{
    private class State {
        String label
        boolean isCompound

        State (String label, boolean isCompound){
            this.label = label
            this.isCompound = isCompound
        }
    }
    private class SimpleCondition {
        State origin;
        char originEvent;
        State target;
        char targetEvent;

        SimpleCondition(State origin, char originEvent, State target, char targetEvent){
            this.origin = origin
            this.originEvent = originEvent
            this.target = target
            this.targetEvent = targetEvent
        }

    }
    final SimpleCondition INVALID = new SimpleCondition(null, (char) '\0', null, (char) '\0');

    @Override
    boolean checkModel(String formula, RuntimeVerifier.Model model) {
        def condition = parse(formula, model)
        if (condition == INVALID){
            //TODO return error
            return false
        }
        //Check if nodes have already been executed
        if ((condition.origin.isCompound
                && (model.nodes.find {it.label == condition.origin.label + "Pre"}.events == null
                || model.nodes.find {it.label == condition.origin.label + "Post"}.events == null))
            || (!condition.origin.isCompound && model.nodes.find {it.label == condition.origin.label}.events == null)){
            return true
        }
        if ((condition.target.isCompound
                && (model.nodes.find {it.label == condition.target.label + "Pre"}.events == null
                || model.nodes.find {it.label == condition.target.label + "Post"}.events == null))
                || (!condition.target.isCompound && model.nodes.find {it.label == condition.target.label}.events == null)){
            return true
        }
        List<Character> originEvents = model.nodes.findAll {(condition.origin.isCompound && (it.label in [condition.origin.label + "Pre", condition.origin.label + "Post"]))
                || (!condition.origin.isCompound && it.label == condition.origin.label)}.events.flatten() as List<Character>
        List<Character> targetEvents = model.nodes.findAll {(condition.target.isCompound && (it.label in [condition.target.label + "Pre", condition.target.label + "Post"]))
                || (!condition.target.isCompound && it.label == condition.target.label)}.events.flatten() as List<Character>
        if (condition.originEvent in originEvents){
            return condition.targetEvent in targetEvents
        }
        return true
    }

    SimpleCondition parse(String formula, RuntimeVerifier.Model model){
        def parts = formula.split("then")
        if (parts.length != 2){
            return INVALID
        }
        def firstPart = parts[0].strip().split("\\.")
        if (!((firstPart[0] in model.nodes.label || firstPart[0] + "Post" in model.nodes.label || firstPart[0] + "Pre" in model.nodes.label)
                && firstPart[1].length() == 1 && Character.isLetter(firstPart[1].charAt(0)))) {
            return INVALID
        }
        def secondPart = parts[1].strip().split("\\.")
        if (!((secondPart[0] in model.nodes.label || secondPart[0] + "Post" in model.nodes.label || secondPart[0] + "Pre" in model.nodes.label)
                && secondPart[1].length() == 1 && Character.isLetter(secondPart[1].charAt(0)))) {
            return INVALID
        }
        return new SimpleCondition(new State(firstPart[0], !firstPart[0].endsWithAny("Pre", "Post")),
                  firstPart[1].charAt(0),
                  new State(secondPart[0], !secondPart[0].endsWithAny("Pre", "Post")),
                  secondPart[1].charAt(0))
    }
}
