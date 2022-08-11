package nextflow.dag


import nextflow.Global
import nextflow.Session
import nextflow.exception.ProcessFailedException
import nextflow.verification.ModelChecker
import nextflow.verification.SimpleConditionChecker

import java.nio.file.Path
import java.time.LocalDateTime

class RuntimeVerifier {
    class FormulaViolatedError extends Error{
        FormulaViolatedError(String reason){
            super(reason)
        }
    }
    class Model {
        private class Node {
            String label
            List<Character> events
            LocalDateTime time
            Node(String label, List<Character> events, LocalDateTime time){
                this.events = events
                this.label = label
                this.time = time
            }
        }
        private class Edge {
            String label
            Node origin
            Node target
            Edge(String label, Node target, Node origin){
                this.label = label
                this.target = target
                this.origin = origin
            }
        }
        List<Node> nodes = []
        List<Edge> edges = []
        List<Node> trace = []

        void addNode(String label){
            if (label.endsWith("Pre") || label.endsWith("Post")){
                throw new IllegalArgumentException("Due to the way runtime verification works we can not permit processes with labels ending in 'Pre' or 'Post'")
            }
            Node preNode = new Node(label + "Pre", null, null)
            Node postNode = new Node(label + "Post", null, null)
            nodes.add(preNode)
            nodes.add(postNode)
            trace.add(preNode)
            trace.add(postNode)
            edges.add(new Edge(label, preNode, postNode))
        }

        void addEvents(String label, char[] events, LocalDateTime time) {
            Node node = nodes.find { (it.label == label) }
            node.events = events.findAll {it != "\n"}
            node.time = time
        }

        void addEdge(String label, String origin, String target){
            edges.add(new Edge(label,
                    nodes.find { (it.label == origin + "Post") },
                    nodes.find { (it.label == target + "Pre") }))
        }
    }

    ModelChecker checker
    List<String> formulas
    boolean initialized
    Model model

    RuntimeVerifier(){
        Path formulaFile = Path.of("./formulas.rv")
        if (formulaFile.exists() && formulaFile.isFile()) {
            formulas = formulaFile.toFile().getText().split("@@@").findAll { it != ""}
        } else {
            formulas = []
        }
        checker = new SimpleConditionChecker()
    }

    void addEvents(String label, char[] events, LocalDateTime time){
        initialize()
        model.addEvents(label, events, time)
        def allFailedFormulas = formulas.findAll {!checker.checkModel(it, model)}
        if (allFailedFormulas){
            throw new FormulaViolatedError("Process violated formulas: ${allFailedFormulas}, at state $label, with events ${events ?: []}")
        }
    }

    private void initialize(){
        if (!Global.session instanceof Session || initialized) {
            return
        }
        Session session = (Session) Global.session
        DAG processingDag = session.getDag()
        model = new Model()
        processingDag.getVertices().findAll {
            it.type == DAG.Type.PROCESS
        }.each {
            model.addNode(it.label)
        }

        List<DAG.Edge> dagEdges = processingDag.getEdges()
        model.nodes.each {node ->
            def edges = dagEdges.findAll { dagEdge ->
                dagEdge.from != null && dagEdge.from.label + "Post" == node.label
            }
            def done = false
            Set <String> targets = []
            while (!done) {
                if (!edges.isEmpty()) {
                    List<DAG.Vertex> toNodes = edges.to
                    targets += toNodes.findAll {it != null && it.type == DAG.Type.PROCESS }.label
                    def invalidNodes = toNodes.findAll {it != null && it.type != DAG.Type.PROCESS }
                    edges = dagEdges.findAll { it.from in invalidNodes }
                } else {
                    done = true
                }
            }
            targets.each {
                model.addEdge(node.label - "Post" + (String) it, node.label - "Post", (String) it)
            }
        }
        initialized = true
    }
}
