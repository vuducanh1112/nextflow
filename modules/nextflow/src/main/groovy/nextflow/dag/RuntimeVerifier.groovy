package nextflow.dag

import groovy.transform.MapConstructor
import nextflow.Global
import nextflow.Session
import nextflow.dag.DAG

import java.nio.file.Path

class RuntimeVerifier {
    private class Model {
        @MapConstructor
        private class Node {
            String label
            List<Character> events
        }
        @MapConstructor
        private class Edge {
            String label
            Node origin
            Node target
        }
        List<Node> nodes = []
        List<Edge> edges = []

        void addNode(String label){
            Node preNode = new Node(label: label + "Pre", events: [])
            Node postNode = new Node(label: label + "Post", events: [])
            nodes.add(preNode)
            nodes.add(postNode)
            edges.add(new Edge(label: label, origin: preNode, target: postNode))
        }

        void addEvents(String label, char[] events) {
            Node node = nodes.find { (it.label == label) }
            events.findAll {it != "\n"}.each {node.events.add((char) it)}
        }

        void addEdge(String label, String origin, String target){
            edges.add(new Edge(label: label,
                    origin: nodes.find { (it.label == origin + "Post") },
                    target: nodes.find { (it.label == target + "Pre") }))
        }
    }

    List<String> formulas
    boolean initialized
    Model model

    RuntimeVerifier(){
        Path formulaFile = Path.of("./formulas.rv")
        if (formulaFile.exists() && formulaFile.isFile()) {
            formulaFile.toFile().getText() // TODO finish formula extraction
        }
    }

    void addEvents(String label, char[] events){
        initialize()
        model.addEvents(label, events)
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
                dagEdge.from.label + "Post" == node.label
            }
            def done = false
            Set <String> targets = []
            while (!done) {
                if (!edges.isEmpty()) {
                    List<DAG.Vertex> toNodes = edges.to
                    targets += toNodes.findAll { it.type == DAG.Type.PROCESS }.label
                    def invalidNodes = toNodes.findAll { it.type != DAG.Type.PROCESS }
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
