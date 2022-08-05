package nextflow.verification

import nextflow.dag.RuntimeVerifier

interface ModelChecker {
    boolean checkModel(String formula, RuntimeVerifier.Model model);
}
