/*
 * Copyright 2020-2022, Seqera Labs
 * Copyright 2013-2019, Centre for Genomic Regulation (CRG)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package nextflow.executor

import nextflow.script.ContractLevel

import java.nio.file.FileSystemException
import java.nio.file.Files
import java.nio.file.Path

import groovy.transform.CompileStatic
import groovy.transform.PackageScope
import groovy.util.logging.Slf4j
import nextflow.container.CharliecloudBuilder
import nextflow.container.ContainerBuilder
import nextflow.container.DockerBuilder
import nextflow.container.PodmanBuilder
import nextflow.container.ShifterBuilder
import nextflow.container.SingularityBuilder
import nextflow.container.UdockerBuilder
import nextflow.exception.ProcessException
import nextflow.processor.TaskBean
import nextflow.processor.TaskProcessor
import nextflow.processor.TaskRun
import nextflow.secret.SecretsLoader
import nextflow.util.Escape
/**
 * Builder to create the Bash script which is used to
 * wrap and launch the user task
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@Slf4j
@CompileStatic
class BashWrapperBuilder {

    static final public KILL_CMD = '[[ "$pid" ]] && nxf_kill $pid'

    static final private ENDL = '\n'

    static final public List<String> BASH

    static private int level

    @PackageScope
    static public String systemOsName = System.getProperty('os.name')

    static {
        /*
         * Env variable `NXF_DEBUG` is used to control debug options in executed BASH scripts
         * - 0: no debug
         * - 1: dump current environment in the `.command.log` file
         * - 2: trace the execution of user script adding the `set -x` flag
         * - 3: trace the execution of wrapper scripts
         */
        def str = System.getenv('NXF_DEBUG')
        try {
            level = str ? str as int : 0
        }
        catch( Exception e ) {
            log.warn "Invalid value for `NXF_DEBUG` variable: $str -- See http://www.nextflow.io/docs/latest/config.html#environment-variables"
        }
        BASH = Collections.unmodifiableList( level > 0 ? ['/bin/bash','-uex'] : ['/bin/bash','-ue'] )

    }


    @Delegate
    ScriptFileCopyStrategy copyStrategy

    @Delegate
    private TaskBean bean

    private boolean runWithContainer

    private ContainerBuilder containerBuilder

    private Path scriptFile
    
    private Path preGuardFile
    
    private Path postGuardFile

    private Path inputFile

    private Path startedFile

    private Path exitedFile

    private Path wrapperFile

    private BashTemplateEngine engine = new BashTemplateEngine()

    BashWrapperBuilder( TaskRun task ) {
        this(new TaskBean(task))
    }

    BashWrapperBuilder( TaskBean bean, ScriptFileCopyStrategy strategy = null ) {
        this.bean = bean
        this.copyStrategy = strategy ?: new SimpleFileCopyStrategy(bean)
    }

    /** only for testing -- do not use */
    protected BashWrapperBuilder() { }

    /**
     * @return The bash script fragment to change to the 'scratch' directory if it has been specified in the task configuration
     */
    protected String getScratchDirectoryCommand() {

        // convert to string for safety
        final scratchStr = scratch?.toString()

        if( scratchStr == null || scratchStr == 'false' ) {
            return null
        }

        /*
         * when 'scratch' is defined as a bool value
         * try to use the 'TMP' variable, if does not exist fallback to a tmp folder
         */
        if( scratchStr == 'true' ) {
            return 'NXF_SCRATCH="$(set +u; nxf_mktemp $TMPDIR)"'
        }

        if( scratchStr.toLowerCase() in ['ramdisk','ram-disk']) {
            return 'NXF_SCRATCH="$(nxf_mktemp /dev/shm)"'
        }

        return "NXF_SCRATCH=\"\$(set +u; nxf_mktemp $scratchStr)\""
    }

    protected boolean shouldUnstageOutputs() {
        return targetDir && workDir!=targetDir
    }

    protected boolean fixOwnership() {
        systemOsName == 'Linux' && containerConfig?.fixOwnership && runWithContainer && containerConfig.engine == 'docker' // <-- note: only for docker (shifter is not affected)
    }

    protected isMacOS() {
        systemOsName.startsWith('Mac')
    }

    @PackageScope String buildNew0() {
        final template = BashWrapperBuilder.class.getResourceAsStream('command-run.txt')
        try {
            return buildNew0(template.newReader())
        }
        finally {
            template.close()
        }
    }

    protected String getOutputEnvCaptureSnippet(List<String> names) {
        def result = new StringBuilder()
        result.append('\n')
        result.append('# capture process environment\n')
        result.append('set +u\n')
        for( int i=0; i<names.size(); i++) {
            final key = names[i]
            result.append "echo $key=\${$key[@]} "
            result.append( i==0 ? '> ' : '>> ' )
            result.append(TaskRun.CMD_ENV)
            result.append('\n')
        }
        result.toString()
    }
    
    protected Map<String,String> makeBinding() {
        /*
         * initialise command files
         */
        scriptFile = workDir.resolve(TaskRun.CMD_SCRIPT)
        inputFile = workDir.resolve(TaskRun.CMD_INFILE)
        startedFile = workDir.resolve(TaskRun.CMD_START)
        exitedFile = workDir.resolve(TaskRun.CMD_EXIT)
        wrapperFile = workDir.resolve(TaskRun.CMD_RUN)
        preGuardFile = workDir.resolve(".preGuard.sh")
        postGuardFile = workDir.resolve(".postGuard.sh")

        // set true when running with through a container engine
        runWithContainer = containerEnabled && !containerNative

        // whenever it has to change to the scratch directory
        final changeDir = getScratchDirectoryCommand()

        /*
         * create the container launcher command if needed
         */
        containerBuilder = runWithContainer ? createContainerBuilder(changeDir) : null

        // ugly, it should be done somewhere else
        if( script )
            script = TaskProcessor.normalizeScript(script, shell)

        /*
         * fetch the script interpreter i.e. BASH, Perl, Python, etc
         */
        final interpreter = TaskProcessor.fetchInterpreter(script)

        if( outputEnvNames ) {
            if( !isBash(interpreter) ) throw new IllegalArgumentException("Process output of type env is only allowed with Bash process command -- Current interpreter: $interpreter")
            script += getOutputEnvCaptureSnippet(outputEnvNames)
        }

        final binding = new HashMap<String,String>(20)
        binding.header_script = headerScript
        binding.task_name = name
        binding.helpers_script = getHelpersScript()

        if( runWithContainer ) {
            binding.container_boxid = 'export NXF_BOXID="nxf-$(dd bs=18 count=1 if=/dev/urandom 2>/dev/null | base64 | tr +/ 0A)"'
            binding.container_helpers = containerBuilder.getScriptHelpers()
            binding.kill_cmd = containerBuilder.getKillCommand()
        }
        else {
            binding.container_boxid = null
            binding.container_helpers = null
            binding.kill_cmd = KILL_CMD
        }

        binding.cleanup_cmd = getCleanupCmd(changeDir)
        binding.scratch_cmd = ( changeDir ?: "NXF_SCRATCH=''" )

        binding.exit_file = exitFile(exitedFile)
        binding.touch_file = touchFile(startedFile)

        binding.module_load = getModuleLoadSnippet()
        binding.before_script = getBeforeScriptSnippet()
        binding.conda_activate = getCondaActivateSnippet()

        /*
         * add the task environment
         */
        final env = copyStrategy.getEnvScript(environment, runWithContainer)
        if( runWithContainer ) {
            binding.task_env = null
            binding.container_env = env
        }
        else {
            binding.task_env = env
            binding.container_env = null
        }

        /*
         * add the task secrets
         */
        if( !isSecretNative() ) {
            binding.secrets_env = getSecretsEnv()
        }
        else {
            binding.secrets_env = null
        }

        /*
         * staging input files when required
         */
        final stagingScript = copyStrategy.getStageInputFilesScript(inputFiles)
        String backups = "";
	for (Map.Entry<String, Path> entry : inputFiles) {
            backups += "echo -n '${Escape.path(entry.key)} ' >> .timestamps\nstat -Lc %Y ${Escape.path(entry.key)} >> .timestamps\n"
        }
        binding.stage_inputs = stagingScript ? "# stage input files\n${stagingScript}" + (postGuard ? "\n${backups}" : "") : null

        binding.stdout_file = TaskRun.CMD_OUTFILE
        binding.stderr_file = TaskRun.CMD_ERRFILE
        binding.trace_file = TaskRun.CMD_TRACE

        binding.trace_cmd = getTraceCommand(interpreter)
        
        binding.pre_guard = ""
        if( preGuard) {
            (preGuard as Map<String, ContractLevel>).findAll((key, value) -> value.shouldCheck())*.key.eachWithIndex { val, index -> binding.pre_guard += "chmod +x ${workDir.resolve(".preGuard_" + index + ".sh")}\nif (! ${workDir.resolve(".preGuard_" + index + ".sh")}); then echo Pre Guard $index failed >> ${binding.stderr_file}; exit 1; fi\n"}
        }

        binding.pre_emit = "touch ${workDir.resolve(".preEmit.state")}\n"
        if( preEmit) {
            preEmit.each((id, command) -> binding.pre_emit += "chmod +x ${workDir.resolve("." + id + "_pre_command.sh")}\nif ${workDir.resolve("." + id + "_pre_command.sh")}; then echo $id >> .preEmit.state; fi\n")
        }
        binding.launch_cmd = getLaunchCommand(interpreter,env)

        binding.post_guard = ""
        if( postGuard) {
            (postGuard as Map<String, ContractLevel>).findAll((key, value) -> value.shouldCheck())*.key.eachWithIndex { val, index -> binding.post_guard += "chmod +x ${workDir.resolve(".postGuard_" + index + ".sh")}\nif (! ${workDir.resolve(".postGuard_" + index + ".sh")}); then echo Post Guard $index failed >> ${binding.stderr_file}; exit 1; fi\n"}
        }
        binding.post_emit = "touch ${workDir.resolve(".postEmit.state")}\n"
        if( postEmit) {
            postEmit.each((id, command) -> binding.post_emit += "chmod +x ${workDir.resolve("." + id + "_post_command.sh")}\nif ${workDir.resolve("." + id + "_post_command.sh")}; then echo $id >> .postEmit.state; fi\n")
        }

        binding.stage_cmd = getStageCommand()
        binding.unstage_cmd = getUnstageCommand()
        binding.unstage_controls = changeDir || shouldUnstageOutputs() ? getUnstageControls() : null

        if( changeDir || shouldUnstageOutputs() ) {
            binding.unstage_outputs = copyStrategy.getUnstageOutputFilesScript(outputFiles,targetDir)
        }
        else {
            binding.unstage_outputs = null
        }

        binding.after_script = afterScript ? "# 'afterScript' directive\n$afterScript" : null

        // patch root ownership problem on files created with docker
        binding.fix_ownership = fixOwnership() ? "[ \${NXF_OWNER:=''} ] && chown -fR --from root \$NXF_OWNER ${workDir}/{*,.*} || true" : null

        binding.trace_script = isTraceRequired() ? getTraceScript(binding) : null
        
        return binding
    }

    protected String getSecretsEnv() {
        return SecretsLoader.isEnabled()
                ? SecretsLoader.instance.load() .getSecretsEnv(secretNames)
                : null
    }

    protected boolean isBash(String interpreter) {
        interpreter.tokenize(' /').contains('bash')
    }

    protected String getTraceScript(Map binding) {
        def res = BashWrapperBuilder.class.getResource('command-trace.txt')
        engine.render(res.newReader(), binding)
    }

    @PackageScope String buildNew0(BufferedReader template) {
        final binding = makeBinding()
        engine.render(template, binding)
    }

    /**
     * Build up the BASH wrapper script file which will launch the user provided script
     * @return The {@code Path} of the created wrapper script
     */
    Path build() {
        assert workDir, "Missing 'workDir' property in BashWrapperBuilder object"
        assert script, "Missing 'script' property in BashWrapperBuilder object"

        if( statsEnabled && isMacOS() && !isContainerEnabled() )
            log.warn1("Task runtime metrics are not reported when using macOS without a container engine")

        final wrapper = buildNew0()
        final result = write0(targetWrapperFile(), wrapper)
        write0(targetScriptFile(), script)
        if( input != null )
            write0(targetInputFile(), input.toString())
        if( preGuard)
            (preGuard as Map<String, ContractLevel>).findAll {(String key, ContractLevel value) -> value.shouldCheck()}*.key.eachWithIndex { val, index -> write0(workDir.resolve(".preGuard_" + index + ".sh"), val as String)}
        if( postGuard)
            (postGuard as Map<String, ContractLevel>).findAll {(String key, ContractLevel value) -> value.shouldCheck()}*.key.eachWithIndex { val, index -> write0(workDir.resolve(".postGuard_" + index + ".sh"), val as String)}
        if( preEmit)
            preEmit.each((id, command) -> write0(workDir.resolve("." + id + "_pre_command.sh"), command as String))
        if( postEmit)
            postEmit.each((id, command) -> write0(workDir.resolve("." + id + "_post_command.sh"), command as String))
        return result
    }

    protected Path targetWrapperFile() { return wrapperFile }

    protected Path targetScriptFile() { return scriptFile }

    protected Path targetInputFile() { return inputFile }

    private Path write0(Path path, String data) {
        try {
            return Files.write(path, data.getBytes())
        }
        catch (FileSystemException e) {
            // throw a ProcessStageException so that the error can be recovered
            // via nextflow re-try mechanism
            throw new ProcessException("Unable to create file ${path.toUriString()}", e)
        }
    }

    protected String getHelpersScript() {
        def result = new StringBuilder()

        def s1 = copyStrategy.getBeforeStartScript()
        if( s1 )
            result.append(s1).append('\n')

        def s2 = moduleNames ? engine.render(BashWrapperBuilder.class.getResource('modules-env.txt').newReader(), Collections.emptyMap()) : null
        if( s2 )
            result.append(s2).append('\n')

        result.size() ? result.toString() : null
    }

    private String getBeforeScriptSnippet() {
        beforeScript ? "# beforeScript directive\n$beforeScript\n" : null
    }

    private String getModuleLoadSnippet() {
        if( !moduleNames )
            return null
        String result=''
        for( String it : moduleNames) {
            result += moduleLoad(it) + ENDL
        }
        return result
    }

    private String getCondaActivateSnippet() {
        if( !condaEnv )
            return null
        def result = "# conda environment\n"
        result += 'source $(conda info --json | awk \'/conda_prefix/ { gsub(/"|,/, "", $2); print $2 }\')'
        result += "/bin/activate ${Escape.path(condaEnv)}\n"
        return result
    }

    protected String getTraceCommand(String interpreter) {
        String result = "${interpreter} ${fileStr(scriptFile)}"
        if( input != null )
            result += pipeInputFile(inputFile)

        return result
    }

    protected boolean isTraceRequired() {
        statsEnabled || fixOwnership()
    }

    protected String getLaunchCommand(String interpreter, String env) {
        /*
        * process stats
        */
        String launcher

        // NOTE: the isTraceRequired() check must match the logic in launchers (i.e. AwsBatchScriptLauncher)
        // that determines when to stage the file.
        final traceWrapper = isTraceRequired()
        if( traceWrapper ) {
            // executes the stub which in turn executes the target command
            launcher = "/bin/bash ${fileStr(wrapperFile)} nxf_trace"
        }
        else {
            launcher = "${interpreter} ${fileStr(scriptFile)}"
        }

        /*
         * create the container engine command when needed
         */
        if( containerBuilder ) {
            String cmd = env ? 'eval $(nxf_container_env); ' + launcher : launcher
            if( env && !containerConfig.entrypointOverride() ) {
                if( containerBuilder instanceof SingularityBuilder )
                    cmd = 'cd $PWD; ' + cmd
                cmd = "/bin/bash -c \"$cmd\""
            }
            launcher = containerBuilder.getRunCommand(cmd)
        }

        /*
         * pipe the input file on the command standard input
         */
        if( !traceWrapper && input != null ) {
            launcher += pipeInputFile(inputFile)
        }

        return launcher
    }


    private String copyFileToWorkDir(String fileName) {
        copyFile(fileName, workDir.resolve(fileName))
    }
    

    String getCleanupCmd(String scratch) {
        String result = ''
        // -- cleanup the scratch dir
        if( scratch && cleanup != false ) {
            result += (containerBuilder !instanceof DockerBuilder ? 'rm -rf $NXF_SCRATCH || true' : '(sudo -n true && sudo rm -rf "$NXF_SCRATCH" || rm -rf "$NXF_SCRATCH")&>/dev/null || true')
            result += '\n'
        }
        // -- remove the container in this way because 'docker run --rm'  fail in some cases -- see https://groups.google.com/d/msg/docker-user/0Ayim0wv2Ls/-mZ-ymGwg8EJ
        final remove = containerBuilder?.getRemoveCommand()
        if( remove ) {
            result += "${remove} &>/dev/null || true"
            result += '\n'
        }
        return result
    }

    String getExitScriptLegacy(String scratch) {
        def result = getCleanupCmd(scratch)
        result += 'exit $exit_status'
        result.readLines().join('\n  ')
    }

    @PackageScope
    ContainerBuilder createContainerBuilder0(String engine) {
        /*
         * create a builder instance given the container engine
         */
        if( engine == 'docker' )
            return new DockerBuilder(containerImage)
        if( engine == 'podman' )
            return new PodmanBuilder(containerImage)
        if( engine == 'singularity' )
            return new SingularityBuilder(containerImage)
        if( engine == 'udocker' )
            return new UdockerBuilder(containerImage)
        if( engine == 'shifter' )
            return new ShifterBuilder(containerImage)
        if( engine == 'charliecloud' )
            return new CharliecloudBuilder(containerImage)
        //
        throw new IllegalArgumentException("Unknown container engine: $engine")
    }

    protected boolean getAllowContainerMounts() {
        return true
    }

    /**
     * Build a {@link DockerBuilder} object to handle Docker commands
     *
     * @param envFile A file containing environment configuration
     * @param changeDir String command to change to the working directory
     * @return A {@link DockerBuilder} instance
     */
    @PackageScope
    ContainerBuilder createContainerBuilder(String changeDir) {

        final engine = containerConfig.getEngine()
        ContainerBuilder builder = createContainerBuilder0(engine)

        /*
         * initialise the builder
         */
        // do not mount inputs when they are copied in the task work dir -- see #1105
        if( stageInMode != 'copy' && allowContainerMounts )
            builder.addMountForInputs(inputFiles)

        if( allowContainerMounts )
            builder.addMount(binDir)

        if(this.containerMount)
            builder.addMount(containerMount)

        // task work dir
        if( allowContainerMounts )
            builder.setWorkDir(workDir)

        // set the name
        builder.setName('$NXF_BOXID')

        if( this.containerMemory )
            builder.setMemory(containerMemory)

        if( this.containerCpus )
            builder.setCpus(containerCpus)

        if( this.containerCpuset )
            builder.addRunOptions(containerCpuset)

        // export the nextflow script debug variable
        if( isTraceRequired() )
            builder.addEnv( 'NXF_DEBUG=${NXF_DEBUG:=0}')

        // add the user owner variable in order to patch root owned files problem
        if( fixOwnership() )
            builder.addEnv( 'NXF_OWNER=$(id -u):$(id -g)' )

        if( engine=='docker' && System.getenv('NXF_DOCKER_OPTS') ) {
            builder.addRunOptions(System.getenv('NXF_DOCKER_OPTS'))
        }

        for( String var : containerConfig.getEnvWhitelist() ) {
            builder.addEnv(var)
        }

        // when secret are not managed by the execution platform natively
        // the secret names are added to the container env var white list
        if( !isSecretNative() && secretNames )  {
            for( String var : secretNames )
                builder.addEnv(var)
        }

        // set up run docker params
        builder.params(containerConfig)

        // extra rule for the 'auto' temp dir temp dir
        def temp = containerConfig.temp?.toString()
        if( temp == 'auto' || temp == 'true' ) {
            builder.setTemp( changeDir ? '$NXF_SCRATCH' : '$(nxf_mktemp)' )
        }

        if( containerConfig.containsKey('kill') )
            builder.params(kill: containerConfig.kill)

        if( containerConfig.writableInputMounts==false )
            builder.params(readOnlyInputs: true)

        if( this.containerConfig.entrypointOverride() )
            builder.params(entry: '/bin/bash')

        // give a chance to override any option with process specific `containerOptions`
        if( containerOptions ) {
            builder.addRunOptions(containerOptions)
        }

        // The current work directory should be mounted only when
        // the task is executed in a temporary scratch directory (ie changeDir != null)
        // See https://github.com/nextflow-io/nextflow/issues/1710
        builder.addMountWorkDir( changeDir as boolean )

        builder.build()
        return builder
    }

    String moduleLoad(String name) {
        int p = name.lastIndexOf('/')
        p != -1 ? "nxf_module_load ${name.substring(0,p)} ${name.substring(p+1)}" : "nxf_module_load ${name}"
    }

    protected String getStageCommand() { 'nxf_stage' }

    protected String getUnstageCommand() { 'nxf_unstage' }

    protected String getUnstageControls() {
        def result = copyFileToWorkDir(TaskRun.CMD_OUTFILE) + ' || true' + ENDL
        result += copyFileToWorkDir(TaskRun.CMD_ERRFILE) + ' || true' + ENDL
        if( statsEnabled )
            result += copyFileToWorkDir(TaskRun.CMD_TRACE) + ' || true' + ENDL
        if(  outputEnvNames )
            result += copyFileToWorkDir(TaskRun.CMD_ENV) + ' || true' + ENDL
        return result
    }

}
