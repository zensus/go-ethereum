package adapters

import (
	"bytes"
	"fmt"
	"net"
	"os"
	"strconv"
)

import (
	"flag"
	"github.com/vharitonsky/iniflags" // https://stackoverflow.com/questions/16465705/how-to-handle-configuration-in-go
)

var (
	kubernetesConfigFile = flag.String("kubeconfig", "$GOPATH/go/src/github.com/ethereum/cluster/configs/swarmeastus/kubeconfig.json", "kubernetes config file")
)

//commandObjects assemble into shell command line command strings
type commandObject interface {
	Assemble() string
}

//A string is a trivial command object
func (self string) Assemble() (commandString string) {
	return self
}

func executeCommand(cmd commandObject) {
	//TODO
	//call an os.exec on cmd.Assemble()
	//possibly read and parse the output and return either an error or the output as a string
}

//We want to issue commands to the bzzd console in the various swarm nodes
//These commands must be injected to the console using geth attached to the swarm IPC.

//bzzdCommands use geth to inject commands into the bzzd console
type bzzdCommand struct {
	ipc      string        "the IPC path. Example /bzzd.ipc"
	gethpath string        "the full path to geth executable. Example: /geth"
	command  commandObject "the actual command to inject on the bzzd console"
}

func (self bzzdCommand) Assemble() (commandString string) {
	//assembles a geth --exec command string from a given bzzdCommand struct.
	//example: /geth --exec 'bzz.info' attach ipc:/bzzd.ipc
	commandString = self.gethpath
	commandString += " --exec '" + self.command.Assemble() + "' attach "
	commandString += " ipc:" + self.ipc
	return commandString
}

//newBzzdCommand is a factory taking a commandObject 'cmd' and producing a corresponding bzzdCommand with the correct defaults for the test cluster.
func newBzzdCommand(cmd commandObject) *bzzdCommand {
	return &bzzdCommand{ipc: "/bzzd.ipc", gethpath: "/geth", command: cmd}
}

//The cluster of swarm nodes is orchestrated by kubernetes.
type cluster struct {
	kubeConfig
}

//To issue any commands to the kubernetes cluster we must have a working kubernetes config
//(We use namespace 'swarm' for everything)

//kubeConfig contains the necessary parameters to access swarm pods in the kubernetes cluster
type kubeConfig struct {
	kubeConfigFile string "path to kubeconfig.json"
	kubeNamespace  string "kubernetes namespace for swarm pods. Example: 'swarm'"
}

//newKubeConfig is a factory generating kubeConfig objects tailored to the cluster
func newKubeConfig() *kubeConfig {
	return &kubeConfig{kubeConfigFile: kubernetesConfigFile, kubeNamespace: "swarm"}
}

//kubeCommands use kubectl to issue general commands to the cluster
type kubeCommand struct {
	kubeConfig kubeConfig    "configuration parameters for accessing kubernetes"
	command    commandObject "The kubernetes command to execute"
}

func (self kubeCommand) Assemble() (commandString string) {
	//assembles a kubectl command string from a given kubeCommand struct.
	//example: kubectl --kubeconfig="/path/kubeconfig.json" --namespace='swarm' get pods
	commandString = "kubectl --kubeconfig='" + self.kubeconfig.kubeConfigFile + "'"
	commandString += " --namespace='" + self.kubeConfig.namespace + "'"
	commandString += self.command.Assemble()
	return commandString
}

func newKubeCommand(conf kubeConfig, cmd commandObject) *kubeCommand {
	return &kubeCommand{kubeConfig: conf, command: cmd}
}

//canWeAccessKubernetes is a check function to see if kubectl is available and can connect to the cluster
func canWeAccessKubernetes(conf kubeConfig) {
	//this function should run 'kubectl version' too see if there are any errors in accessing the cluster
	//config = newKubeConfig()
	versionCmd = newKubeCommand(conf, "version")
	//and then we must do an os.exec versionCommand.Assemble() and see the return value.
}

//Kubernetes orchestrates 'pods' which contain docker containers for swarm.
// To control the swarm processes we must be able to access the pods and inject commands

//podCommands use kubectl to inject commands into running pods
type podCommand struct {
	kubeConfig kubeConfig    "configuration parameters for accessing kubernetes"
	podName    string        "Name of the kubernetes pod to inject command into"
	command    commandObject "The command to be executes in the pod"
}

func (self podCommand) Assemble() (commandString string) {
	//assembles a kubectl exec command string from a given podCommand struct.
	//example: kubectl --kubeconfig="/path/kubeconfig.json" --namespace='swarm' exec -it pod PODNAME -- ls
	commandString = "kubectl --kubeconfig='" + self.kubeconfig.kubeConfigFile + "'"
	commandString += " --namespace='" + self.kubeConfig.namespace + "'"
	commandString += " exec -it '" + self.podName + "' -- "
	commandString += self.command.Assemble()
	return commandString
}

//
//
//

func getBzzdPodNames(cluster cluster) (runningPods []string) {
	//executes the following kubernetes command on the cluster:
	getPods = newKubeCommand(cluster, "get pods -o custom-columns=NAME:.metadata.name | grep bzzd")
	//then we must execute that command ... possibly wrap it in bash because of the pipe.
	//if all ok the output is one pod-name per line.
}

func getBzzdPodName(cluster cluster, id int) (podName string) {
	//executes the following kubernetes command on the cluster:
	getPod = newKubeCommand(cluster, "get pods -o custom-columns=NAME:.metadata.name | grep bzzd-"+id)
	//then we must execute that command ... possibly wrap it in bash because of the pipe.
	//if all ok the output is one pod-name of the form bzzd-30399-3196715608-ub01i
	//we read and parse the output and if all ok, return the name
	return podName
}

func getBzzdConfig(cluster cluster, id int) (bzzdConfig string) {
	//pulls the current configuration of a bzzd deployment from the cluster.
	//The otuput is actually JSON so we should parse it and ...
	cmd = "get deployment bzzd-" + id + " -o json"
	getDeploymentJson = newKubeCommand(cluster, cmd)
	//then execute getDeploymentJson and parse the returned string
}

func restartBzzd(cluster cluster, id int) {
	//to restart bzzd, delete the pod. The replication controller will automatically create a new one.
	podToDelete = getBzzdPod(cluster, id)
	restartThePod = newKubeCommand(cluster, "delete pod "+podToDelete)
	//then we must execute the command
}

func stopBzzd(cluster cluster, id int) {
	//to stop a bzzd instance, delete the controlling deployment
	stopBzzd = newKubeCommand(cluster, "delete deployment bzzd-"+id)
	//then execute the command
}
