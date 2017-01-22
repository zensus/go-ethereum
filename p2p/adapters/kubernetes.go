package adapters

import (
	"bytes"
	"flag"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strconv"
)

var (
	kubernetesConfigFile = flag.String("kubeconfig", "$GOPATH/go/src/github.com/ethereum/cluster/configs/swarmeastus/kubeconfig.json", "kubernetes config file")
)

//commandObjects assemble into shell command line command strings
type commandObject interface {
	Assemble() string
}

//A string is a trivial command object

type commandString string

func (self commandString) Assemble() string {
	return string(self)
}

func executeCommand(cmd commandObject) (outstring string) {
	//given a commandObject, executeCommand assembles it into string and executes on using os.exec
	var (
		cmdOut []byte
		err    error
	)
	if cmdOut, err = exec.Command(cmd.Assemble()).Output(); err != nil {
		fmt.Fprintln(os.Stderr, "There was an error running a shell command: ", err)
		os.Exit(1)
	}
	outstring := string(cmdOut)
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
func canWeAccessKubernetes(conf kubeConfig) bool {
	//this function should run 'kubectl version' too see if there are any errors in accessing the cluster
	//config = newKubeConfig()
	versionCmd = newKubeCommand(conf, "version --short")
	//and then we must do an os.exec versionCommand.Assemble() and see the return value.
	versionstring := executeCommand(versionCmd)
	// should be sth like:
	// Client Version: v1.5.1
	// Server Version: v1.4.1
	if strings.Contains(versionstring, "Client Version") == false || strings.Contains(versionstring, "Server Version") == false {
		return false
	}
	return true
}

func activateKubernetesProxy(conf kubeConfig) {
	//test if the proxy is already running. If not start it:
	if canWeAccessKubernetesProxy() == false {
		startProxy = newKubeCommand(conf, "proxy –api-prefix=/")
		res := executeCommand(startProxy)
		if strings.Contains(res, "Starting to serve on 127.0.0.1:8001") == false {
			//ERROR
		}
	}
	if canWeAccessKubernetesProxy() == false {
		//ERROR
	}
	return
}

//canWeAccessKubernetesProxy is a check function to see if we can access the local kubernetes API proxy
func canWeAccessKubernetesProxy() bool {
	//this function should query the proxy 'kubectl version' too see if there are any errors in accessing the cluster
	//we must try to load http://127.0.0.1:8001/api/
	url := "/api/"
	var api map[string]interface{}
	getFromKubeApi(url, &api)
	if api[kind] != "APIVersions" {
		return false
	}
	return true
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

//newBzzdCommand is a factory taking a commandObject 'cmd' and producing a corresponding bzzdCommand with default locations for geth binary and bzzd ipc.
func newBzzdCommand(cmd commandObject) *bzzdCommand {
	return &bzzdCommand{ipc: "/bzzd.ipc", gethpath: "/geth", command: cmd}
}

//
//
//

func getBzzdPodNames(cluster cluster) (runningPods []string) {
	//executes the following kubernetes command on the cluster:
	//getPods = newKubeCommand(cluster, "get pods -o custom-columns=NAME:.metadata.name | grep bzzd")
	//then we must execute that command ... possibly wrap it in bash because of the pipe.
	//if all ok the output is one pod-name per line.
	//
	//UPDATE: using the kubectl proxy we can get pods (as JSON object) from
	//http://127.0.0.1:8001/api/v1/namespaces/swarm/pods
	//or all bzzd pods
	//http://127.0.0.1:8001/api/v1/namespaces/swarm/pods?labelSelector=app%3Dbzzd

	url := "/api/v1/namespaces/swarm/pods?labelSelector=app%3Dbzzd"
	pods := Pods{}
	getFromKubeApi(url, &pods)
	var runningPods []string
	//todo, fill runningPods with content of
	//pods[Items][Metadata][Name]
	//and return it.

}

func getBzzdPodName(cluster cluster, id int) (podName string) {
	//executes the following kubernetes command on the cluster:
	//getPod = newKubeCommand(cluster, "get pods -o custom-columns=NAME:.metadata.name | grep bzzd-"+id)
	//then we must execute that command ... possibly wrap it in bash because of the pipe.
	//if all ok the output is one pod-name of the form bzzd-30399-3196715608-ub01i
	//we read and parse the output and if all ok, return the name
	//
	//UPDATE: using the kubectl proxy we can get pod names (from JSON object) from
	//http://127.0.0.1:8001/api/v1/namespaces/swarm/pods?labelSelector=appn%3Dbzzd-30400

	url := "/api/v1/namespaces/swarm/pods?labelSelector=appn%3Dbzzd-" + id
	pods := Pods{}
	getFromKubeApi(url, &pods)
	//todo: there should be only one pod with that tag (eg appn-30399). However there can always be the situation when there appear to be two - for example when one is 'terminating' and the other is 'starting up'.
	//We should check that we return only (the right) one.
	return pods[Items][Metadata][Name]
}

//getFromKubeApi is a wrapper function to retrieve data from kubernetes using the local api proxy (kubectl proxy --api-prefix=/)
func getFromKubeApi(url string, result *interface{}) {
	//retrieves 'url' from kubernetes api proxy and unmarshals the retruned JSON into 'result'
	responseReader, err := http.Get("http://127.0.0.1:8001" + url)
	if err != nil {
		// handle error
	}
	defer responseReader.Body.Close()
	response, err1 := ioutil.ReadAll(responseReader.Body)
	if err1 != nil {
		// handle error
	}
	err2 := json.Unmarshal(podlist, &result)
	if err2 != nil {
		// handle error
	}
}

func getBzzdConfig(cluster cluster, id int) (bzzdConfig Deployments) {
	//pulls the current configuration of a bzzd deployment from the cluster.
	//The output is actually JSON so we should parse it and ...
	// cmd = "get deployment bzzd-" + id + " -o json"
	// getDeploymentJson = newKubeCommand(cluster, cmd)
	//then execute getDeploymentJson and parse the returned string
	//
	// Alternative:
	//http://127.0.0.1:8001/apis/extensions/v1beta1/namespaces/swarm/deployments?labelSelector=appn%3Dbzzd-30400
	deps := Deployments{}
	getFromKubeApi("/apis/extensions/v1beta1/namespaces/swarm/deployments?labelSelector=app%3Dbzzd-"+id, &deps)

}

func restartBzzd(cluster cluster, id int) {
	//to restart bzzd, delete the pod. The replication controller will automatically create a new one.
	podToDelete = getBzzdPodName(cluster, id)
	restartThePod = newKubeCommand(cluster, "delete pod "+podToDelete)
	executeCommand(restartThePod)
}

func stopBzzd(cluster cluster, id int) {
	//to stop a bzzd instance, delete the controlling deployment
	stopBzzd = newKubeCommand(cluster, "delete deployment bzzd-"+id)
	//then execute the command
}

// When we get JSON from Kubernets API we want to parse it easily. Best way is to have structs at hand to receive the data.
//here are some such structs

// generated by github.com/bashtian/jsonutils/cmd/jsonutil/main.go

//Pods are returned from http://127.0.0.1:8001/api/v1/namespaces/swarm/pods
type Pods struct {
	// ApiVersion string `json:"apiVersion"`
	Items []struct {
		Metadata struct {
			// Annotations struct {
			// 	KubernetesIoCreatedBy string `json:"kubernetes.io/created-by"`
			// } `json:"annotations"`
			// CreationTimestamp time.Time `json:"creationTimestamp"`
			// GenerateName      string    `json:"generateName"`
			// Labels            struct {
			// 	App             string `json:"app"`
			// 	Appn            string `json:"appn"`
			// 	PodTemplateHash int64  `json:"pod-template-hash,string"`
			// } `json:"labels"`
			Name string `json:"name"`
			// 		Namespace       string `json:"namespace"`
			// 		OwnerReferences []struct {
			// 			ApiVersion string `json:"apiVersion"`
			// 			Controller bool   `json:"controller"`
			// 			Kind       string `json:"kind"`
			// 			Name       string `json:"name"`
			// 			Uid        string `json:"uid"`
			// 		} `json:"ownerReferences"`
			// 		ResourceVersion int64  `json:"resourceVersion,string"`
			// 		SelfLink        string `json:"selfLink"`
			// 		Uid             string `json:"uid"`
		} `json:"metadata"`
		// 	Spec struct {
		// 		Containers []struct {
		// 			Env []struct {
		// 				Name  string `json:"name"`
		// 				Value string `json:"value"`
		// 			} `json:"env"`
		// 			Image           string `json:"image"`
		// 			ImagePullPolicy string `json:"imagePullPolicy"`
		// 			Name            string `json:"name"`
		// 			Ports           []struct {
		// 				ContainerPort int64  `json:"containerPort"`
		// 				Name          string `json:"name"`
		// 				Protocol      string `json:"protocol"`
		// 			} `json:"ports"`
		// 			Resources struct {
		// 			} `json:"resources"`
		// 			TerminationMessagePath string `json:"terminationMessagePath"`
		// 			VolumeMounts           []struct {
		// 				MountPath string `json:"mountPath"`
		// 				Name      string `json:"name"`
		// 			} `json:"volumeMounts"`
		// 		} `json:"containers"`
		// 		DnsPolicy       string `json:"dnsPolicy"`
		// 		NodeName        string `json:"nodeName"`
		// 		RestartPolicy   string `json:"restartPolicy"`
		// 		SecurityContext struct {
		// 		} `json:"securityContext"`
		// 		ServiceAccount                string `json:"serviceAccount"`
		// 		ServiceAccountName            string `json:"serviceAccountName"`
		// 		TerminationGracePeriodSeconds int64  `json:"terminationGracePeriodSeconds"`
		// 		Volumes                       []struct {
		// 			Name string `json:"name"`
		// 			Nfs  struct {
		// 				Path   string `json:"path"`
		// 				Server net.IP `json:"server"`
		// 			} `json:"nfs"`
		// 		} `json:"volumes"`
		// 	} `json:"spec"`
		// 	Status struct {
		// 		Conditions []struct {
		// 			LastProbeTime      interface{} `json:"lastProbeTime"`
		// 			LastTransitionTime time.Time   `json:"lastTransitionTime"`
		// 			Status             bool        `json:"status,string"`
		// 			Type               string      `json:"type"`
		// 		} `json:"conditions"`
		// 		ContainerStatuses []struct {
		// 			ContainerID string `json:"containerID"`
		// 			Image       string `json:"image"`
		// 			ImageID     string `json:"imageID"`
		// 			LastState   struct {
		// 			} `json:"lastState"`
		// 			Name         string `json:"name"`
		// 			Ready        bool   `json:"ready"`
		// 			RestartCount int64  `json:"restartCount"`
		// 			State        struct {
		// 				Running struct {
		// 					StartedAt time.Time `json:"startedAt"`
		// 				} `json:"running"`
		// 			} `json:"state"`
		// 		} `json:"containerStatuses"`
		// 		HostIP    net.IP    `json:"hostIP"`
		// 		Phase     string    `json:"phase"`
		// 		PodIP     net.IP    `json:"podIP"`
		// 		StartTime time.Time `json:"startTime"`
		// 	} `json:"status"`
	} `json:"items"`
	// Kind     string `json:"kind"`
	// 	Metadata struct {
	// 		ResourceVersion int64  `json:"resourceVersion,string"`
	// 		SelfLink        string `json:"selfLink"`
	// } `json:"metadata"`
}

type Deployments struct {
	ApiVersion string `json:"apiVersion"`
	Items      []struct {
		Metadata struct {
			Annotations struct {
				DeploymentKubernetesIoRevision              int64  `json:"deployment.kubernetes.io/revision,string"`
				KubectlKubernetesIoLastAppliedConfiguration string `json:"kubectl.kubernetes.io/last-applied-configuration"`
			} `json:"annotations"`
			CreationTimestamp time.Time `json:"creationTimestamp"`
			Generation        int64     `json:"generation"`
			Labels            struct {
				App  string `json:"app"`
				Appn string `json:"appn"`
			} `json:"labels"`
			Name            string `json:"name"`
			Namespace       string `json:"namespace"`
			ResourceVersion int64  `json:"resourceVersion,string"`
			SelfLink        string `json:"selfLink"`
			Uid             string `json:"uid"`
		} `json:"metadata"`
		Spec struct {
			Replicas int64 `json:"replicas"`
			Selector struct {
				MatchLabels struct {
					Appn string `json:"appn"`
				} `json:"matchLabels"`
			} `json:"selector"`
			Strategy struct {
				RollingUpdate struct {
					MaxSurge       int64 `json:"maxSurge"`
					MaxUnavailable int64 `json:"maxUnavailable"`
				} `json:"rollingUpdate"`
				Type string `json:"type"`
			} `json:"strategy"`
			Template struct {
				Metadata struct {
					CreationTimestamp interface{} `json:"creationTimestamp"`
					Labels            struct {
						App  string `json:"app"`
						Appn string `json:"appn"`
					} `json:"labels"`
				} `json:"metadata"`
				Spec struct {
					Containers []struct {
						Env []struct {
							Name  string `json:"name"`
							Value string `json:"value"`
						} `json:"env"`
						Image           string `json:"image"`
						ImagePullPolicy string `json:"imagePullPolicy"`
						Name            string `json:"name"`
						Ports           []struct {
							ContainerPort int64  `json:"containerPort"`
							Name          string `json:"name"`
							Protocol      string `json:"protocol"`
						} `json:"ports"`
						Resources struct {
						} `json:"resources"`
						TerminationMessagePath string `json:"terminationMessagePath"`
						VolumeMounts           []struct {
							MountPath string `json:"mountPath"`
							Name      string `json:"name"`
						} `json:"volumeMounts"`
					} `json:"containers"`
					DnsPolicy       string `json:"dnsPolicy"`
					RestartPolicy   string `json:"restartPolicy"`
					SecurityContext struct {
					} `json:"securityContext"`
					TerminationGracePeriodSeconds int64 `json:"terminationGracePeriodSeconds"`
					Volumes                       []struct {
						Name string `json:"name"`
						Nfs  struct {
							Path   string `json:"path"`
							Server net.IP `json:"server"`
						} `json:"nfs"`
					} `json:"volumes"`
				} `json:"spec"`
			} `json:"template"`
		} `json:"spec"`
		Status struct {
			AvailableReplicas  int64 `json:"availableReplicas"`
			ObservedGeneration int64 `json:"observedGeneration"`
			Replicas           int64 `json:"replicas"`
			UpdatedReplicas    int64 `json:"updatedReplicas"`
		} `json:"status"`
	} `json:"items"`
	Kind     string `json:"kind"`
	Metadata struct {
		ResourceVersion int64  `json:"resourceVersion,string"`
		SelfLink        string `json:"selfLink"`
	} `json:"metadata"`
}
