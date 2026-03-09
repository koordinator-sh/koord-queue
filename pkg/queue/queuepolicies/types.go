package queuepolicies

import (
	"flag"
	"os"
)

const Priority string = "Priority"
const Block string = "Block"
const Round string = "Round"

const QueueArgsAnnotationKey string = "kube-queue/queue-args"
const QueuePolicyLabelKey string = "kube-queue/queue-policy"

var defaultPolicyEnv string
var defaultPolicyCLI string

func init() {
	if os.Getenv("StrictPriority") == "true" {
		defaultPolicyEnv = Block
	} else if os.Getenv("StrictConsistency") == "true" {
		defaultPolicyEnv = Priority
	} else {
		defaultPolicyEnv = Priority
	}
}

func AddCommandLine(fs *flag.FlagSet) {
	fs.StringVar(&defaultPolicyCLI, "default-queue-policy", defaultPolicyEnv, "The policy to use for dequeuing queueunits in the kube-queue.")
}

func GetDefaultQueuePolicy() string {
	return defaultPolicyCLI
}
