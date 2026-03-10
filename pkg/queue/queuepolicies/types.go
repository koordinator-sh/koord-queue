package queuepolicies

import (
	"flag"
	"os"
)

const Priority string = "Priority"
const Block string = "Block"
const Round string = "Round"
const Intelligent string = "Intelligent"

const QueueArgsAnnotationKey string = "koord-queue/queue-args"
const QueuePolicyLabelKey string = "koord-queue/queue-policy"
const PriorityThresholdAnnotationKey string = "koord-queue/priority-threshold"

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
	fs.StringVar(&defaultPolicyCLI, "default-queue-policy", defaultPolicyEnv, "The policy to use for dequeuing queueunits in the koord-queue.")
}

func GetDefaultQueuePolicy() string {
	return defaultPolicyCLI
}
