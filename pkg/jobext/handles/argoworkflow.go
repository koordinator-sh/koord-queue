package handles

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	argov1alpha1 "github.com/argoproj/argo-workflows/v3/pkg/apis/workflow/v1alpha1"
	"github.com/koordinator-sh/koord-queue/pkg/apis/scheduling/v1alpha1"
	"github.com/koordinator-sh/koord-queue/pkg/jobext/framework"
	"github.com/koordinator-sh/koord-queue/pkg/jobext/util"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/kueue/apis/kueue/v1beta1"
)

type ArgoWorkflow struct {
	c client.Client

	f func(ctx context.Context, namespaceName types.NamespacedName) ([]*v1.Pod, error)
}

var _ framework.GenericJobExtension = &ArgoWorkflow{}

func (a *ArgoWorkflow) GetPodsFunc() func(ctx context.Context, namespaceName types.NamespacedName) ([]*v1.Pod, error) {
	return a.f
}

func (a *ArgoWorkflow) Object() client.Object {
	return &argov1alpha1.Workflow{}
}

func (a *ArgoWorkflow) DeepCopy(obj client.Object) client.Object {
	o, _ := obj.(*argov1alpha1.Workflow)
	return o.DeepCopy()
}

func (a *ArgoWorkflow) GVK() schema.GroupVersionKind {
	return schema.GroupVersionKind{
		Group:   argov1alpha1.SchemeGroupVersion.Group,
		Version: argov1alpha1.SchemeGroupVersion.Version,
		Kind:    "Workflow",
	}
}

func IsJSONStr(str string) bool {
	str = strings.TrimSpace(str)
	return len(str) > 0 && str[0] == '{'
}

func isWorkflowSuspend(wf *argov1alpha1.Workflow) bool {
	if wf == nil {
		return false
	}

	for _, no := range wf.Status.Nodes {
		if no.Type == argov1alpha1.NodeTypeSuspend && no.Phase == argov1alpha1.NodeRunning {
			return true
		}
	}

	return wf.Spec.Suspend != nil && (*wf.Spec.Suspend)
}

func checkWfConditions(wf *argov1alpha1.Workflow, cond argov1alpha1.ConditionType) bool {
	for _, condition := range wf.Status.Conditions {
		if condition.Type == cond {
			return condition.Status == metav1.ConditionTrue
		}
	}
	return false
}

func (a *ArgoWorkflow) getEntryTemp(wf *argov1alpha1.Workflow) *argov1alpha1.Template {
	entry := wf.Spec.Entrypoint
	if entry == "" {
		return nil
	}

	temps := wf.Spec.Templates
	for _, temp := range temps {
		if temp.Name == entry {
			return &temp
		}
	}
	return nil
}

func (a *ArgoWorkflow) Resources(ctx context.Context, obj client.Object) v1.ResourceList {
	wf, ok := obj.(*argov1alpha1.Workflow)
	if !ok {
		return v1.ResourceList{}
	}

	rl := v1.ResourceList{}
	if res := wf.Annotations["koord-queue/min-resources"]; res != "" {
		rl = make(v1.ResourceList)
		var err error
		if IsJSONStr(res) {
			err = json.Unmarshal([]byte(res), &rl)
			if err == nil {
				return rl
			}
		} else {
			err = yaml.Unmarshal([]byte(res), &rl)
			if err == nil {
				return rl
			}
		}
		klog.ErrorS(err, "failed to unmarshal min resources")
	}

	return rl
}

func (a *ArgoWorkflow) GetPodSetName(ownerName string, p *v1.Pod) string {
	return "default"
}

func (a *ArgoWorkflow) PodSet(ctx context.Context, obj client.Object) []v1beta1.PodSet {
	wf, ok := obj.(*argov1alpha1.Workflow)
	if !ok {
		return []v1beta1.PodSet{}
	}

	rl := v1.ResourceList{}
	if res := wf.Annotations["koord-queue/min-resources"]; res != "" {
		rl = make(v1.ResourceList)
		var err error
		if IsJSONStr(res) {
			err = json.Unmarshal([]byte(res), &rl)
		} else {
			err = yaml.Unmarshal([]byte(res), &rl)
		}
		if err != nil {
			klog.ErrorS(err, "failed to unmarshal min resources")
		}
	}

	return []v1beta1.PodSet{{
		Name:  "default",
		Count: 1,
		Template: v1.PodTemplateSpec{
			Spec: v1.PodSpec{
				Containers: []v1.Container{{
					Name:  "default",
					Image: "non-exist",
					Resources: v1.ResourceRequirements{
						Requests: rl,
						Limits:   rl,
					},
				}},
			},
		},
	}}
}

func (a *ArgoWorkflow) Priority(ctx context.Context, obj client.Object) (string, *int32) {
	wf, ok := obj.(*argov1alpha1.Workflow)
	if !ok {
		return "", nil
	}
	if wf.Spec.Priority != nil {
		return "", wf.Spec.Priority
	}
	entryTmpl := a.getEntryTemp(wf)
	if entryTmpl == nil {
		return "", nil
	}
	return entryTmpl.PriorityClassName, nil
}

func (a *ArgoWorkflow) QueueUnitSuffix() string {
	return ""
}

func (a *ArgoWorkflow) GetJobStatus(ctx context.Context, obj client.Object, client client.Client) (framework.JobStatus, time.Time) {
	wf, ok := obj.(*argov1alpha1.Workflow)
	if !ok {
		return framework.Created, time.Now()
	}
	if !checkWfConditions(wf, argov1alpha1.ConditionType("Enqueued")) {
		return framework.Created, time.Now()
	}

	if isWorkflowSuspend(wf) {
		return framework.Queuing, time.Now()
	}

	if wf.Status.Phase == argov1alpha1.WorkflowSucceeded {
		return framework.Succeeded, time.Now()
	}
	if wf.Status.Phase == argov1alpha1.WorkflowError || wf.Status.Phase == argov1alpha1.WorkflowFailed {
		return framework.Failed, time.Now()
	}
	return framework.Running, time.Now()
}

func (a *ArgoWorkflow) Enqueue(ctx context.Context, obj client.Object, cli client.Client) error {
	wf, ok := obj.(*argov1alpha1.Workflow)
	if !ok {
		return nil
	}

	if len(wf.Annotations) == 0 {
		wf.Annotations = map[string]string{}
	}
	wf.Annotations["koord-queue/job-has-enqueued"] = "true"
	wf.Annotations["koord-queue/job-enqueue-timestamp"] = time.Now().String()
	wf.Status.Conditions = append(wf.Status.Conditions, argov1alpha1.Condition{
		Type:    argov1alpha1.ConditionType("Enqueued"),
		Status:  metav1.ConditionTrue,
		Message: fmt.Sprintf("%s is enqueued", wf.Name),
	})
	if err := a.c.Update(ctx, wf); err != nil {
		return err
	}
	return nil
}

func (a *ArgoWorkflow) Suspend(ctx context.Context, obj client.Object, cli client.Client) error {
	wf, ok := obj.(*argov1alpha1.Workflow)
	if !ok {
		return nil
	}
	if isWorkflowSuspend(wf) {
		return nil
	}
	wf.Spec.Suspend = ptr.To(true)
	return a.c.Update(ctx, wf)
}

func (a *ArgoWorkflow) Resume(ctx context.Context, obj client.Object, cli client.Client) error {
	wf, ok := obj.(*argov1alpha1.Workflow)
	if !ok {
		return nil
	}
	if !isWorkflowSuspend(wf) {
		return nil
	}
	wf.Spec.Suspend = ptr.To(false)
	return a.c.Update(ctx, wf)
}

func (a *ArgoWorkflow) ManagedByQueue(ctx context.Context, obj client.Object) bool {
	wf := obj.(*argov1alpha1.Workflow)
	for _, template := range wf.Spec.Templates {
		if template.Name == "koord-queue-suspend" && template.Suspend != nil {
			return true
		}
	}
	for _, storedTemp := range wf.Status.StoredTemplates {
		if storedTemp.Name == "koord-queue-suspend" && storedTemp.Suspend != nil {
			return true
		}
	}
	return false
}

func (a *ArgoWorkflow) GetRelatedQueueUnit(ctx context.Context, obj client.Object, client client.Client) (*v1alpha1.QueueUnit, error) {
	var qu = &v1alpha1.QueueUnit{}
	err := client.Get(context.Background(), types.NamespacedName{Namespace: obj.GetNamespace(), Name: obj.GetName()}, qu)
	return qu, err
}

func (a *ArgoWorkflow) GetRelatedJob(ctx context.Context, qu *v1alpha1.QueueUnit, client client.Client) client.Object {
	object := a.Object()
	client.Get(context.Background(), types.NamespacedName{Namespace: qu.GetNamespace(), Name: qu.GetName()}, object)
	return object
}

func (a *ArgoWorkflow) SupportReservation() (framework.GenericReservationJobExtension, bool) {
	// TODO: 如果支持 Reservation，则返回具体实现
	return nil, false
}

func (a *ArgoWorkflow) SupportNetworkAware() (framework.NetworkAwareJobExtension, bool) {
	// TODO: 如果支持 NetworkAware，则返回具体实现
	return nil, false
}

func NewWFReconciler(cli client.Client, config *rest.Config, scheme *runtime.Scheme, managedAllJobs bool, args string) framework.JobHandle {
	j := &ArgoWorkflow{c: cli, f: func(ctx context.Context, namespaceName types.NamespacedName) ([]*v1.Pod, error) {
		qu := v1alpha1.QueueUnit{}
		if err := cli.Get(ctx, namespaceName, &qu); err != nil {
			return nil, err
		}
		po := v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Annotations: map[string]string{util.RelatedQueueUnitAnnoKey: qu.Namespace + "/" + qu.Name, util.RelatedPodSetAnnoKey: "default"},
			},
			Spec: qu.Spec.PodSets[0].Template.Spec,
		}
		po.Spec.NodeName = "mocked-node"
		return []*v1.Pod{&po}, nil
	}}
	argov1alpha1.AddToScheme(scheme)
	extension := framework.NewGenericJobExtensionWithJob(j, j.ManagedByQueue)
	return framework.NewJobHandle(0, 0, extension, false)
}
