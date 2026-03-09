package admissioncontroller

import (
	"context"
	"reflect"
	"sync"

	"github.com/go-logr/logr"
	"github.com/koordinator-sh/koord-queue/pkg/apis/scheduling/v1alpha1"
	"github.com/koordinator-sh/koord-queue/pkg/jobext/framework"
	"github.com/koordinator-sh/koord-queue/pkg/jobext/util"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/util/errors"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

type AdmissionControlelr struct {
	cli client.Client

	jm framework.JobManager
}

func (a *AdmissionControlelr) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := ctrl.LoggerFrom(ctx)
	qu := &v1alpha1.QueueUnit{}
	if err := a.cli.Get(ctx, req.NamespacedName, qu); err != nil {
		if apierrors.IsNotFound(err) {
			logger.Info("queueUnit not found")
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	relatedPods, err := a.jm.GetRelatedPods(ctx, qu)
	if err != nil {
		logger.Error(err, "failed to get related pods")
		return ctrl.Result{}, err
	}

	// if schedule-admission is false, change it to true, otherwise, change it to false
	admitted, unadmitted := a.updatePodStates(logger, qu, relatedPods)

	xorAdmit := a.buildXorAdmit(qu.Status.Admissions, admitted, unadmitted)

	allPods := []*corev1.Pod{}
	for _, pods := range xorAdmit {
		allPods = append(allPods, pods...)
	}
	if len(allPods) == 0 {
		return ctrl.Result{}, nil
	}
	parallism := 10
	if len(allPods) < parallism {
		parallism = len(allPods)
	}
	var wg sync.WaitGroup
	errCh := make(chan error, len(allPods))
	chunkSize := len(allPods) / parallism
	updatePodLabels := func(logger logr.Logger, qu *v1alpha1.QueueUnit, pods []*corev1.Pod) {
		defer wg.Done()
		for _, pod := range pods {
			oldPod := &corev1.Pod{TypeMeta: pod.TypeMeta, ObjectMeta: pod.ObjectMeta}
			newPod := &corev1.Pod{TypeMeta: pod.TypeMeta, ObjectMeta: pod.ObjectMeta}
			newPod.Labels = map[string]string{}
			for k := range pod.Labels {
				newPod.Labels[k] = pod.Labels[k]
			}
			if value := pod.Labels[util.SchedulerAdmissionLabelKey]; value != "" && value == "false" {
				newPod.Labels[util.SchedulerAdmissionLabelKey] = "true"
			} else {
				newPod.Labels[util.SchedulerAdmissionLabelKey] = "false"
			}
			if err := a.cli.Patch(ctx, newPod, client.MergeFrom(oldPod)); err != nil {
				errCh <- err
			}
		}
	}
	for i := 0; i < parallism; i++ {
		if i == parallism-1 {
			wg.Add(1)
			go updatePodLabels(logger, qu, allPods[i*chunkSize:])
		} else {
			wg.Add(1)
			go updatePodLabels(logger, qu, allPods[i*chunkSize:(i+1)*chunkSize])
		}
	}
	wg.Wait()
	close(errCh)
	errs := []error{}
	for err := range errCh {
		errs = append(errs, err)
	}

	return ctrl.Result{}, errors.NewAggregate(errs)
}

func (a *AdmissionControlelr) buildXorAdmit(admissions []v1alpha1.Admission, admitted, unadmitted map[string][]*corev1.Pod) (xorAdmit map[string][]*corev1.Pod) {
	xorAdmit = map[string][]*corev1.Pod{}
	for _, ads := range admissions {
		psName := ads.Name
		maxReplica := ads.Replicas
		currentReplicas := int64(len(admitted[psName]))
		if currentReplicas == maxReplica {
			continue
		} else if currentReplicas < maxReplica {
			unadmittedPods := unadmitted[psName]
			for currentReplicas < maxReplica && len(unadmittedPods) > 0 {
				if unadmittedPods[0].Spec.NodeName != "" {
					unadmittedPods = unadmittedPods[1:]
					continue
				}
				if _, ok := xorAdmit[psName]; !ok {
					xorAdmit[psName] = []*corev1.Pod{}
				}
				xorAdmit[psName] = append(xorAdmit[psName], unadmittedPods[0])
				unadmittedPods = unadmittedPods[1:]
				currentReplicas++
			}
		} else {
			admittedPods := admitted[psName]
			for currentReplicas > maxReplica && len(admittedPods) > 0 {
				if admittedPods[0].Spec.NodeName != "" {
					admittedPods = admittedPods[1:]
					continue
				}
				if _, ok := xorAdmit[psName]; !ok {
					xorAdmit[psName] = []*corev1.Pod{}
				}
				xorAdmit[psName] = append(xorAdmit[psName], admittedPods[0])
				admittedPods = admittedPods[1:]
				currentReplicas--
			}
		}
	}

	return
}

func (a *AdmissionControlelr) updatePodStates(logger logr.Logger, qu *v1alpha1.QueueUnit, pods []*corev1.Pod) (map[string][]*corev1.Pod, map[string][]*corev1.Pod) {
	admittedReplicas := map[string][]*corev1.Pod{}
	unAdmittedPods := map[string][]*corev1.Pod{}

	for _, po := range pods {
		podSetName := a.jm.GetPodSetName(context.Background(), qu, po)
		if podSetName == "" {
			logger.Info("pod has no related podset", "pod", po.Name)
			continue
		}
		if po.Status.Phase == corev1.PodFailed || po.Status.Phase == corev1.PodSucceeded {
			continue
		}
		if po.Spec.NodeName != "" {
			if _, ok := admittedReplicas[podSetName]; !ok {
				admittedReplicas[podSetName] = []*corev1.Pod{po}
			} else {
				admittedReplicas[podSetName] = append(admittedReplicas[podSetName], po)
			}
			continue
		}
		if ad := po.Labels[util.SchedulerAdmissionLabelKey]; ad != "" && ad == "false" {
			if _, ok := unAdmittedPods[podSetName]; !ok {
				unAdmittedPods[podSetName] = []*corev1.Pod{po}
			} else {
				unAdmittedPods[podSetName] = append(unAdmittedPods[podSetName], po)
			}
		} else {
			if _, ok := admittedReplicas[podSetName]; !ok {
				admittedReplicas[podSetName] = []*corev1.Pod{po}
			} else {
				admittedReplicas[podSetName] = append(admittedReplicas[podSetName], po)
			}
		}
	}
	return admittedReplicas, unAdmittedPods
}

func NewAdmissionController(cli client.Client, jhm map[string]framework.JobHandle) *AdmissionControlelr {
	ac := &AdmissionControlelr{
		cli: cli,

		jm: framework.NewManager(cli, jhm),
	}
	return ac
}

func (a *AdmissionControlelr) SetupWithManager(mgr ctrl.Manager) error {
	build := ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.QueueUnit{}, builder.WithPredicates(predicate.Funcs{
			CreateFunc: func(ce event.CreateEvent) bool { return true },
			UpdateFunc: func(ue event.UpdateEvent) bool {
				nq, err := util.ToQueueUnit(ue.ObjectNew)
				if err != nil {
					return false
				}
				oq, err := util.ToQueueUnit(ue.ObjectOld)
				if err != nil {
					return false
				}
				return !reflect.DeepEqual(nq.Status.Admissions, oq.Status.Admissions)
			},
			DeleteFunc: func(de event.DeleteEvent) bool { return false }})).
		Watches(&corev1.Pod{}, buildPodEventhandler(a))
	return build.Complete(a)
}
