package controllers

import (
	"context"
	"errors"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/koordinator-sh/koord-queue/pkg/apis/scheduling/v1alpha1"
	versionedfake "github.com/koordinator-sh/koord-queue/pkg/client/clientset/versioned/fake"
	externalv1alpha1 "github.com/koordinator-sh/koord-queue/pkg/client/listers/scheduling/v1alpha1"
	"github.com/koordinator-sh/koord-queue/pkg/test/testutils/queueunits"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/klog/v2/klogr"
	"sigs.k8s.io/kueue/apis/kueue/v1beta1"
)

func Test_updateQueueUnitStatus(t *testing.T) {
	type args struct {
		queueUnit *v1alpha1.QueueUnit
	}
	tests := []struct {
		name      string
		args      args
		want      bool
		wantPhase v1alpha1.QueueUnitPhase
	}{
		{
			name: "rejected by admission check controllers",
			args: args{
				queueUnit: queueunits.MakeQueueUnit("q1", "ns1").
					AdmissionCheck("ad1", v1beta1.CheckStateReady).
					AdmissionCheck("ad2", v1beta1.CheckStateReady).
					AdmissionCheck("ad3", v1beta1.CheckStateRejected).QueueUnit(),
			},
			want:      true,
			wantPhase: v1alpha1.Backoff,
		},
		{
			name: "marked ready by admission check controllers",
			args: args{
				queueUnit: queueunits.MakeQueueUnit("q1", "ns1").
					AdmissionCheck("ad1", v1beta1.CheckStateReady).
					AdmissionCheck("ad2", v1beta1.CheckStateReady).
					AdmissionCheck("ad3", v1beta1.CheckStateReady).QueueUnit(),
			},
			want:      true,
			wantPhase: v1alpha1.Dequeued,
		},
		{
			name: "some of the admission checks are ready and others are pending",
			args: args{
				queueUnit: queueunits.MakeQueueUnit("q1", "ns1").Phase(v1alpha1.Reserved).
					AdmissionCheck("ad1", v1beta1.CheckStateReady).
					AdmissionCheck("ad2", v1beta1.CheckStatePending).
					AdmissionCheck("ad3", v1beta1.CheckStateReady).QueueUnit(),
			},
			want:      false,
			wantPhase: v1alpha1.Reserved,
		},
		{
			name: "some of the admission checks are ready and others are pending",
			args: args{
				queueUnit: queueunits.MakeQueueUnit("q1", "ns1").
					AdmissionCheck("ad1", v1beta1.CheckStateReady).
					AdmissionCheck("ad2", v1beta1.CheckStatePending).
					AdmissionCheck("ad3", v1beta1.CheckStateReady).QueueUnit(),
			},
			want:      false,
			wantPhase: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := updateQueueUnitStatus(tt.args.queueUnit, false, klogr.New()); got != tt.want {
				t.Errorf("updateQueueUnitStatus() = %v, want %v", got, tt.want)
			}
			if diff := cmp.Diff(tt.wantPhase, tt.args.queueUnit.Status.Phase); diff != "" {
				t.Errorf("unexpected queue unit phase: %v", diff)
			}
		})
	}
}

// FakeQueueUnitLister implements externalv1alpha1.QueueUnitLister interface for testing
type FakeQueueUnitLister struct {
	queueUnits map[string]*v1alpha1.QueueUnit
	err        error
}

func (f *FakeQueueUnitLister) QueueUnits(namespace string) externalv1alpha1.QueueUnitNamespaceLister {
	return &FakeQueueUnitNamespaceLister{
		queueUnits: f.queueUnits,
		err:        f.err,
		namespace:  namespace,
	}
}

func (f *FakeQueueUnitLister) List(selector labels.Selector) ([]*v1alpha1.QueueUnit, error) {
	return nil, nil
}

// FakeQueueUnitNamespaceLister implements externalv1alpha1.QueueUnitNamespaceLister interface for testing
type FakeQueueUnitNamespaceLister struct {
	queueUnits map[string]*v1alpha1.QueueUnit
	err        error
	namespace  string
}

func (f *FakeQueueUnitNamespaceLister) Get(name string) (*v1alpha1.QueueUnit, error) {
	if f.err != nil {
		return nil, f.err
	}
	key := f.namespace + "/" + name
	if qu, exists := f.queueUnits[key]; exists {
		return qu, nil
	}
	return nil, errors.New("not found")
}

func (f *FakeQueueUnitNamespaceLister) List(selector labels.Selector) ([]*v1alpha1.QueueUnit, error) {
	return nil, nil
}

func TestQueueUnitController_reconcile(t *testing.T) {
	tests := []struct {
		name                    string
		key                     string
		queueUnits              map[string]*v1alpha1.QueueUnit
		listError               error
		clientError             error
		enableStrictDequeueMode bool
		wantRequeue             bool
		wantErr                 bool
		wantPhase               v1alpha1.QueueUnitPhase
	}{
		{
			name:        "invalid key format",
			key:         "invalid-key",
			wantRequeue: false,
			wantErr:     false,
		},
		{
			name:        "queue unit not found",
			key:         "ns1/qu1",
			queueUnits:  map[string]*v1alpha1.QueueUnit{},
			wantRequeue: false,
			wantErr:     false,
		},
		{
			name:        "list error",
			key:         "ns1/qu1",
			listError:   errors.New("list error"),
			wantRequeue: false,
			wantErr:     false,
		},
		{
			name: "queue unit not in reserved phase",
			key:  "ns1/qu1",
			queueUnits: map[string]*v1alpha1.QueueUnit{
				"ns1/qu1": queueunits.MakeQueueUnit("qu1", "ns1").
					Phase(v1alpha1.Enqueued).QueueUnit(),
			},
			wantRequeue: false,
			wantErr:     false,
		},
		{
			name: "no update needed",
			key:  "ns1/qu1",
			queueUnits: map[string]*v1alpha1.QueueUnit{
				"ns1/qu1": queueunits.MakeQueueUnit("qu1", "ns1").
					Phase(v1alpha1.Reserved).
					AdmissionCheck("ad1", v1beta1.CheckStateReady).
					AdmissionCheck("ad2", v1beta1.CheckStatePending).QueueUnit(),
			},
			wantRequeue: false,
			wantErr:     false,
		},
		{
			name: "update to dequeued",
			key:  "ns1/qu1",
			queueUnits: map[string]*v1alpha1.QueueUnit{
				"ns1/qu1": queueunits.MakeQueueUnit("qu1", "ns1").
					Phase(v1alpha1.Reserved).
					AdmissionCheck("ad1", v1beta1.CheckStateReady).
					AdmissionCheck("ad2", v1beta1.CheckStateReady).QueueUnit(),
			},
			wantPhase:   v1alpha1.Dequeued,
			wantRequeue: false,
			wantErr:     false,
		},
		{
			name: "update to backoff due to rejected admission check",
			key:  "ns1/qu1",
			queueUnits: map[string]*v1alpha1.QueueUnit{
				"ns1/qu1": queueunits.MakeQueueUnit("qu1", "ns1").
					Phase(v1alpha1.Reserved).
					AdmissionCheck("ad1", v1beta1.CheckStateReady).
					AdmissionCheck("ad2", v1beta1.CheckStateRejected).QueueUnit(),
			},
			wantPhase:   v1alpha1.Backoff,
			wantRequeue: false,
			wantErr:     false,
		},
		{
			name: "update to sched ready with strict dequeue mode",
			key:  "ns1/qu1",
			queueUnits: map[string]*v1alpha1.QueueUnit{
				"ns1/qu1": queueunits.MakeQueueUnit("qu1", "ns1").
					Phase(v1alpha1.Reserved).
					AdmissionCheck("ad1", v1beta1.CheckStateReady).
					AdmissionCheck("ad2", v1beta1.CheckStateReady).QueueUnit(),
			},
			enableStrictDequeueMode: true,
			wantPhase:               v1alpha1.SchedReady,
			wantRequeue:             false,
			wantErr:                 false,
		},
		{
			name: "client update error",
			key:  "ns1/qu1",
			queueUnits: map[string]*v1alpha1.QueueUnit{
				"ns1/qu1": queueunits.MakeQueueUnit("qu1", "ns1").
					Phase(v1alpha1.Reserved).
					AdmissionCheck("ad1", v1beta1.CheckStateReady).
					AdmissionCheck("ad2", v1beta1.CheckStateReady).QueueUnit(),
			},
			clientError: errors.New("update error"),
			wantRequeue: false,
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create fake client
			objects := make([]runtime.Object, 0)
			for _, qu := range tt.queueUnits {
				objects = append(objects, qu)
			}
			fakeClient := versionedfake.NewSimpleClientset(objects...)

			if tt.clientError != nil {
				fakeClient.PrependReactor("update", "queueunits", func(action k8stesting.Action) (handled bool, ret runtime.Object, err error) {
					return true, nil, tt.clientError
				})
			}

			// Create fake lister
			fakeLister := &FakeQueueUnitLister{
				queueUnits: tt.queueUnits,
				err:        tt.listError,
			}

			// Create controller
			controller := &QueueUnitController{
				queueUnitClient:         fakeClient,
				queueUnitLister:         fakeLister,
				enableStrictDequeueMode: tt.enableStrictDequeueMode,
			}
			controller.logger = klogr.New()

			// Call reconcile
			got, err := controller.reconcile(tt.key)

			// Check error
			if (err != nil) != tt.wantErr {
				t.Errorf("QueueUnitController.reconcile() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			// Check requeue
			if got.Requeue != tt.wantRequeue {
				t.Errorf("QueueUnitController.reconcile() = %v, want %v", got.Requeue, tt.wantRequeue)
			}

			// Check phase if needed
			if tt.wantPhase != "" {
				// Get the updated queue unit from the fake client
				updatedQU, err := fakeClient.SchedulingV1alpha1().QueueUnits("ns1").Get(context.Background(), "qu1", metav1.GetOptions{})
				if err != nil {
					t.Errorf("Failed to get updated queue unit: %v", err)
					return
				}

				if updatedQU.Status.Phase != tt.wantPhase {
					t.Errorf("QueueUnitController.reconcile() phase = %v, want %v", updatedQU.Status.Phase, tt.wantPhase)
				}
			}
		})
	}
}
