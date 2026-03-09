package elasticquotav1alpha1

import (
	"encoding/json"
	"testing"

	queuev1 "github.com/kube-queue/api/pkg/apis/scheduling/v1alpha1"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"

	"github.com/kube-queue/kube-queue/pkg/utils"
)

func TestEquals(t *testing.T) {
	type args struct {
		a map[v1.ResourceName]int64
		b map[v1.ResourceName]int64
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "key not found",
			args: args{
				a: map[v1.ResourceName]int64{
					"cpu": 1,
					"gpu": 1,
				},
				b: map[v1.ResourceName]int64{
					"cpu": 1,
					"mem": 1,
				},
			},
			want: false,
		},
		{
			name: "length not equal",
			args: args{
				a: map[v1.ResourceName]int64{
					"cpu": 1,
				},
				b: map[v1.ResourceName]int64{
					"cpu": 1,
					"mem": 1,
				},
			},
			want: false,
		},
		{
			name: "value not equal",
			args: args{
				a: map[v1.ResourceName]int64{
					"cpu": 1,
				},
				b: map[v1.ResourceName]int64{
					"cpu": 2,
				},
			},
			want: false,
		},
		{
			name: "normal",
			args: args{
				a: map[v1.ResourceName]int64{
					"cpu": 1,
				},
				b: map[v1.ResourceName]int64{
					"cpu": 1,
				},
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, Equals(tt.args.a, tt.args.b), "Equals(%v, %v)", tt.args.a, tt.args.b)
			klog.Info(printResourceList(tt.args.a))
		})
	}
}

func TestRemoveZeros(t *testing.T) {
	type args struct {
		a map[v1.ResourceName]int64
	}
	tests := []struct {
		name string
		args args
		want map[v1.ResourceName]int64
	}{
		{
			name: "normal",
			args: args{
				a: map[v1.ResourceName]int64{
					"cpu": 1,
					"gpu": 0,
				},
			},
			want: map[v1.ResourceName]int64{
				"cpu": 1,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, RemoveZeros(tt.args.a), "RemoveZeros(%v)", tt.args.a)
		})
	}
}

func Test_updateQueueAnnotation(t *testing.T) {
	type args struct {
		q   *queuev1.Queue
		k   string
		res map[v1.ResourceName]int64
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "normal",
			args: args{
				q: &queuev1.Queue{
					ObjectMeta: metav1.ObjectMeta{
						Annotations: map[string]string{},
					},
				},
				k: "test",
				res: map[v1.ResourceName]int64{
					"cpu":    1,
					"memory": 2,
				},
			},
			want: "{\"cpu\":1,\"memory\":2}",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_ = updateQueueAnnotation(tt.args.q, tt.args.k, tt.args.res)
			assert.Equal(t, tt.want, tt.args.q.Annotations[tt.args.k])
		})
	}
}

func Test_isQueueAnnotationDiff(t *testing.T) {
	type args struct {
		q   *queuev1.Queue
		key string
		res map[v1.ResourceName]int64
	}
	tests := []struct {
		name  string
		args  args
		want  bool
		want1 map[v1.ResourceName]int64
	}{
		{
			name: "not diff",
			args: args{
				q: &queuev1.Queue{
					ObjectMeta: metav1.ObjectMeta{
						Annotations: map[string]string{
							"k": "{\"cpu\":1,\"memory\":2}",
						},
					},
				},
				key: "k",
				res: map[v1.ResourceName]int64{
					"cpu":    1,
					"memory": 2,
					"gpu":    0,
				},
			},
			want: false,
			want1: map[v1.ResourceName]int64{
				"cpu":    1,
				"memory": 2,
			},
		},
		{
			name: "diff",
			args: args{
				q: &queuev1.Queue{
					ObjectMeta: metav1.ObjectMeta{
						Annotations: map[string]string{
							"k": "{\"cpu\":1,\"memory\":2}",
						},
					},
				},
				key: "k",
				res: map[v1.ResourceName]int64{
					"cpu":    1,
					"memory": 2,
					"gpu":    2,
					"tt":     0,
				},
			},
			want: true,
			want1: map[v1.ResourceName]int64{
				"cpu":    1,
				"memory": 2,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1, _ := isQueueAnnotationDiff(tt.args.q, tt.args.key, tt.args.res)
			assert.Equalf(t, tt.want, got, "isQueueAnnotationDiff(%v, %v, %v)", tt.args.q, tt.args.key, tt.args.res)
			assert.Equalf(t, tt.want1, got1, "isQueueAnnotationDiff(%v, %v, %v)", tt.args.q, tt.args.key, tt.args.res)
		})
	}
}

func Test_updateQueueStatusIfChanged(t *testing.T) {
	tests := []struct {
		name    string
		eq      *queuev1.Queue
		summary *ElasticQuotaDebugInfo
		wantEQ  *queuev1.Queue
	}{
		{
			name: "full sync",
			eq:   &queuev1.Queue{},
			summary: &ElasticQuotaDebugInfo{
				Used:                   MakeResourceList().CPU(1).Mem(50).Obj(),
				GuaranteedUsed:         MakeResourceList().CPU(2).Mem(50).Obj(),
				SelfUsed:               MakeResourceList().CPU(3).Mem(50).Obj(),
				SelfGuaranteedUsed:     MakeResourceList().CPU(4).Mem(50).Obj(),
				ChildrenUsed:           MakeResourceList().CPU(5).Mem(50).Obj(),
				ChildrenGuaranteedUsed: MakeResourceList().CPU(6).Mem(50).Obj(),
			},
			wantEQ: &queuev1.Queue{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						utils.QueueUsed:                   "{\"cpu\":1,\"memory\":50}",
						utils.QueueGuaranteedUsed:         "{\"cpu\":2,\"memory\":50}",
						utils.QueueSelfUsed:               "{\"cpu\":3,\"memory\":50}",
						utils.QueueSelfGuaranteedUsed:     "{\"cpu\":4,\"memory\":50}",
						utils.QueueChildrenUsed:           "{\"cpu\":5,\"memory\":50}",
						utils.QueueChildrenGuaranteedUsed: "{\"cpu\":6,\"memory\":50}",
					},
				},
			},
		},
		{
			name: "diff sync",
			eq: &queuev1.Queue{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						utils.QueueUsed:                   "{\"cpu\":1,\"memory\":50}",
						utils.QueueGuaranteedUsed:         "{\"cpu\":2,\"memory\":50}",
						utils.QueueSelfUsed:               "{\"cpu\":3,\"memory\":50}",
						utils.QueueSelfGuaranteedUsed:     "{\"cpu\":4,\"memory\":50}",
						utils.QueueChildrenUsed:           "{\"cpu\":5,\"memory\":50}",
						utils.QueueChildrenGuaranteedUsed: "{\"cpu\":6,\"memory\":50}",
					},
				},
			},
			summary: &ElasticQuotaDebugInfo{
				Used:                   MakeResourceList().CPU(100).Mem(50).Obj(),
				GuaranteedUsed:         MakeResourceList().CPU(2).Mem(50).Obj(),
				SelfUsed:               MakeResourceList().CPU(300).Mem(50).Obj(),
				SelfGuaranteedUsed:     MakeResourceList().CPU(4).Mem(50).Obj(),
				ChildrenUsed:           MakeResourceList().CPU(500).Mem(50).Obj(),
				ChildrenGuaranteedUsed: MakeResourceList().CPU(6).Mem(50).Obj(),
			},
			wantEQ: &queuev1.Queue{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						utils.QueueUsed:                   "{\"cpu\":100,\"memory\":50}",
						utils.QueueGuaranteedUsed:         "{\"cpu\":2,\"memory\":50}",
						utils.QueueSelfUsed:               "{\"cpu\":300,\"memory\":50}",
						utils.QueueSelfGuaranteedUsed:     "{\"cpu\":4,\"memory\":50}",
						utils.QueueChildrenUsed:           "{\"cpu\":500,\"memory\":50}",
						utils.QueueChildrenGuaranteedUsed: "{\"cpu\":6,\"memory\":50}",
					},
				},
			},
		},
		{
			name: "no sync",
			eq: &queuev1.Queue{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						utils.QueueUsed:                   "{\"cpu\":1,\"memory\":50}",
						utils.QueueGuaranteedUsed:         "{\"cpu\":2,\"memory\":50}",
						utils.QueueSelfUsed:               "{\"cpu\":3,\"memory\":50}",
						utils.QueueSelfGuaranteedUsed:     "{\"cpu\":4,\"memory\":50}",
						utils.QueueChildrenUsed:           "{\"cpu\":5,\"memory\":50}",
						utils.QueueChildrenGuaranteedUsed: "{\"cpu\":6,\"memory\":50}",
					},
				},
			},
			summary: &ElasticQuotaDebugInfo{
				Used:                   MakeResourceList().CPU(1).Mem(50).Obj(),
				GuaranteedUsed:         MakeResourceList().CPU(2).Mem(50).Obj(),
				SelfUsed:               MakeResourceList().CPU(3).Mem(50).Obj(),
				SelfGuaranteedUsed:     MakeResourceList().CPU(4).Mem(50).Obj(),
				ChildrenUsed:           MakeResourceList().CPU(5).Mem(50).Obj(),
				ChildrenGuaranteedUsed: MakeResourceList().CPU(6).Mem(50).Obj(),
			},
			wantEQ: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			newEQ, err := updateQueueStatusIfChanged(tt.eq, tt.summary, true)
			assert.NoError(t, err)
			if tt.wantEQ == nil {
				assert.Nil(t, newEQ)
				return
			}
			for k, v := range tt.wantEQ.Annotations {
				var expect map[v1.ResourceName]int64
				assert.NoError(t, json.Unmarshal([]byte(v), &expect))
				var got map[v1.ResourceName]int64
				v = newEQ.Annotations[k]
				assert.NoError(t, json.Unmarshal([]byte(v), &got))
				assert.Equal(t, expect, got)
			}
		})
	}
}

type resourceWrapper struct{ v1.ResourceList }

func MakeResourceList() *resourceWrapper {
	return &resourceWrapper{v1.ResourceList{}}
}

func (r *resourceWrapper) CPU(val int64) *resourceWrapper {
	r.ResourceList[v1.ResourceCPU] = *resource.NewQuantity(val, resource.DecimalSI)
	return r
}

func (r *resourceWrapper) Mem(val int64) *resourceWrapper {
	r.ResourceList[v1.ResourceMemory] = *resource.NewQuantity(val, resource.DecimalSI)
	return r
}

func (r *resourceWrapper) GPU(val int64) *resourceWrapper {
	r.ResourceList["nvidia.com/gpu"] = *resource.NewQuantity(val, resource.DecimalSI)
	return r
}

func (r *resourceWrapper) Obj() map[v1.ResourceName]int64 {
	return utils.TransResourceList(r.ResourceList)
}
