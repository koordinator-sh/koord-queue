package utils

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	queuev1 "github.com/kube-queue/api/pkg/apis/scheduling/v1alpha1"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta1"

	"github.com/kube-queue/kube-queue/pkg/framework/apis/elasticquota/scheduling/v1alpha1"
)

func TestUpdateUsage(t *testing.T) {
	{
		a := make(map[v1.ResourceName]int64)
		a["cpu"] = 10
		a["mem"] = 5

		b := make(v1.ResourceList)
		b["cpu"] = resource.MustParse("3")
		b["xx"] = resource.MustParse("4")

		UpdateUsage(a, TransResourceList(b), 2)
		assert.Equal(t, a["cpu"], int64(16))
		assert.Equal(t, a["mem"], int64(5))
		assert.Equal(t, a["xx"], int64(8))
	}
}

func TestUpdateUsage2(t *testing.T) {
	{
		a := make(map[v1.ResourceName]int64)
		a["cpu"] = 10
		a["mem"] = 5

		b := make(v1.ResourceList)
		b["cpu"] = resource.MustParse("3")
		b["xx"] = resource.MustParse("4")

		UpdateUsage(a, TransResourceList(b), 2)
		assert.Equal(t, a["cpu"], int64(16))
		assert.Equal(t, a["mem"], int64(5))
		assert.Equal(t, a["xx"], int64(8))
	}
	{
		a := make(map[v1.ResourceName]int64)
		a["cpu"] = 10
		a["mem"] = 5

		b := make(v1.ResourceList)
		b["cpu"] = resource.MustParse("3")
		b["xx"] = resource.MustParse("4")

		UpdateUsage(a, TransResourceList(b), -2)
		assert.Equal(t, a["cpu"], int64(4))
		assert.Equal(t, a["mem"], int64(5))
		assert.Equal(t, a["xx"], int64(-8))
	}
}

func TestKey(t *testing.T) {
	q := &v1alpha1.ElasticQuota{}
	q.Name = "test1"

	assert.Equal(t, "test1", q.Name)
}

func TestTransResourceList(t *testing.T) {
	res := v1.ResourceList{"cpu": resource.MustParse("5")}
	result := TransResourceList(res)

	assert.Equal(t, 1, len(result))
	assert.Equal(t, result["cpu"], int64(5))
}

func TestIsQueueUnitSuspend(t *testing.T) {
	type args struct {
		unit *queuev1.QueueUnit
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "unit annotation empty",
			args: args{
				unit: &queuev1.QueueUnit{},
			},
			want: false,
		},
		{
			name: "unit annotation not has this key",
			args: args{
				unit: &queuev1.QueueUnit{
					ObjectMeta: metav1.ObjectMeta{
						Annotations: map[string]string{
							"tt": "t",
						},
					},
				},
			},
			want: false,
		},
		{
			name: "unit annotation has this key, value not true",
			args: args{
				unit: &queuev1.QueueUnit{
					ObjectMeta: metav1.ObjectMeta{
						Annotations: map[string]string{
							QueueSuspend: "false",
						},
					},
				},
			},
			want: false,
		},
		{
			name: "unit annotation has this key, value true",
			args: args{
				unit: &queuev1.QueueUnit{
					ObjectMeta: metav1.ObjectMeta{
						Annotations: map[string]string{
							QueueSuspend: "true",
						},
					},
				},
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, IsQueueUnitSuspend(tt.args.unit), "IsQueueUnitSuspend(%v)", tt.args.unit)
		})
	}
}

func TestIsQueueUnitAssigned(t *testing.T) {
	tests := []struct {
		name string
		qu   *queuev1.QueueUnit
		want bool
	}{
		{
			name: "has admissions",
			qu: &queuev1.QueueUnit{
				Status: queuev1.QueueUnitStatus{
					Admissions: []queuev1.Admission{
						{
							Name:     "test",
							Replicas: 1,
						},
					},
				},
			},
			want: true,
		},
		{
			name: "no admissions, Running phase",
			qu: &queuev1.QueueUnit{
				Status: queuev1.QueueUnitStatus{
					Phase: queuev1.Running,
				},
			},
			want: true,
		},
		{
			name: "no admissions, Dequeued phase",
			qu: &queuev1.QueueUnit{
				Status: queuev1.QueueUnitStatus{
					Phase: queuev1.Dequeued,
				},
			},
			want: true,
		},
		{
			name: "no admissions, Reserved phase",
			qu: &queuev1.QueueUnit{
				Status: queuev1.QueueUnitStatus{
					Phase: queuev1.Reserved,
				},
			},
			want: true,
		},
		{
			name: "no admissions, SchedReady phase",
			qu: &queuev1.QueueUnit{
				Status: queuev1.QueueUnitStatus{
					Phase: queuev1.SchedReady,
				},
			},
			want: true,
		},
		{
			name: "no admissions, Enqueued phase",
			qu: &queuev1.QueueUnit{
				Status: queuev1.QueueUnitStatus{
					Phase: queuev1.Enqueued,
				},
			},
			want: false,
		},
		{
			name: "no admissions, empty phase",
			qu: &queuev1.QueueUnit{
				Status: queuev1.QueueUnitStatus{
					Phase: "",
				},
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IsQueueUnitAssigned(tt.qu)
			assert.Equalf(t, tt.want, got, "IsQueueUnitAssigned(%v)", tt.qu)
		})
	}
}

func TestIsResourceReleased(t *testing.T) {
	tests := []struct {
		name string
		old  *queuev1.QueueUnit
		new  *queuev1.QueueUnit
		want bool
	}{
		{
			name: "different admission count - old has more",
			old: &queuev1.QueueUnit{
				Status: queuev1.QueueUnitStatus{
					Admissions: []queuev1.Admission{
						{Name: "test1", Replicas: 2},
						{Name: "test2", Replicas: 1},
					},
				},
			},
			new: &queuev1.QueueUnit{
				Status: queuev1.QueueUnitStatus{
					Admissions: []queuev1.Admission{
						{Name: "test1", Replicas: 2},
					},
				},
			},
			want: true,
		},
		{
			name: "different admission count - new has more",
			old: &queuev1.QueueUnit{
				Status: queuev1.QueueUnitStatus{
					Admissions: []queuev1.Admission{
						{Name: "test1", Replicas: 2},
					},
				},
			},
			new: &queuev1.QueueUnit{
				Status: queuev1.QueueUnitStatus{
					Admissions: []queuev1.Admission{
						{Name: "test1", Replicas: 2},
						{Name: "test2", Replicas: 1},
					},
				},
			},
			want: true,
		},
		{
			name: "same admission count, replica decreased",
			old: &queuev1.QueueUnit{
				Status: queuev1.QueueUnitStatus{
					Admissions: []queuev1.Admission{
						{Name: "test1", Replicas: 5},
						{Name: "test2", Replicas: 3},
					},
				},
			},
			new: &queuev1.QueueUnit{
				Status: queuev1.QueueUnitStatus{
					Admissions: []queuev1.Admission{
						{Name: "test1", Replicas: 3},
						{Name: "test2", Replicas: 3},
					},
				},
			},
			want: true,
		},
		{
			name: "same admission count, replica increased",
			old: &queuev1.QueueUnit{
				Status: queuev1.QueueUnitStatus{
					Admissions: []queuev1.Admission{
						{Name: "test1", Replicas: 2},
						{Name: "test2", Replicas: 3},
					},
				},
			},
			new: &queuev1.QueueUnit{
				Status: queuev1.QueueUnitStatus{
					Admissions: []queuev1.Admission{
						{Name: "test1", Replicas: 5},
						{Name: "test2", Replicas: 3},
					},
				},
			},
			want: false,
		},
		{
			name: "same admission count, same replicas",
			old: &queuev1.QueueUnit{
				Status: queuev1.QueueUnitStatus{
					Admissions: []queuev1.Admission{
						{Name: "test1", Replicas: 2},
						{Name: "test2", Replicas: 3},
					},
				},
			},
			new: &queuev1.QueueUnit{
				Status: queuev1.QueueUnitStatus{
					Admissions: []queuev1.Admission{
						{Name: "test1", Replicas: 2},
						{Name: "test2", Replicas: 3},
					},
				},
			},
			want: false,
		},
		{
			name: "no admissions in both",
			old: &queuev1.QueueUnit{
				Status: queuev1.QueueUnitStatus{
					Admissions: []queuev1.Admission{},
				},
			},
			new: &queuev1.QueueUnit{
				Status: queuev1.QueueUnitStatus{
					Admissions: []queuev1.Admission{},
				},
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IsResourceReleased(tt.old, tt.new)
			assert.Equalf(t, tt.want, got, "IsResourceReleased(%v, %v)", tt.old, tt.new)
		})
	}
}

func TestConvertFromStatusAdmissionToResource(t *testing.T) {
	// 测试用例1: 测试V1版本已出队的情况
	t.Run("V1VersionDequeued", func(t *testing.T) {
		qu := &queuev1.QueueUnit{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-qu",
				Namespace: "default",
			},
			Spec: queuev1.QueueUnitSpec{
				Resource: map[v1.ResourceName]resource.Quantity{
					v1.ResourceCPU:    resource.MustParse("2"),
					v1.ResourceMemory: resource.MustParse("4Gi"),
				},
			},
			Status: queuev1.QueueUnitStatus{
				Admissions: []queuev1.Admission{},
				Phase:      queuev1.Dequeued,
			},
		}

		ads := []queuev1.Admission{}
		result := ConvertFromStatusAdmissionToResource(qu, ads)

		expected := NewResource(qu.Spec.Resource)
		assert.True(t, result.Equal(expected))
	})

	// 测试用例2: 测试默认需求的情况
	t.Run("DefaultRequirement", func(t *testing.T) {
		qu := &queuev1.QueueUnit{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-qu",
				Namespace: "default",
			},
			Spec: queuev1.QueueUnitSpec{
				Request: map[v1.ResourceName]resource.Quantity{
					v1.ResourceCPU:    resource.MustParse("1"),
					v1.ResourceMemory: resource.MustParse("2Gi"),
				},
			},
		}

		ads := []queuev1.Admission{
			{
				Name:     KubeQueueDefaultRequirement,
				Replicas: 1,
			},
		}

		result := ConvertFromStatusAdmissionToResource(qu, ads)

		expected := NewResource(qu.Spec.Request)
		assert.True(t, result.Equal(expected))
	})

	// 测试用例3: 测试正常的PodSets情况
	t.Run("NormalPodSets", func(t *testing.T) {
		qu := &queuev1.QueueUnit{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-qu",
				Namespace: "default",
			},
			Spec: queuev1.QueueUnitSpec{
				PodSets: []kueue.PodSet{
					{
						Name:  "worker",
						Count: 2,
						Template: v1.PodTemplateSpec{
							Spec: v1.PodSpec{
								Containers: []v1.Container{
									{
										Name:  "container",
										Image: "nginx",
										Resources: v1.ResourceRequirements{
											Requests: v1.ResourceList{
												v1.ResourceCPU:    resource.MustParse("1"),
												v1.ResourceMemory: resource.MustParse("1Gi"),
											},
										},
									},
								},
							},
						},
					},
				},
				Resource: map[v1.ResourceName]resource.Quantity{
					v1.ResourceCPU:    resource.MustParse("3"),
					v1.ResourceMemory: resource.MustParse("5Gi"),
				},
			},
			Status: queuev1.QueueUnitStatus{
				Phase: queuev1.Dequeued,
			},
		}

		ads := []queuev1.Admission{
			{
				Name:     "worker",
				Replicas: 2,
			},
		}

		result := ConvertFromStatusAdmissionToResource(qu, ads)

		// 预期结果应该是2个worker pod的资源需求
		expectedResourceList := v1.ResourceList{
			v1.ResourceCPU:    resource.MustParse("2"),
			v1.ResourceMemory: resource.MustParse("2Gi"),
		}
		expected := NewResource(expectedResourceList)

		// 添加Spec.Resource中更大值的资源
		realtimeRes := NewResource(qu.Spec.Resource)
		for k, v := range realtimeRes.Resources {
			if expected.Resources[k] == 0 {
				expected.Resources[k] = v
			} else if v > expected.Resources[k] {
				expected.Resources[k] = v
			}
		}

		assert.True(t, result.Equal(expected))
	})

	// 测试用例4: 测试多个PodSets的情况
	t.Run("MultiplePodSets", func(t *testing.T) {
		qu := &queuev1.QueueUnit{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-qu",
				Namespace: "default",
			},
			Spec: queuev1.QueueUnitSpec{
				PodSets: []kueue.PodSet{
					{
						Name:  "worker",
						Count: 2,
						Template: v1.PodTemplateSpec{
							Spec: v1.PodSpec{
								Containers: []v1.Container{
									{
										Name:  "container",
										Image: "nginx",
										Resources: v1.ResourceRequirements{
											Requests: v1.ResourceList{
												v1.ResourceCPU:    resource.MustParse("1"),
												v1.ResourceMemory: resource.MustParse("1Gi"),
											},
										},
									},
								},
							},
						},
					},
					{
						Name:  "ps",
						Count: 1,
						Template: v1.PodTemplateSpec{
							Spec: v1.PodSpec{
								Containers: []v1.Container{
									{
										Name:  "container",
										Image: "nginx",
										Resources: v1.ResourceRequirements{
											Requests: v1.ResourceList{
												v1.ResourceCPU:    resource.MustParse("2"),
												v1.ResourceMemory: resource.MustParse("4Gi"),
											},
										},
									},
								},
							},
						},
					},
				},
				Resource: map[v1.ResourceName]resource.Quantity{
					v1.ResourceCPU:    resource.MustParse("5"),
					v1.ResourceMemory: resource.MustParse("8Gi"),
				},
			},
			Status: queuev1.QueueUnitStatus{
				Phase: queuev1.Dequeued,
			},
		}

		ads := []queuev1.Admission{
			{
				Name:     "worker",
				Replicas: 2,
			},
			{
				Name:     "ps",
				Replicas: 1,
			},
		}

		result := ConvertFromStatusAdmissionToResource(qu, ads)

		// 预期结果应该是2个worker + 1个ps pod的资源需求
		expectedResourceList := v1.ResourceList{
			v1.ResourceCPU:    resource.MustParse("4"),   // 2*1 + 1*2
			v1.ResourceMemory: resource.MustParse("6Gi"), // 2*1Gi + 1*4Gi
		}
		expected := NewResource(expectedResourceList)

		// 添加Spec.Resource中更大值的资源
		realtimeRes := NewResource(qu.Spec.Resource)
		for k, v := range realtimeRes.Resources {
			if expected.Resources[k] == 0 {
				expected.Resources[k] = v
			} else if v > expected.Resources[k] {
				expected.Resources[k] = v
			}
		}

		assert.True(t, result.Equal(expected))
	})

	// 测试用例5: 测试空的Admissions情况
	t.Run("EmptyAdmissions", func(t *testing.T) {
		qu := &queuev1.QueueUnit{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-qu",
				Namespace: "default",
			},
			Spec: queuev1.QueueUnitSpec{
				PodSets: []kueue.PodSet{
					{
						Name:  "worker",
						Count: 2,
						Template: v1.PodTemplateSpec{
							Spec: v1.PodSpec{
								Containers: []v1.Container{
									{
										Name:  "container",
										Image: "nginx",
										Resources: v1.ResourceRequirements{
											Requests: v1.ResourceList{
												v1.ResourceCPU:    resource.MustParse("1"),
												v1.ResourceMemory: resource.MustParse("1Gi"),
											},
										},
									},
								},
							},
						},
					},
				},
				Resource: map[v1.ResourceName]resource.Quantity{
					v1.ResourceCPU:    resource.MustParse("3"),
					v1.ResourceMemory: resource.MustParse("5Gi"),
				},
			},
			Status: queuev1.QueueUnitStatus{
				Phase: queuev1.Dequeued,
			},
		}

		ads := []queuev1.Admission{}

		result := ConvertFromStatusAdmissionToResource(qu, ads)

		// 预期结果应该只包含Spec.Resource中的资源
		expected := NewResource(qu.Spec.Resource)
		assert.True(t, result.Equal(expected))
	})

	t.Run("Resources in Admission should be considered", func(t *testing.T) {
		qu := &queuev1.QueueUnit{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-qu",
				Namespace: "default",
			},
			Spec: queuev1.QueueUnitSpec{
				PodSets: []kueue.PodSet{
					{
						Name:  "worker",
						Count: 2,
						Template: v1.PodTemplateSpec{
							Spec: v1.PodSpec{
								Containers: []v1.Container{
									{
										Name:  "container",
										Image: "nginx",
										Resources: v1.ResourceRequirements{
											Requests: v1.ResourceList{
												v1.ResourceCPU:    resource.MustParse("1"),
												v1.ResourceMemory: resource.MustParse("1Gi"),
											},
										},
									},
								},
							},
						},
					},
				},
				Resource: map[v1.ResourceName]resource.Quantity{
					v1.ResourceCPU:    resource.MustParse("3"),
					v1.ResourceMemory: resource.MustParse("5Gi"),
				},
			},
			Status: queuev1.QueueUnitStatus{
				Admissions: []queuev1.Admission{{
					Name:     "worker",
					Replicas: 5,
					Resources: v1.ResourceList{
						v1.ResourceCPU:    resource.MustParse("5"),
						v1.ResourceMemory: resource.MustParse("5Gi"),
						"nvidia.com/gpu":  resource.MustParse("8"),
					},
				}},
			},
		}

		result := ConvertFromStatusAdmissionToResource(qu, qu.Status.Admissions)

		// 预期结果应该只包含Spec.Resource中的资源
		expected := &Resource{
			Resources: map[v1.ResourceName]int64{
				v1.ResourceCPU:    5000,
				v1.ResourceMemory: 5 * 1024 * 1024 * 1024,
				"nvidia.com/gpu":  8,
			},
		}
		if diff := cmp.Diff(expected.Resources, result.Resources); diff != "" {
			t.Errorf("ConvertFromStatusAdmissionToResource() failed (-want +got):\n%s", diff)
		}
	})
}
