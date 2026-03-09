package elasticquotatree

import (
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/sets"

	"github.com/koordinator-sh/koord-queue/pkg/framework/apis/elasticquota/scheduling/v1beta1"
	"github.com/koordinator-sh/koord-queue/pkg/utils"
)

func TestNewElasticQuotaTree(t *testing.T) {
	testCases := []struct {
		name string
		tree *v1beta1.ElasticQuotaTree
	}{
		{
			name: "quota with same node",
			tree: &v1beta1.ElasticQuotaTree{
				Spec: v1beta1.ElasticQuotaTreeSpec{Root: v1beta1.ElasticQuotaSpec{
					Children: []v1beta1.ElasticQuotaSpec{
						{Name: "test1", Namespaces: []string{"ns1"}},
						{Name: "test2", Children: []v1beta1.ElasticQuotaSpec{{Name: "test1", Namespaces: []string{"ns2"}}}},
					},
				}},
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			tree := newElasticQuotaTree(testCase.tree)
			info1 := tree.GetElasticQuotaInfoByNamespace("ns1")
			info2 := tree.GetElasticQuotaInfoByNamespace("ns2")
			assert.NotEqual(t, info1, info2, "two quotas with the same name and different parent nodes should co-exist")
		})
	}

}

func TestReserveResource(t *testing.T) {
	tests := []struct {
		name           string
		initialMin     v1.ResourceList
		initialMax     v1.ResourceList
		initialUsed    v1.ResourceList
		request        v1.ResourceList
		preemptible    bool
		removeJob      bool
		expectedUsed   v1.ResourceList
		expectedCount  int64
		expectedPCount int64
	}{
		{
			name: "Reserve non-preemptible resource",
			initialMin: v1.ResourceList{
				v1.ResourceCPU:    resource.MustParse("1"),
				v1.ResourceMemory: resource.MustParse("1Gi"),
			},
			initialMax: v1.ResourceList{
				v1.ResourceCPU:    resource.MustParse("4"),
				v1.ResourceMemory: resource.MustParse("4Gi"),
			},
			initialUsed: v1.ResourceList{
				v1.ResourceCPU:    resource.MustParse("2"),
				v1.ResourceMemory: resource.MustParse("2Gi"),
			},
			request: v1.ResourceList{
				v1.ResourceCPU:    resource.MustParse("1"),
				v1.ResourceMemory: resource.MustParse("1Gi"),
			},
			preemptible: false,
			removeJob:   false,
			expectedUsed: v1.ResourceList{
				v1.ResourceCPU:    resource.MustParse("3"),
				v1.ResourceMemory: resource.MustParse("3Gi"),
			},
			expectedCount:  0,
			expectedPCount: 0,
		},
		{
			name: "Unreserve non-preemptible resource",
			initialMin: v1.ResourceList{
				v1.ResourceCPU:    resource.MustParse("1"),
				v1.ResourceMemory: resource.MustParse("1Gi"),
			},
			initialMax: v1.ResourceList{
				v1.ResourceCPU:    resource.MustParse("4"),
				v1.ResourceMemory: resource.MustParse("4Gi"),
			},
			initialUsed: v1.ResourceList{
				v1.ResourceCPU:    resource.MustParse("2"),
				v1.ResourceMemory: resource.MustParse("2Gi"),
			},
			request: v1.ResourceList{
				v1.ResourceCPU:    resource.MustParse("-1"),
				v1.ResourceMemory: resource.MustParse("-1Gi"),
			},
			preemptible: false,
			removeJob:   false,
			expectedUsed: v1.ResourceList{
				v1.ResourceCPU:    resource.MustParse("1"),
				v1.ResourceMemory: resource.MustParse("1Gi"),
			},
			expectedCount:  0,
			expectedPCount: 0,
		},
		{
			name: "Reserve preemptible resource",
			initialMin: v1.ResourceList{
				v1.ResourceCPU:    resource.MustParse("1"),
				v1.ResourceMemory: resource.MustParse("1Gi"),
			},
			initialMax: v1.ResourceList{
				v1.ResourceCPU:    resource.MustParse("4"),
				v1.ResourceMemory: resource.MustParse("4Gi"),
			},
			initialUsed: v1.ResourceList{
				v1.ResourceCPU:    resource.MustParse("2"),
				v1.ResourceMemory: resource.MustParse("2Gi"),
			},
			request: v1.ResourceList{
				v1.ResourceCPU:    resource.MustParse("1"),
				v1.ResourceMemory: resource.MustParse("1Gi"),
			},
			preemptible: true,
			removeJob:   false,
			expectedUsed: v1.ResourceList{
				v1.ResourceCPU:    resource.MustParse("3"),
				v1.ResourceMemory: resource.MustParse("3Gi"),
			},
			expectedCount:  0,
			expectedPCount: 0, // 因为初始值为 0，所以减 1 变成 -1（注意应避免负值）
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 创建一个 ElasticQuotaInfo 实例
			eqi := newElasticQuotaInfo("test-quota", nil, tt.initialMin, tt.initialMax, tt.initialUsed, false)

			// 转换 request 到 utils.Resource
			req := utils.NewResource(tt.request)

			// 调用 unreserveResource
			eqi.reserveResource(req, nil, false, tt.removeJob, tt.preemptible)

			// 验证 Used 是否正确
			for rName, expectedValue := range tt.expectedUsed {
				if rName == "cpu" {
					if eqi.Used.Resources[rName] != expectedValue.MilliValue() {
						t.Errorf("Expected %v for resource %v in Used, got %v", expectedValue.MilliValue(), rName, eqi.Used.Resources[rName])
					}
				} else {
					if eqi.Used.Resources[rName] != expectedValue.Value() {
						t.Errorf("Expected %v for resource %v in Used, got %v", expectedValue.Value(), rName, eqi.Used.Resources[rName])
					}
				}
			}

			// 验证 Count 和 PreemptibleCount
			if tt.preemptible {
				if eqi.PreemptibleCount != tt.expectedPCount {
					t.Errorf("Expected PreemptibleCount to be %d, got %d", tt.expectedPCount, eqi.PreemptibleCount)
				}
			} else {
				if eqi.Count != tt.expectedCount {
					t.Errorf("Expected Count to be %d, got %d", tt.expectedCount, eqi.Count)
				}
			}
		})
	}
}

func TestElasticQuotaTree_GetElasticQuotaInfoByNamespace(t *testing.T) {
	// 创建测试用的 ElasticQuotaTree
	tree := &v1beta1.ElasticQuotaTree{
		Spec: v1beta1.ElasticQuotaTreeSpec{
			Root: v1beta1.ElasticQuotaSpec{
				Name: "root",
				Children: []v1beta1.ElasticQuotaSpec{
					{
						Name:       "test-quota",
						Namespaces: []string{"test-namespace"},
						Min: v1.ResourceList{
							v1.ResourceCPU:    resource.MustParse("1"),
							v1.ResourceMemory: resource.MustParse("1Gi"),
						},
						Max: v1.ResourceList{
							v1.ResourceCPU:    resource.MustParse("2"),
							v1.ResourceMemory: resource.MustParse("2Gi"),
						},
					},
				},
			},
		},
	}

	eqTree := NewElasticQuotaTree(tree)

	// 测试存在的命名空间
	info := eqTree.GetElasticQuotaInfoByNamespace("test-namespace")
	assert.NotNil(t, info)
	assert.Equal(t, "test-quota", info.Name)

	// 测试不存在的命名空间
	info = eqTree.GetElasticQuotaInfoByNamespace("non-existent-namespace")
	assert.Nil(t, info)
}

func TestElasticQuotaTree_GetElasticQuotaInfoByName(t *testing.T) {
	// 创建测试用的 ElasticQuotaTree
	tree := &v1beta1.ElasticQuotaTree{
		Spec: v1beta1.ElasticQuotaTreeSpec{
			Root: v1beta1.ElasticQuotaSpec{
				Name: "root",
				Children: []v1beta1.ElasticQuotaSpec{
					{
						Name:       "test-quota",
						Namespaces: []string{"test-namespace"},
						Min: v1.ResourceList{
							v1.ResourceCPU:    resource.MustParse("1"),
							v1.ResourceMemory: resource.MustParse("1Gi"),
						},
						Max: v1.ResourceList{
							v1.ResourceCPU:    resource.MustParse("2"),
							v1.ResourceMemory: resource.MustParse("2Gi"),
						},
					},
				},
			},
		},
	}

	eqTree := NewElasticQuotaTree(tree)

	// 测试存在的配额名称
	info := eqTree.GetElasticQuotaInfoByName("test-quota")
	assert.NotNil(t, info)
	assert.Equal(t, "test-quota", info.Name)

	// 测试不存在的配额名称
	info = eqTree.GetElasticQuotaInfoByName("non-existent-quota")
	assert.Nil(t, info)
}

func TestElasticQuotaTree_GetElasticQuotaInfoByFullName(t *testing.T) {
	// 创建测试用的 ElasticQuotaTree
	tree := &v1beta1.ElasticQuotaTree{
		Spec: v1beta1.ElasticQuotaTreeSpec{
			Root: v1beta1.ElasticQuotaSpec{
				Name: "root",
				Children: []v1beta1.ElasticQuotaSpec{
					{
						Name:       "test-quota",
						Namespaces: []string{"test-namespace"},
						Min: v1.ResourceList{
							v1.ResourceCPU:    resource.MustParse("1"),
							v1.ResourceMemory: resource.MustParse("1Gi"),
						},
						Max: v1.ResourceList{
							v1.ResourceCPU:    resource.MustParse("2"),
							v1.ResourceMemory: resource.MustParse("2Gi"),
						},
					},
				},
			},
		},
	}

	eqTree := NewElasticQuotaTree(tree)

	// 测试存在的配额全名
	info := eqTree.GetElasticQuotaInfoByFullName("root/test-quota")
	assert.NotNil(t, info)
	assert.Equal(t, "test-quota", info.Name)
	assert.Equal(t, "root/test-quota", info.FullName)

	// 测试不存在的配额全名
	info = eqTree.GetElasticQuotaInfoByFullName("root/non-existent-quota")
	assert.Nil(t, info)
}

func TestElasticQuotaInfo_UsedOverMax(t *testing.T) {
	min := v1.ResourceList{
		v1.ResourceCPU:    resource.MustParse("1"),
		v1.ResourceMemory: resource.MustParse("1Gi"),
	}
	max := v1.ResourceList{
		v1.ResourceCPU:    resource.MustParse("2"),
		v1.ResourceMemory: resource.MustParse("2Gi"),
	}
	used := v1.ResourceList{
		v1.ResourceCPU:    resource.MustParse("1.5"),
		v1.ResourceMemory: resource.MustParse("1.5Gi"),
	}

	eqi := newElasticQuotaInfo("test-quota", nil, min, max, used, false)

	// 测试请求超过最大值的情况
	request := utils.NewResource(v1.ResourceList{
		v1.ResourceCPU: resource.MustParse("1"),
	})
	assert.True(t, eqi.UsedOverMax(request))

	// 测试请求未超过最大值的情况
	request = utils.NewResource(v1.ResourceList{
		v1.ResourceCPU: resource.MustParse("0.1"),
	})
	assert.False(t, eqi.UsedOverMax(request))
}

func TestElasticQuotaInfo_UsedOverMin(t *testing.T) {
	min := v1.ResourceList{
		v1.ResourceCPU:    resource.MustParse("1"),
		v1.ResourceMemory: resource.MustParse("1Gi"),
	}
	max := v1.ResourceList{
		v1.ResourceCPU:    resource.MustParse("2"),
		v1.ResourceMemory: resource.MustParse("2Gi"),
	}
	used := v1.ResourceList{
		v1.ResourceCPU:    resource.MustParse("0.5"),
		v1.ResourceMemory: resource.MustParse("0.5Gi"),
	}

	eqi := newElasticQuotaInfo("test-quota", nil, min, max, used, false)

	// 测试请求超过最小值的情况
	request := utils.NewResource(v1.ResourceList{
		v1.ResourceCPU: resource.MustParse("0.6"),
	})
	assert.True(t, eqi.UsedOverMin(request))

	// 测试请求未超过最小值的情况
	request = utils.NewResource(v1.ResourceList{
		v1.ResourceCPU: resource.MustParse("0.1"),
	})
	assert.False(t, eqi.UsedOverMin(request))
}

func TestElasticQuotaInfo_IsLeaf(t *testing.T) {
	eqi := newElasticQuotaInfo("test-quota", nil, v1.ResourceList{}, v1.ResourceList{}, v1.ResourceList{}, false)

	// 默认情况下是叶子节点
	assert.True(t, eqi.IsLeaf())

	// 添加子节点后不再是叶子节点
	child := newElasticQuotaInfo("child-quota", nil, v1.ResourceList{}, v1.ResourceList{}, v1.ResourceList{}, false)
	eqi.children = append(eqi.children, child)
	assert.False(t, eqi.IsLeaf())
}

func TestElasticQuotaInfo_GetParent(t *testing.T) {
	parent := newElasticQuotaInfo("parent-quota", nil, v1.ResourceList{}, v1.ResourceList{}, v1.ResourceList{}, false)
	child := newElasticQuotaInfo("child-quota", nil, v1.ResourceList{}, v1.ResourceList{}, v1.ResourceList{}, false)
	child.parent = parent

	// 测试获取父节点
	assert.Equal(t, parent, child.GetParent())

	// 测试根节点的父节点为nil
	assert.Nil(t, parent.GetParent())
}

func TestElasticQuotaInfo_UsedLowerThanMinWithOverSell(t *testing.T) {
	min := v1.ResourceList{
		v1.ResourceCPU:        resource.MustParse("1"),
		v1.ResourceMemory:     resource.MustParse("1Gi"),
		"koord-queue/max-jobs": resource.MustParse("10"),
	}
	max := v1.ResourceList{
		v1.ResourceCPU:    resource.MustParse("2"),
		v1.ResourceMemory: resource.MustParse("2Gi"),
	}
	used := v1.ResourceList{
		v1.ResourceCPU:    resource.MustParse("0.5"),
		v1.ResourceMemory: resource.MustParse("0.5Gi"),
	}

	eqi := newElasticQuotaInfo("test-quota", nil, min, max, used, false)
	eqi.Count = 5

	// 测试在超卖率范围内
	ok, res := eqi.UsedLowerThanMinWithOverSell(2.0, sets.New[string]())
	assert.True(t, ok)
	assert.Equal(t, "", res)

	// 测试超过超卖率的情况 (CPU)
	eqi.Used.Resources["cpu"] = 2000 // 2 cores, exceeds 1*2
	ok, res = eqi.UsedLowerThanMinWithOverSell(1.5, sets.New[string]())
	assert.False(t, ok)
	assert.Equal(t, "cpu", res)

	// 测试超过超卖率的情况 (Jobs)
	eqi.Count = 16 // Exceeds 10*1.5
	ok, res = eqi.UsedLowerThanMinWithOverSell(1.5, sets.New[string]())
	assert.False(t, ok)
	assert.Equal(t, "jobs", res)
}

func TestElasticQuotaInfo_UsedOverMinWithOverSell(t *testing.T) {
	min := v1.ResourceList{
		v1.ResourceCPU:        resource.MustParse("1"),
		v1.ResourceMemory:     resource.MustParse("1Gi"),
		"koord-queue/max-jobs": resource.MustParse("10"),
	}
	max := v1.ResourceList{
		v1.ResourceCPU:    resource.MustParse("2"),
		v1.ResourceMemory: resource.MustParse("2Gi"),
	}
	used := v1.ResourceList{
		v1.ResourceCPU:    resource.MustParse("0.5"),
		v1.ResourceMemory: resource.MustParse("0.5Gi"),
	}

	eqi := newElasticQuotaInfo("test-quota", nil, min, max, used, false)
	eqi.Count = 5
	eqi.NonPreemptibleUsed = utils.NewResource(v1.ResourceList{
		v1.ResourceCPU: resource.MustParse("0.3"),
	})

	// 测试请求未超过超卖最小值
	request := utils.NewResource(v1.ResourceList{
		v1.ResourceCPU: resource.MustParse("0.1"),
	})
	preempt := utils.NewResource(nil)
	ok, res := eqi.UsedOverMinWithOverSell(request, preempt, 2.0)
	assert.False(t, ok)
	assert.Equal(t, "", res)

	// 测试请求超过超卖最小值 (CPU)
	request = utils.NewResource(v1.ResourceList{
		v1.ResourceCPU: resource.MustParse("1.5"),
	})
	ok, res = eqi.UsedOverMinWithOverSell(request, preempt, 1.5)
	assert.True(t, ok)
	assert.Equal(t, "cpu", res)

	// 测试请求超过超卖最小值 (Jobs)
	request = utils.NewResource(v1.ResourceList{})
	eqi.Count = 16 // Exceeds 10*1.5
	ok, res = eqi.UsedOverMinWithOverSell(request, preempt, 1.5)
	assert.True(t, ok)
	assert.Equal(t, "jobs", res)
}

// TestMinSubLendLimit 测试 MinSubLendLimit 的正确解析
func TestMinSubLendLimit(t *testing.T) {
	// 创建带有 lendlimit 属性的 ElasticQuotaSpec
	elasticQuota := v1beta1.ElasticQuotaSpec{
		Name: "test-quota",
		Min: v1.ResourceList{
			v1.ResourceCPU:    resource.MustParse("4"),
			v1.ResourceMemory: resource.MustParse("8Gi"),
		},
		Max: v1.ResourceList{
			v1.ResourceCPU:    resource.MustParse("8"),
			v1.ResourceMemory: resource.MustParse("16Gi"),
		},
		Attributes: map[string]string{
			"alibabacloud.com/lendlimit": `{"cpu":"2","memory":"4Gi"}`,
		},
	}

	// 创建 ElasticQuotaInfos 映射
	elasticQuotaInfos := make(ElasticQuotaInfos)
	name2Quota := make(ElasticQuotaInfos)
	namespaceToQuota := make(map[string]string)

	// 构建 ElasticQuotaInfo
	info := buildElasticQuotaInfo(elasticQuota, elasticQuotaInfos, name2Quota, namespaceToQuota, nil, "")

	assert.Equal(t, int64(2000), info.MinSubLendLimit.Resources["cpu"])                       // 4-2=2 cores
	assert.Equal(t, int64(4*1024*1024*1024), info.MinSubLendLimit.Resources["memory"])        // 8Gi-4Gi=4Gi
	assert.Equal(t, int64(2000), info.getUsedWithLendLimit().Resources["cpu"])                // 4-2=2 cores
	assert.Equal(t, int64(4*1024*1024*1024), info.getUsedWithLendLimit().Resources["memory"]) // 8Gi-4Gi=4Gi
}

// TestGetUsedWithLendLimit 测试 getUsedWithLendLimit 方法
func TestGetUsedWithLendLimit(t *testing.T) {
	// 创建基础配额信息
	min := v1.ResourceList{
		v1.ResourceCPU:    resource.MustParse("4"),
		v1.ResourceMemory: resource.MustParse("8Gi"),
	}
	max := v1.ResourceList{
		v1.ResourceCPU:    resource.MustParse("8"),
		v1.ResourceMemory: resource.MustParse("16Gi"),
	}
	used := v1.ResourceList{
		v1.ResourceCPU:    resource.MustParse("3"),
		v1.ResourceMemory: resource.MustParse("6Gi"),
	}

	eqi := newElasticQuotaInfo("test-quota", nil, min, max, used, false)

	// 测试没有 MinSubLendLimit 的情况
	result := eqi.getUsedWithLendLimit()
	assert.Equal(t, eqi.Used.Resources["cpu"], result.Resources["cpu"])
	assert.Equal(t, eqi.Used.Resources["memory"], result.Resources["memory"])

	// 设置 MinSubLendLimit
	eqi.MinSubLendLimit = utils.NewResource(v1.ResourceList{
		v1.ResourceCPU:    resource.MustParse("2"),
		v1.ResourceMemory: resource.MustParse("4Gi"),
	})
	// 同时设置 UsedWithLendLimit 用于测试
	eqi.UsedWithLendLimit = utils.NewResource(v1.ResourceList{
		v1.ResourceCPU:    resource.MustParse("3"),
		v1.ResourceMemory: resource.MustParse("6Gi"),
	})

	// 测试有 MinSubLendLimit 的情况
	result = eqi.getUsedWithLendLimit()
	// 应该返回 max(usedWithLendLimit, min-lendlimit) = max(3, 4-2) = 3 for CPU
	assert.Equal(t, int64(3000), result.Resources["cpu"])
	// 应该返回 max(usedWithLendLimit, min-lendlimit) = max(6Gi, 8Gi-4Gi) = 6Gi for Memory
	assert.Equal(t, int64(6*1024*1024*1024), result.Resources["memory"])

	// 修改 UsedWithLendLimit 值使其小于 min-lendlimit
	eqi.UsedWithLendLimit = utils.NewResource(v1.ResourceList{
		v1.ResourceCPU:    resource.MustParse("1"),
		v1.ResourceMemory: resource.MustParse("2Gi"),
	})

	// 测试 usedWithLendLimit 小于 min-lendlimit 的情况
	result = eqi.getUsedWithLendLimit()
	// 应该返回 max(usedWithLendLimit, min-lendlimit) = max(1, 4-2) = 2 for CPU
	assert.Equal(t, int64(2000), result.Resources["cpu"])
	// 应该返回 max(usedWithLendLimit, min-lendlimit) = max(2Gi, 8Gi-4Gi) = 4Gi for Memory
	assert.Equal(t, int64(4*1024*1024*1024), result.Resources["memory"])
}

// TestUpdateUsedWithLendLimitLeafNode 测试叶节点的 updateUsedWithLendLimit 方法
func TestUpdateUsedWithLendLimitLeafNode(t *testing.T) {
	min := v1.ResourceList{
		v1.ResourceCPU:    resource.MustParse("4"),
		v1.ResourceMemory: resource.MustParse("8Gi"),
	}
	max := v1.ResourceList{
		v1.ResourceCPU:    resource.MustParse("8"),
		v1.ResourceMemory: resource.MustParse("16Gi"),
	}
	used := v1.ResourceList{
		v1.ResourceCPU:    resource.MustParse("3"),
		v1.ResourceMemory: resource.MustParse("6Gi"),
	}

	eqi := newElasticQuotaInfo("test-quota", nil, min, max, used, false)
	assert.True(t, eqi.IsLeaf()) // 确认是叶节点

	// 初始状态，UsedWithLendLimit 应该为 nil
	assert.Nil(t, eqi.UsedWithLendLimit)

	// 调用 updateUsedWithLendLimit
	eqi.updateUsedWithLendLimit(nil)

	// 验证 UsedWithLendLimit 是否被正确设置为 Used 的副本
	assert.NotNil(t, eqi.UsedWithLendLimit)
	assert.Equal(t, eqi.Used.Resources["cpu"], eqi.UsedWithLendLimit.Resources["cpu"])
	assert.Equal(t, eqi.Used.Resources["memory"], eqi.UsedWithLendLimit.Resources["memory"])
}

// TestUpdateUsedWithLendLimitParentNode 测试父节点的 updateUsedWithLendLimit 方法
func TestUpdateUsedWithLendLimitParentNode(t *testing.T) {
	// 创建父节点
	parentMin := v1.ResourceList{
		v1.ResourceCPU:    resource.MustParse("10"),
		v1.ResourceMemory: resource.MustParse("20Gi"),
	}
	parentMax := v1.ResourceList{
		v1.ResourceCPU:    resource.MustParse("20"),
		v1.ResourceMemory: resource.MustParse("40Gi"),
	}
	parentUsed := v1.ResourceList{
		v1.ResourceCPU:    resource.MustParse("5"),
		v1.ResourceMemory: resource.MustParse("10Gi"),
	}

	parent := newElasticQuotaInfo("parent-quota", nil, parentMin, parentMax, parentUsed, false)

	// 创建子节点1
	child1Min := v1.ResourceList{
		v1.ResourceCPU:    resource.MustParse("4"),
		v1.ResourceMemory: resource.MustParse("8Gi"),
	}
	child1Max := v1.ResourceList{
		v1.ResourceCPU:    resource.MustParse("8"),
		v1.ResourceMemory: resource.MustParse("16Gi"),
	}
	child1Used := v1.ResourceList{
		v1.ResourceCPU:    resource.MustParse("3"),
		v1.ResourceMemory: resource.MustParse("6Gi"),
	}

	child1 := newElasticQuotaInfo("child1-quota", nil, child1Min, child1Max, child1Used, false)
	child1.parent = parent

	// 创建子节点2
	child2Min := v1.ResourceList{
		v1.ResourceCPU:    resource.MustParse("4"),
		v1.ResourceMemory: resource.MustParse("8Gi"),
	}
	child2Max := v1.ResourceList{
		v1.ResourceCPU:    resource.MustParse("8"),
		v1.ResourceMemory: resource.MustParse("16Gi"),
	}
	child2Used := v1.ResourceList{
		v1.ResourceCPU:    resource.MustParse("2"),
		v1.ResourceMemory: resource.MustParse("4Gi"),
	}

	child2 := newElasticQuotaInfo("child2-quota", nil, child2Min, child2Max, child2Used, false)
	child2.parent = parent

	// 建立父子关系
	parent.children = append(parent.children, child1, child2)

	// 确认父节点不是叶节点
	assert.False(t, parent.IsLeaf())

	// 初始化子节点的 UsedWithLendLimit
	child1.updateUsedWithLendLimit(nil)
	child2.updateUsedWithLendLimit(nil)

	// 调用父节点的 updateUsedWithLendLimit
	parent.updateUsedWithLendLimit(nil)

	// 验证父节点的 UsedWithLendLimit 是否等于所有子节点 UsedWithLendLimit 的总和
	expectedCPU := child1.getUsedWithLendLimit().Resources["cpu"] + child2.getUsedWithLendLimit().Resources["cpu"]
	expectedMemory := child1.getUsedWithLendLimit().Resources["memory"] + child2.getUsedWithLendLimit().Resources["memory"]

	assert.Equal(t, expectedCPU, parent.UsedWithLendLimit.Resources["cpu"])
	assert.Equal(t, expectedMemory, parent.UsedWithLendLimit.Resources["memory"])
}

// TestLendLimitWithResourceChange 测试资源变化时 lend limit 的更新
func TestLendLimitWithResourceChange(t *testing.T) {
	min := v1.ResourceList{
		v1.ResourceCPU:    resource.MustParse("4"),
		v1.ResourceMemory: resource.MustParse("8Gi"),
	}
	max := v1.ResourceList{
		v1.ResourceCPU:    resource.MustParse("8"),
		v1.ResourceMemory: resource.MustParse("16Gi"),
	}
	used := v1.ResourceList{
		v1.ResourceCPU:    resource.MustParse("3"),
		v1.ResourceMemory: resource.MustParse("6Gi"),
	}

	eqi := newElasticQuotaInfo("test-quota", nil, min, max, used, false)
	eqi.MinSubLendLimit = utils.NewResource(v1.ResourceList{
		v1.ResourceCPU:    resource.MustParse("2"),
		v1.ResourceMemory: resource.MustParse("4Gi"),
	})

	// 初始调用
	eqi.updateUsedWithLendLimit(nil)

	// 模拟资源使用增加
	resourceChange := utils.NewResource(v1.ResourceList{
		v1.ResourceCPU:    resource.MustParse("1"),
		v1.ResourceMemory: resource.MustParse("2Gi"),
	})
	eqi.Used.AddResource(resourceChange)

	// 再次调用 updateUsedWithLendLimit 处理资源变化
	eqi.updateUsedWithLendLimit(resourceChange)

	// 验证 UsedWithLendLimit 是否正确更新
	// 对于叶节点，UsedWithLendLimit 应该等于更新后的 Used
	assert.Equal(t, eqi.Used.Resources["cpu"], eqi.UsedWithLendLimit.Resources["cpu"])
	assert.Equal(t, eqi.Used.Resources["memory"], eqi.UsedWithLendLimit.Resources["memory"])
}
