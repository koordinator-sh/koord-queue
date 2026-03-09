package elasticquotav1alpha1

import (
	"testing"

	api "github.com/koordinator-sh/koord-queue/pkg/apis/scheduling/v1alpha1"
	"github.com/stretchr/testify/assert"
)

func TestGetQuotaName(t *testing.T) {
	{
		unit := &api.QueueUnit{}
		unit.Labels = map[string]string{
			QuotaNameLabelKey:    "test1",
			AsiQuotaNameLabelKey: "test2",
		}
		assert.Equal(t, "test1", getQuotaName(unit))
	}
	{
		unit := &api.QueueUnit{}
		unit.Labels = map[string]string{
			QuotaNameLabelKey:    "",
			AsiQuotaNameLabelKey: "test2",
		}
		assert.Equal(t, "test2", getQuotaName(unit))
	}
}
