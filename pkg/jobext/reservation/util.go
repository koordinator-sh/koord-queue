package reservation

import (
	"context"
	"sync"

	"github.com/go-logr/logr"
	koordinatorschedulerv1alpha1 "github.com/koordinator-sh/apis/scheduling/v1alpha1"

	"k8s.io/apimachinery/pkg/api/errors"
)

func (rc *ReservationController) CreateOrUpdateReservation(ctx context.Context, log logr.Logger, resvs, existing []koordinatorschedulerv1alpha1.Reservation) (bool, error) {
	existingMap := map[string]*koordinatorschedulerv1alpha1.Reservation{}
	for _, r := range existing {
		existingMap[r.Name] = &r
	}

	needUpdate := false
	wg := sync.WaitGroup{}
	l := sync.Mutex{}
	errs := []error{}
	updated := []string{}
	for _, r := range resvs {
		existingR, ok := existingMap[r.Name]
		if !ok {
			wg.Add(1)
			needUpdate = true
			go func(r koordinatorschedulerv1alpha1.Reservation) {
				defer wg.Done()
				if err := rc.cli.Create(ctx, &r); err != nil && !errors.IsAlreadyExists(err) {
					log.Error(err, "failed to create reservation", "reservation", r)
					l.Lock()
					errs = append(errs, err)
					l.Unlock()
				} else {
					l.Lock()
					updated = append(updated, r.Name)
					l.Unlock()
				}
			}(r)
		} else if existingR.Annotations["blacklist-rv"] != r.Annotations["blacklist-rv"] {
			wg.Add(1)
			needUpdate = true
			go func(r koordinatorschedulerv1alpha1.Reservation) {
				defer wg.Done()
				r.UID = existingR.UID
				r.ResourceVersion = existingR.ResourceVersion
				if err := rc.cli.Update(ctx, &r); err != nil {
					log.Error(err, "failed to update reservation blacklist", "reservation", r)
					l.Lock()
					errs = append(errs, err)
					l.Unlock()
				} else {
					log.V(2).Info("update reservation because rv changed", "reservation", r.Name,
						"existingrv", existingR.Annotations["blacklist-rv"], "expectrv", r.Annotations["blacklist-rv"])
					l.Lock()
					updated = append(updated, r.Name)
					l.Unlock()
				}
			}(r)
		}
	}
	wg.Wait()
	if needUpdate {
		log.Info("updated reservations", "updated", updated)
	}
	if len(errs) > 0 {
		// TODO
		return needUpdate, errs[0]
	}
	return needUpdate, nil
}

func (rc *ReservationController) CreateReservations(ctx context.Context, log logr.Logger, resvs []koordinatorschedulerv1alpha1.Reservation) error {
	wg := sync.WaitGroup{}
	l := sync.Mutex{}
	errs := []error{}
	for _, r := range resvs {
		wg.Add(1)
		go func(r koordinatorschedulerv1alpha1.Reservation) {
			defer wg.Done()
			if err := rc.cli.Create(ctx, &r); err != nil && !errors.IsAlreadyExists(err) {
				log.Error(err, "failed to create reservation", "reservation", r)
				l.Lock()
				errs = append(errs, err)
				l.Unlock()
			}
		}(r)
	}
	wg.Wait()
	if len(errs) > 0 {
		// TODO
		return errs[0]
	}
	return nil
}
