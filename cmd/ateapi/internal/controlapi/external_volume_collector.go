// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package controlapi

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/workqueue"
)

const (
	externalVolumeResyncInterval = 60 * time.Second
	externalVolumeListPageSize   = 100

	// externalVolumeWorkerCount is the number of goroutines draining the work
	// queue.
	externalVolumeWorkerCount = 3

	// externalVolumePendingGrace is how long a volume may sit pending before
	// it is treated as the wreckage of a create that died. A create holds the
	// volume's lease for its whole duration, so the grace only has to outlast
	// a CSI provision that runs past its lease rather than one that is still
	// healthy.
	externalVolumePendingGrace = 15 * time.Minute
)

// externalVolumeCollectorStore enumerates the exact storage methods needed by
// ExternalVolumeCollector and nothing more.
type externalVolumeCollectorStore interface {
	GetActor(ctx context.Context, actorRef resources.ActorRef) (*ateapipb.Actor, error)
	GetExternalVolume(ctx context.Context, volumeRef resources.ExternalVolumeRef) (*ateapipb.ExternalVolume, error)
	ListExternalVolumes(ctx context.Context, atespace string, opts store.ListOptions) (store.ListResponse[*ateapipb.ExternalVolume], error)
	UpdateExternalVolume(ctx context.Context, volumeRef resources.ExternalVolumeRef, precondition store.Precondition, mutate func(toUpdate *ateapipb.ExternalVolume) error) (*ateapipb.ExternalVolume, error)
	AcquireLease(ctx context.Context, key string) (*store.Lease, error)
}

// externalVolumeDeleter is the slice of the ExternalVolume workflow the
// collector drives. *ExternalVolumeWorkflow satisfies it.
type externalVolumeDeleter interface {
	DeleteExternalVolume(ctx context.Context, volumeRef resources.ExternalVolumeRef) (*ateapipb.ExternalVolume, error)
}

// ExternalVolumeCollector collects what the ExternalVolume workflows cannot
// collect themselves, because the process doing the work died partway.
//
// Two kinds of wreckage. A create that died between reserving the name and
// stamping the handle leaves a pending row that holds the name and may own a
// disk nobody can name; the collector deletes it, which reclaims the disk by
// the ID the create would have used. And an actor that vanished while holding
// a reference — a crash whose release failed, or a delete that never finished
// — leaves a reference that would keep the volume undeletable forever; the
// collector drops references whose actor is gone.
type ExternalVolumeCollector struct {
	persistence externalVolumeCollectorStore
	workflow    externalVolumeDeleter
	queue       workqueue.TypedRateLimitingInterface[resources.ExternalVolumeRef]

	// now reads the wall clock the pending grace is measured against. Only
	// tests replace it; a stored volume's age is not otherwise observable
	// without waiting out the grace.
	now func() time.Time
}

// NewExternalVolumeCollector creates a new ExternalVolumeCollector.
func NewExternalVolumeCollector(persistence externalVolumeCollectorStore, workflow externalVolumeDeleter) *ExternalVolumeCollector {
	return &ExternalVolumeCollector{
		persistence: persistence,
		workflow:    workflow,
		queue:       workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[resources.ExternalVolumeRef]()),
		now:         time.Now,
	}
}

// Start launches the queue workers and the resync producer; there is no event
// source for stored volumes, so the periodic list is the event source.
func (c *ExternalVolumeCollector) Start(ctx context.Context) {
	go func() {
		defer c.queue.ShutDown()
		for range externalVolumeWorkerCount {
			go wait.UntilWithContext(ctx, c.runWorker, time.Second)
		}
		wait.UntilWithContext(ctx, c.resync, externalVolumeResyncInterval)
	}()
}

// resync enqueues every volume that looks like it may need collecting, so the
// common case — a ready volume whose references are all live — costs one list
// and no further reads.
func (c *ExternalVolumeCollector) resync(ctx context.Context) {
	pageToken := ""
	for {
		// TODO: need sharding
		page, err := c.persistence.ListExternalVolumes(ctx, "", store.ListOptions{PageSize: externalVolumeListPageSize, PageToken: pageToken})
		if err != nil {
			slog.ErrorContext(ctx, "Failed to list external volumes", slog.Any("err", err))
			return
		}
		for _, volume := range page.Items {
			if externalVolumeNeedsCollection(volume, c.now()) {
				c.queue.Add(resources.ExternalVolumeRefFromExternalVolume(volume))
			}
		}
		if page.NextPageToken == "" {
			return
		}
		pageToken = page.NextPageToken
	}
}

// externalVolumeNeedsCollection reports whether volume is worth a closer look:
// it has been pending long enough to be wreckage, or it holds references that
// may name actors that no longer exist.
func externalVolumeNeedsCollection(volume *ateapipb.ExternalVolume, now time.Time) bool {
	if volume.GetStatus().GetState() != ateapipb.ExternalVolumeState_EXTERNAL_VOLUME_STATE_READY {
		return externalVolumeStrandedSince(volume, now)
	}
	return len(volume.GetStatus().GetRefs()) > 0
}

// externalVolumeStrandedSince reports whether an unfinished volume is old
// enough that no create could still be working on it.
func externalVolumeStrandedSince(volume *ateapipb.ExternalVolume, now time.Time) bool {
	created := volume.GetMetadata().GetCreateTime()
	if created == nil {
		// No creation time to age against; leave it alone rather than collect
		// a volume that may have been reserved moments ago.
		return false
	}
	return now.Sub(created.AsTime()) >= externalVolumePendingGrace
}

func (c *ExternalVolumeCollector) runWorker(ctx context.Context) {
	for c.processNextWorkItem(ctx) {
	}
}

func (c *ExternalVolumeCollector) processNextWorkItem(ctx context.Context) bool {
	ref, quit := c.queue.Get()
	if quit {
		return false
	}
	defer c.queue.Done(ref)

	if err := c.collectOne(ctx, ref); err != nil {
		slog.ErrorContext(ctx, "Failed to collect external volume, requeueing",
			slog.Any("externalVolume", ref),
			slog.Any("err", err))
		c.queue.AddRateLimited(ref)
		return true
	}
	c.queue.Forget(ref)
	return true
}

// collectOne re-reads the volume and takes whichever action its observed state
// asks for. The decision is made again here rather than carried from the list,
// because the volume may have been finished, claimed or deleted since.
func (c *ExternalVolumeCollector) collectOne(ctx context.Context, ref resources.ExternalVolumeRef) error {
	volume, err := c.persistence.GetExternalVolume(ctx, ref)
	if err != nil {
		// Deleted after it was enqueued; nothing left to collect.
		if errors.Is(err, store.ErrNotFound) {
			return nil
		}
		return fmt.Errorf("while getting external volume %s: %w", ref, err)
	}

	if volume.GetStatus().GetState() != ateapipb.ExternalVolumeState_EXTERNAL_VOLUME_STATE_READY {
		if !externalVolumeStrandedSince(volume, c.now()) {
			return nil
		}
		// DeleteExternalVolume takes the volume's lease, so a create that is
		// somehow still running is not interrupted, only deferred: the error
		// requeues and the next pass finds either a finished volume or the
		// same wreckage.
		slog.InfoContext(ctx, "Collecting external volume stranded by a failed create", slog.Any("externalVolume", ref))
		if _, err := c.workflow.DeleteExternalVolume(ctx, ref); err != nil {
			return fmt.Errorf("while collecting stranded external volume %s: %w", ref, err)
		}
		return nil
	}

	return c.dropDeadRefs(ctx, volume)
}

// dropDeadRefs releases the references held by actors that no longer exist, so
// a volume is not kept undeletable by an actor that crashed or was deleted
// without giving its reference back.
//
// A reference is dead when the actor it names is gone, or when an actor of that
// name exists but is a different one: names are reused, uids are not.
func (c *ExternalVolumeCollector) dropDeadRefs(ctx context.Context, volume *ateapipb.ExternalVolume) error {
	volumeRef := resources.ExternalVolumeRefFromExternalVolume(volume)
	atespace := volume.GetMetadata().GetAtespace()

	var dead []string
	for _, ref := range volume.GetStatus().GetRefs() {
		alive, err := c.actorAlive(ctx, atespace, ref)
		if err != nil {
			return err
		}
		if !alive {
			dead = append(dead, ref.GetActorUid())
		}
	}
	if len(dead) == 0 {
		return nil
	}

	slog.InfoContext(ctx, "Releasing external volume references held by actors that no longer exist",
		slog.Any("externalVolume", volumeRef), slog.Any("actorUids", dead))
	_, err := c.persistence.UpdateExternalVolume(ctx, volumeRef, store.PreconditionFrom(volume), func(toUpdate *ateapipb.ExternalVolume) error {
		toUpdate.Status.Refs = slices.DeleteFunc(toUpdate.GetStatus().GetRefs(), func(ref *ateapipb.ActorRef) bool {
			return slices.Contains(dead, ref.GetActorUid())
		})
		return nil
	})
	if err != nil {
		// A concurrent claim or release re-reads cleanly on the next pass.
		if errors.Is(err, store.ErrNotFound) || errors.Is(err, store.ErrUIDConflict) {
			return nil
		}
		return fmt.Errorf("while releasing dead references on external volume %s: %w", volumeRef, err)
	}
	return nil
}

// actorAlive reports whether the actor a reference names still exists under
// that uid.
func (c *ExternalVolumeCollector) actorAlive(ctx context.Context, atespace string, ref *ateapipb.ActorRef) (bool, error) {
	actorRef := resources.ActorRef{Atespace: atespace, Name: ref.GetActorName()}
	actor, err := c.persistence.GetActor(ctx, actorRef)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return false, nil
		}
		return false, fmt.Errorf("while getting actor %s holding a reference: %w", actorRef, err)
	}
	return actor.GetMetadata().GetUid() == ref.GetActorUid(), nil
}
