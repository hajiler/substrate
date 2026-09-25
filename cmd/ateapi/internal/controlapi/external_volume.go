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

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/defaults"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"k8s.io/apimachinery/pkg/api/operation"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/api/validate"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

func (s *RPCService) CreateExternalVolume(ctx context.Context, req *ateapipb.CreateExternalVolumeRequest) (*ateapipb.ExternalVolume, error) {
	inVolume := req.ExternalVolume
	if inVolume != nil {
		scrubResourceMetadataForCreate(inVolume.Metadata)
		inVolume.Status = nil
		defaults.Apply(inVolume)
	}
	if errs := validateCreateExternalVolumeRequest(ctx, req); len(errs) > 0 {
		return nil, toGRPCStatusError(errs)
	}
	return s.externalVolumeWorkflow.CreateExternalVolume(ctx, inVolume)
}

func (s *ServiceImpl) CreateExternalVolume(ctx context.Context, volume *ateapipb.ExternalVolume) (*ateapipb.ExternalVolume, error) {
	return s.store.CreateExternalVolume(ctx, volume)
}

func validateCreateExternalVolumeRequest(ctx context.Context, req *ateapipb.CreateExternalVolumeRequest) field.ErrorList {
	op := operation.Operation{Type: operation.Create}
	return Validate_CreateExternalVolumeRequest(ctx, op, nil, req, nil)
}

func ValidateCustom_CreateExternalVolumeRequest(_ context.Context, _ operation.Operation, p *field.Path, req, _ *ateapipb.CreateExternalVolumeRequest) field.ErrorList {
	volume := req.GetExternalVolume()
	if volume == nil {
		return nil
	}
	volumePath := p.Child("external_volume")
	storageClassName, volumeID := volume.GetStorageClassName(), volume.GetVolumeId()

	var errs field.ErrorList
	// TODO: accept READ_WRITE_MANY once concurrent holders are supported.
	if mode := volume.GetAccessMode(); mode != ateapipb.AccessMode_ACCESS_MODE_UNSPECIFIED && mode != ateapipb.AccessMode_ACCESS_MODE_READ_WRITE_ONCE {
		errs = append(errs, field.NotSupported(volumePath.Child("access_mode"), mode.String(), []string{ateapipb.AccessMode_ACCESS_MODE_READ_WRITE_ONCE.String()}))
	}
	switch {
	case storageClassName == "" && volumeID == "":
		errs = append(errs, field.Required(volumePath.Child("storage_class_name"), "one of storage_class_name or volume_id is required"))
	case volumeID != "":
		if volume.GetCapacity() != "" {
			errs = append(errs, field.Forbidden(volumePath.Child("capacity"), "must not be set with volume_id"))
		}
		if volume.GetVolumeType() == "" && storageClassName == "" {
			errs = append(errs, field.Required(volumePath.Child("volume_type"), "required with volume_id unless storage_class_name is set"))
		}
	default:
		if volume.GetVolumeType() != "" {
			errs = append(errs, field.Forbidden(volumePath.Child("volume_type"), "must not be set with storage_class_name; it is taken from the class's provisioner"))
		}
		if len(volume.GetVolumeContext()) > 0 {
			errs = append(errs, field.Forbidden(volumePath.Child("volume_context"), "must not be set with storage_class_name; it is returned by the storage system"))
		}
		if volume.GetCapacity() == "" {
			errs = append(errs, field.Required(volumePath.Child("capacity"), "required with storage_class_name"))
		}
	}
	return errs
}

func ValidateCustom_ExternalVolume_Capacity(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *string) field.ErrorList {
	if value == nil || *value == "" {
		return nil
	}
	if _, err := resource.ParseQuantity(*value); err != nil {
		return field.ErrorList{field.Invalid(fldPath, *value, fmt.Sprintf("must be a Kubernetes resource quantity: %v", err))}
	}
	return nil
}

func (s *RPCService) GetExternalVolume(ctx context.Context, req *ateapipb.GetExternalVolumeRequest) (*ateapipb.ExternalVolume, error) {
	if errs := validateGetExternalVolumeRequest(ctx, req); len(errs) > 0 {
		return nil, toGRPCStatusError(errs)
	}
	volumeRef := resources.ExternalVolumeRefFromObjectRef(req.GetExternalVolume())
	volume, err := s.impl.GetExternalVolume(ctx, volumeRef)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "ExternalVolume %s not found", volumeRef)
		}
		return nil, fmt.Errorf("while getting external volume: %w", err)
	}
	return volume, nil
}

func (s *ServiceImpl) GetExternalVolume(ctx context.Context, volumeRef resources.ExternalVolumeRef) (*ateapipb.ExternalVolume, error) {
	return s.store.GetExternalVolume(ctx, volumeRef)
}

func validateGetExternalVolumeRequest(ctx context.Context, req *ateapipb.GetExternalVolumeRequest) field.ErrorList {
	op := operation.Operation{Type: operation.Create}
	return Validate_GetExternalVolumeRequest(ctx, op, nil, req, nil)
}

func (s *RPCService) ListExternalVolumes(ctx context.Context, req *ateapipb.ListExternalVolumesRequest) (*ateapipb.ListExternalVolumesResponse, error) {
	if errs := validateListExternalVolumesRequest(ctx, req); len(errs) > 0 {
		return nil, toGRPCStatusError(errs)
	}
	page, err := s.impl.ListExternalVolumes(ctx, req.GetAtespace(), store.ListOptions{PageSize: effectivePageSize(req.GetPageSize()), PageToken: req.GetPageToken()})
	if err != nil {
		return nil, mapListError(fmt.Errorf("while listing external volumes: %w", err))
	}
	return &ateapipb.ListExternalVolumesResponse{ExternalVolumes: page.Items, NextPageToken: page.NextPageToken}, nil
}

func (s *ServiceImpl) ListExternalVolumes(ctx context.Context, atespace string, opts store.ListOptions) (store.ListResponse[*ateapipb.ExternalVolume], error) {
	return s.store.ListExternalVolumes(ctx, atespace, opts)
}

func validateListExternalVolumesRequest(ctx context.Context, req *ateapipb.ListExternalVolumesRequest) field.ErrorList {
	op := operation.Operation{Type: operation.Create}
	return Validate_ListExternalVolumesRequest(ctx, op, nil, req, nil)
}

var errExternalVolumePending = errors.New("external volume is still being created")

func (s *RPCService) UpdateExternalVolume(ctx context.Context, req *ateapipb.UpdateExternalVolumeRequest) (*ateapipb.ExternalVolume, error) {
	inVolume := req.ExternalVolume
	if inVolume != nil {
		scrubResourceMetadataForUpdate(inVolume.Metadata)
		inVolume.Status = nil
	}
	if errs := validateUpdateExternalVolumeRequest(ctx, req); len(errs) > 0 {
		return nil, toGRPCStatusError(errs)
	}
	in := req.GetExternalVolume()
	volumeRef := resources.ExternalVolumeRefFromExternalVolume(in)

	stored, err := s.impl.UpdateExternalVolume(ctx, volumeRef, store.PreconditionFrom(in), func(toUpdate *ateapipb.ExternalVolume) error {
		if toUpdate.GetStatus().GetState() != ateapipb.ExternalVolumeState_EXTERNAL_VOLUME_STATE_READY {
			return errExternalVolumePending
		}
		metadata, volumeStatus := toUpdate.GetMetadata(), toUpdate.GetStatus()
		proto.Reset(toUpdate)
		proto.Merge(toUpdate, in)
		toUpdate.Metadata, toUpdate.Status = metadata, volumeStatus
		defaults.Apply(toUpdate)
		return nil
	})
	if err != nil {
		if errors.Is(err, errExternalVolumePending) {
			return nil, status.Errorf(codes.FailedPrecondition, "ExternalVolume %s is still being created", volumeRef)
		}
		if errors.Is(err, store.ErrImmutableField) || errors.Is(err, store.ErrPreconditionRequired) {
			return nil, status.Errorf(codes.InvalidArgument, "while updating external volume %s: %v", volumeRef, err)
		}
		if errors.Is(err, store.ErrVersionConflict) {
			return nil, status.Error(codes.Aborted, "concurrent update conflict, please retry")
		}
		if errors.Is(err, store.ErrUIDConflict) {
			return nil, status.Errorf(codes.Aborted, "ExternalVolume %s not found with uid %s", volumeRef, in.GetMetadata().GetUid())
		}
		if errors.Is(err, store.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "ExternalVolume %s not found", volumeRef)
		}
		return nil, fmt.Errorf("while updating external volume: %w", err)
	}
	return stored, nil
}

func (s *ServiceImpl) UpdateExternalVolume(ctx context.Context, volumeRef resources.ExternalVolumeRef, precondition store.Precondition, mutate func(toUpdate *ateapipb.ExternalVolume) error) (*ateapipb.ExternalVolume, error) {
	return s.store.UpdateExternalVolume(ctx, volumeRef, precondition, func(toUpdate *ateapipb.ExternalVolume) error {
		oldVal := proto.CloneOf(toUpdate)
		if err := mutate(toUpdate); err != nil {
			return err
		}
		if errs := validateExternalVolumeUpdate(ctx, field.NewPath("external_volume"), toUpdate, oldVal); len(errs) > 0 {
			return toGRPCStatusError(errs)
		}
		return nil
	})
}

func validateUpdateExternalVolumeRequest(ctx context.Context, req *ateapipb.UpdateExternalVolumeRequest) field.ErrorList {
	op := operation.Operation{Type: operation.Create}
	return Validate_UpdateExternalVolumeRequest(ctx, op, nil, req, nil)
}

func validateExternalVolumeUpdate(ctx context.Context, fldPath *field.Path, newVal, oldVal *ateapipb.ExternalVolume) field.ErrorList {
	op := operation.Operation{Type: operation.Update}
	return Validate_ExternalVolume(ctx, op, fldPath, newVal, oldVal)
}

// This exists only because nested subfield tags are not supported yet.
func ValidateCustom_UpdateExternalVolumeRequest_ExternalVolume(ctx context.Context, op operation.Operation, fldPath *field.Path, volume, _ *ateapipb.ExternalVolume) field.ErrorList {
	if volume == nil || volume.Metadata == nil {
		return nil
	}
	errs := Validate_ResourceMetadata(ctx, op, fldPath.Child("metadata"), volume.Metadata, nil)
	errs = append(errs, validate.RequiredValue(ctx, op, fldPath.Child("metadata", "atespace"), &volume.Metadata.Atespace, nil)...)
	return errs
}

func (s *RPCService) DeleteExternalVolume(ctx context.Context, req *ateapipb.DeleteExternalVolumeRequest) (*ateapipb.ExternalVolume, error) {
	if errs := validateDeleteExternalVolumeRequest(ctx, req); len(errs) > 0 {
		return nil, toGRPCStatusError(errs)
	}
	return s.externalVolumeWorkflow.DeleteExternalVolume(ctx, resources.ExternalVolumeRefFromObjectRef(req.GetExternalVolume()), toDeletePreconditions(req.GetOptions()))
}

func (s *ServiceImpl) DeleteExternalVolume(ctx context.Context, volumeRef resources.ExternalVolumeRef, precondition store.DeletePreconditions) (*ateapipb.ExternalVolume, error) {
	return s.store.DeleteExternalVolume(ctx, volumeRef, precondition)
}

func validateDeleteExternalVolumeRequest(ctx context.Context, req *ateapipb.DeleteExternalVolumeRequest) field.ErrorList {
	op := operation.Operation{Type: operation.Create}
	return Validate_DeleteExternalVolumeRequest(ctx, op, nil, req, nil)
}
