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

package cmd

import (
	"context"
	"fmt"
	"strings"

	"github.com/agent-substrate/substrate/cmd/kubectl-ate/internal/printer"
	"github.com/agent-substrate/substrate/internal/ateclient"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

var (
	externalVolumeAtespaceFlag            string
	externalVolumeAllAtespacesFlag        bool
	createExternalVolumeAtespaceFlag      string
	createExternalVolumeClassFlag         string
	createExternalVolumeCapacityFlag      string
	createExternalVolumeVolumeIDFlag      string
	createExternalVolumeTypeFlag          string
	createExternalVolumeContextFlag       map[string]string
	createExternalVolumeDeleteTriggerFlag string
	createExternalVolumeAccessModeFlag    string
	updateExternalVolumeAtespaceFlag      string
	updateExternalVolumeDeleteTriggerFlag string
	deleteExternalVolumeAtespaceFlag      string
)

var getExternalVolumesCmd = &cobra.Command{
	Use:     "externalvolumes [volume-name ...]",
	Aliases: []string{"externalvolume", "ev"},
	Short:   "List or get external volumes",
	RunE: func(cmd *cobra.Command, args []string) error {
		if externalVolumeAllAtespacesFlag && externalVolumeAtespaceFlag != "" {
			return fmt.Errorf("--atespace and -A/--all-atespaces are mutually exclusive")
		}
		if len(args) > 0 && externalVolumeAtespaceFlag == "" {
			return fmt.Errorf("--atespace is required when getting external volumes")
		}
		if len(args) == 0 && !externalVolumeAllAtespacesFlag && externalVolumeAtespaceFlag == "" {
			return fmt.Errorf("specify --atespace <name>, or -A/--all-atespaces")
		}

		ctx := cmd.Context()
		client, err := ateclient.NewClient(ctx, kubeconfig, k8sContext, endpoint, tokenFile, traceEnabled)
		if err != nil {
			return fmt.Errorf("failed to connect to ate-api-server: %w", err)
		}
		defer client.Close()

		if len(args) > 0 {
			volumes := make([]*ateapipb.ExternalVolume, 0, len(args))
			for _, name := range args {
				volume, err := client.GetExternalVolume(ctx, &ateapipb.GetExternalVolumeRequest{
					ExternalVolume: &ateapipb.ObjectRef{Atespace: externalVolumeAtespaceFlag, Name: name},
				})
				if err != nil {
					return fmt.Errorf("failed to get external volume %q: %w", name, err)
				}
				volumes = append(volumes, volume)
			}
			if len(volumes) == 1 {
				return printer.PrintExternalVolumeTo(cmd.OutOrStdout(), volumes[0], outputFmt)
			}
			return printer.PrintExternalVolumesTo(cmd.OutOrStdout(), volumes, outputFmt)
		}

		var volumes []*ateapipb.ExternalVolume
		pageToken := ""
		for {
			resp, err := client.ListExternalVolumes(ctx, &ateapipb.ListExternalVolumesRequest{
				Atespace: externalVolumeAtespaceFlag, PageSize: 1000, PageToken: pageToken,
			})
			if err != nil {
				return fmt.Errorf("failed to list external volumes: %w", err)
			}
			volumes = append(volumes, resp.GetExternalVolumes()...)
			pageToken = resp.GetNextPageToken()
			if pageToken == "" {
				break
			}
		}
		return printer.PrintExternalVolumesTo(cmd.OutOrStdout(), volumes, outputFmt)
	},
}

var createExternalVolumeCmd = &cobra.Command{
	Use:     "externalvolume <volume-name>",
	Aliases: []string{"externalvolumes", "ev"},
	Short:   "Create an external volume actors can share",
	Long: "Create an external volume actors can share.\n\n" +
		"Name a storage class with --storage-class to provision a new volume, or\n" +
		"supply --volume-id to register a volume that already exists, but not both.",
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		trigger, err := parseDeleteTrigger(createExternalVolumeDeleteTriggerFlag)
		if err != nil {
			return err
		}
		mode, err := parseAccessMode(createExternalVolumeAccessModeFlag)
		if err != nil {
			return err
		}
		ctx := cmd.Context()
		client, err := ateclient.NewClient(ctx, kubeconfig, k8sContext, endpoint, tokenFile, traceEnabled)
		if err != nil {
			return fmt.Errorf("failed to connect to ate-api-server: %w", err)
		}
		defer client.Close()

		volume, err := client.CreateExternalVolume(ctx, &ateapipb.CreateExternalVolumeRequest{
			ExternalVolume: &ateapipb.ExternalVolume{
				Metadata:         &ateapipb.ResourceMetadata{Atespace: createExternalVolumeAtespaceFlag, Name: args[0]},
				DeleteTrigger:    trigger,
				AccessMode:       mode,
				StorageClassName: createExternalVolumeClassFlag,
				Capacity:         createExternalVolumeCapacityFlag,
				VolumeId:         createExternalVolumeVolumeIDFlag,
				VolumeType:       createExternalVolumeTypeFlag,
				VolumeContext:    createExternalVolumeContextFlag,
			},
		})
		if err != nil {
			return fmt.Errorf("failed to create external volume: %w", err)
		}
		return printer.PrintExternalVolumeTo(cmd.OutOrStdout(), volume, outputFmt)
	},
}

var updateExternalVolumeCmd = &cobra.Command{
	Use:     "externalvolume <volume-name>",
	Aliases: []string{"externalvolumes", "ev"},
	Short:   "Change an external volume's delete trigger",
	Args:    cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		trigger, err := parseDeleteTrigger(updateExternalVolumeDeleteTriggerFlag)
		if err != nil {
			return err
		}
		ctx := cmd.Context()
		client, err := ateclient.NewClient(ctx, kubeconfig, k8sContext, endpoint, tokenFile, traceEnabled)
		if err != nil {
			return fmt.Errorf("failed to connect to ate-api-server: %w", err)
		}
		defer client.Close()

		ref := &ateapipb.ObjectRef{Atespace: updateExternalVolumeAtespaceFlag, Name: args[0]}
		resp, err := updateExternalVolumeDeleteTrigger(ctx, client, ref, trigger)
		if err != nil {
			return err
		}
		return printer.PrintExternalVolumeTo(cmd.OutOrStdout(), resp, outputFmt)
	},
}

var deleteExternalVolumeCmd = &cobra.Command{
	Use:     "externalvolume <volume-name>",
	Aliases: []string{"externalvolumes", "ev"},
	Short:   "Delete an external volume and its storage",
	Args:    cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		client, err := ateclient.NewClient(ctx, kubeconfig, k8sContext, endpoint, tokenFile, traceEnabled)
		if err != nil {
			return fmt.Errorf("failed to connect to ate-api-server: %w", err)
		}
		defer client.Close()

		_, err = client.DeleteExternalVolume(ctx, &ateapipb.DeleteExternalVolumeRequest{
			ExternalVolume: &ateapipb.ObjectRef{Atespace: deleteExternalVolumeAtespaceFlag, Name: args[0]},
		})
		if err != nil {
			return fmt.Errorf("failed to delete external volume: %w", err)
		}
		fmt.Fprintf(cmd.OutOrStdout(), "external volume %q deleted\n", args[0])
		return nil
	},
}

type externalVolumeClient interface {
	GetExternalVolume(ctx context.Context, in *ateapipb.GetExternalVolumeRequest, opts ...grpc.CallOption) (*ateapipb.ExternalVolume, error)
	UpdateExternalVolume(ctx context.Context, in *ateapipb.UpdateExternalVolumeRequest, opts ...grpc.CallOption) (*ateapipb.ExternalVolume, error)
}

// updateExternalVolumeDeleteTrigger reads the volume first so the update carries
// the uid and version the server requires as preconditions.
func updateExternalVolumeDeleteTrigger(ctx context.Context, client externalVolumeClient, ref *ateapipb.ObjectRef, trigger ateapipb.DeleteTrigger) (*ateapipb.ExternalVolume, error) {
	volume, err := client.GetExternalVolume(ctx, &ateapipb.GetExternalVolumeRequest{ExternalVolume: ref})
	if err != nil {
		return nil, fmt.Errorf("failed to get external volume %q: %w", ref.GetName(), err)
	}
	volume.DeleteTrigger = trigger

	resp, err := client.UpdateExternalVolume(ctx, &ateapipb.UpdateExternalVolumeRequest{ExternalVolume: volume})
	if err != nil {
		return nil, fmt.Errorf("failed to update external volume: %w", err)
	}
	return resp, nil
}

func parseDeleteTrigger(value string) (ateapipb.DeleteTrigger, error) {
	switch strings.ToLower(value) {
	case "":
		return ateapipb.DeleteTrigger_DELETE_TRIGGER_UNSPECIFIED, nil
	case "manual":
		return ateapipb.DeleteTrigger_DELETE_TRIGGER_MANUAL, nil
	case "last-actor", "lastactor":
		return ateapipb.DeleteTrigger_DELETE_TRIGGER_LAST_ACTOR, nil
	default:
		return ateapipb.DeleteTrigger_DELETE_TRIGGER_UNSPECIFIED, fmt.Errorf("invalid delete trigger %q; must be manual or last-actor", value)
	}
}

func parseAccessMode(value string) (ateapipb.AccessMode, error) {
	switch strings.ToLower(value) {
	case "":
		return ateapipb.AccessMode_ACCESS_MODE_UNSPECIFIED, nil
	case "rwo", "readwriteonce":
		return ateapipb.AccessMode_ACCESS_MODE_READ_WRITE_ONCE, nil
	case "rwx", "readwritemany":
		return ateapipb.AccessMode_ACCESS_MODE_READ_WRITE_MANY, nil
	default:
		return ateapipb.AccessMode_ACCESS_MODE_UNSPECIFIED, fmt.Errorf("invalid access mode %q; must be rwo or rwx", value)
	}
}

func init() {
	getExternalVolumesCmd.Flags().StringVarP(&externalVolumeAtespaceFlag, "atespace", "a", "", "Atespace to list/get external volumes in")
	getExternalVolumesCmd.Flags().BoolVarP(&externalVolumeAllAtespacesFlag, "all-atespaces", "A", false, "List external volumes across all atespaces")
	getCmd.AddCommand(getExternalVolumesCmd)

	createExternalVolumeCmd.Flags().StringVarP(&createExternalVolumeAtespaceFlag, "atespace", "a", "", "Atespace to create the external volume in (required)")
	_ = createExternalVolumeCmd.MarkFlagRequired("atespace")
	createExternalVolumeCmd.Flags().StringVar(&createExternalVolumeClassFlag, "storage-class", "", "StorageClass to provision the volume from")
	createExternalVolumeCmd.Flags().StringVar(&createExternalVolumeCapacityFlag, "capacity", "", "Size to provision, as a Kubernetes quantity (e.g. 10Gi); required with --storage-class")
	createExternalVolumeCmd.Flags().StringVar(&createExternalVolumeVolumeIDFlag, "volume-id", "", "Volume ID of a volume that already exists, to register instead of provisioning")
	createExternalVolumeCmd.Flags().StringVar(&createExternalVolumeTypeFlag, "volume-type", "", "CSI driver name; required with --volume-id unless --storage-class is set")
	createExternalVolumeCmd.Flags().StringToStringVar(&createExternalVolumeContextFlag, "volume-context", nil, "Driver metadata for mounting, as key=value pairs; only valid with --volume-id")
	createExternalVolumeCmd.Flags().StringVar(&createExternalVolumeDeleteTriggerFlag, "delete-trigger", "", "What removes the volume: manual or last-actor (default: last-actor when provisioned, manual when registered)")
	createExternalVolumeCmd.Flags().StringVar(&createExternalVolumeAccessModeFlag, "access-mode", "", "How many resumed actors may hold the volume: rwo (default) or rwx")
	createCmd.AddCommand(createExternalVolumeCmd)

	updateExternalVolumeCmd.Flags().StringVarP(&updateExternalVolumeAtespaceFlag, "atespace", "a", "", "Atespace owning the external volume (required)")
	updateExternalVolumeCmd.Flags().StringVar(&updateExternalVolumeDeleteTriggerFlag, "delete-trigger", "", "What removes the volume: manual or last-actor (required)")
	_ = updateExternalVolumeCmd.MarkFlagRequired("atespace")
	_ = updateExternalVolumeCmd.MarkFlagRequired("delete-trigger")
	updateCmd.AddCommand(updateExternalVolumeCmd)

	deleteExternalVolumeCmd.Flags().StringVarP(&deleteExternalVolumeAtespaceFlag, "atespace", "a", "", "Atespace owning the external volume (required)")
	_ = deleteExternalVolumeCmd.MarkFlagRequired("atespace")
	deleteCmd.AddCommand(deleteExternalVolumeCmd)
}
