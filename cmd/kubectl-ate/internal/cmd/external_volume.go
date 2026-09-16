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
	externalVolumeAtespaceFlag       string
	externalVolumeAllAtespacesFlag   bool
	createExternalVolumeAtespaceFlag string
	createExternalVolumeClassFlag    string
	createExternalVolumeCapacityFlag string
	createExternalVolumeVolumeIDFlag string
	createExternalVolumeTypeFlag     string
	createExternalVolumeContextFlag  map[string]string
	createExternalVolumeReclaimFlag  string
	updateExternalVolumeAtespaceFlag string
	updateExternalVolumeReclaimFlag  string
	deleteExternalVolumeAtespaceFlag string
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
		"supply --volume-id to register a disk that already exists, but not both.\n" +
		"The volume outlives the actors that mount it; whether the storage itself\n" +
		"survives deletion is the reclaim policy's decision.",
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		reclaim, err := parseReclaimPolicy(createExternalVolumeReclaimFlag)
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
				ReclaimPolicy:    reclaim,
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
	Short:   "Change an external volume's reclaim policy",
	Args:    cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		reclaim, err := parseReclaimPolicy(updateExternalVolumeReclaimFlag)
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
		resp, err := updateExternalVolumeReclaimPolicy(ctx, client, ref, reclaim)
		if err != nil {
			return err
		}
		return printer.PrintExternalVolumeTo(cmd.OutOrStdout(), resp, outputFmt)
	},
}

var deleteExternalVolumeCmd = &cobra.Command{
	Use:     "externalvolume <volume-name>",
	Aliases: []string{"externalvolumes", "ev"},
	Short:   "Delete an external volume",
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
		fmt.Printf("external volume %q deleted\n", args[0])
		return nil
	},
}

type externalVolumeClient interface {
	GetExternalVolume(ctx context.Context, in *ateapipb.GetExternalVolumeRequest, opts ...grpc.CallOption) (*ateapipb.ExternalVolume, error)
	UpdateExternalVolume(ctx context.Context, in *ateapipb.UpdateExternalVolumeRequest, opts ...grpc.CallOption) (*ateapipb.ExternalVolume, error)
}

// updateExternalVolumeReclaimPolicy reads the volume back before writing it, so
// that the update carries the uid and version the server requires as
// preconditions.
func updateExternalVolumeReclaimPolicy(ctx context.Context, client externalVolumeClient, ref *ateapipb.ObjectRef, reclaim ateapipb.ReclaimPolicy) (*ateapipb.ExternalVolume, error) {
	volume, err := client.GetExternalVolume(ctx, &ateapipb.GetExternalVolumeRequest{ExternalVolume: ref})
	if err != nil {
		return nil, fmt.Errorf("failed to get external volume %q: %w", ref.GetName(), err)
	}
	volume.ReclaimPolicy = reclaim

	resp, err := client.UpdateExternalVolume(ctx, &ateapipb.UpdateExternalVolumeRequest{ExternalVolume: volume})
	if err != nil {
		return nil, fmt.Errorf("failed to update external volume: %w", err)
	}
	return resp, nil
}

func parseReclaimPolicy(value string) (ateapipb.ReclaimPolicy, error) {
	switch strings.ToLower(value) {
	case "":
		// Unset: the server defaults it from the storage class.
		return ateapipb.ReclaimPolicy_RECLAIM_POLICY_UNSPECIFIED, nil
	case "delete":
		return ateapipb.ReclaimPolicy_RECLAIM_POLICY_DELETE, nil
	case "retain":
		return ateapipb.ReclaimPolicy_RECLAIM_POLICY_RETAIN, nil
	default:
		return ateapipb.ReclaimPolicy_RECLAIM_POLICY_UNSPECIFIED, fmt.Errorf("invalid reclaim policy %q; must be delete or retain", value)
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
	createExternalVolumeCmd.Flags().StringVar(&createExternalVolumeVolumeIDFlag, "volume-id", "", "Volume ID of a disk that already exists, to register instead of provisioning")
	createExternalVolumeCmd.Flags().StringVar(&createExternalVolumeTypeFlag, "volume-type", "", "CSI driver name; required with --volume-id unless --storage-class names a class to read it from")
	createExternalVolumeCmd.Flags().StringToStringVar(&createExternalVolumeContextFlag, "volume-context", nil, "Driver metadata for mounting, as key=value pairs; only valid with --volume-id")
	createExternalVolumeCmd.Flags().StringVar(&createExternalVolumeReclaimFlag, "reclaim-policy", "", "What to do with the storage once nothing references it: delete or retain (default: the storage class's policy, else retain)")
	createCmd.AddCommand(createExternalVolumeCmd)

	updateExternalVolumeCmd.Flags().StringVarP(&updateExternalVolumeAtespaceFlag, "atespace", "a", "", "Atespace owning the external volume (required)")
	updateExternalVolumeCmd.Flags().StringVar(&updateExternalVolumeReclaimFlag, "reclaim-policy", "", "What to do with the storage once nothing references it: delete or retain (required)")
	_ = updateExternalVolumeCmd.MarkFlagRequired("atespace")
	_ = updateExternalVolumeCmd.MarkFlagRequired("reclaim-policy")
	updateCmd.AddCommand(updateExternalVolumeCmd)

	deleteExternalVolumeCmd.Flags().StringVarP(&deleteExternalVolumeAtespaceFlag, "atespace", "a", "", "Atespace owning the external volume (required)")
	_ = deleteExternalVolumeCmd.MarkFlagRequired("atespace")
	deleteCmd.AddCommand(deleteExternalVolumeCmd)
}
