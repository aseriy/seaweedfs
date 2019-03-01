package topology

import (
	"context"
	"../operation"
	"../pb/volume_server_pb"
	"../storage"
	"google.golang.org/grpc"
)

type AllocateVolumeResult struct {
	Error string
}

func AllocateVolume(dn *DataNode, grpcDialOption grpc.DialOption, vid storage.VolumeId, option *VolumeGrowOption) error {

	return operation.WithVolumeServerClient(dn.Url(), grpcDialOption, func(client volume_server_pb.VolumeServerClient) error {

		_, deleteErr := client.AssignVolume(context.Background(), &volume_server_pb.AssignVolumeRequest{
			VolumdId:    uint32(vid),
			Collection:  option.Collection,
			Replication: option.ReplicaPlacement.String(),
			Ttl:         option.Ttl.String(),
			Preallocate: option.Prealloacte,
		})
		return deleteErr
	})

}
