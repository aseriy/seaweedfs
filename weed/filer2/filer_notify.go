package filer2

import (
	"../glog"
	"../notification"
	"../pb/filer_pb"
)

func (f *Filer) NotifyUpdateEvent(oldEntry, newEntry *Entry, deleteChunks bool) {
	var key string
	if oldEntry != nil {
		key = string(oldEntry.FullPath)
	} else if newEntry != nil {
		key = string(newEntry.FullPath)
	} else {
		return
	}

	if notification.Queue != nil {

		glog.V(3).Infof("notifying entry update %v", key)

		notification.Queue.SendMessage(
			key,
			&filer_pb.EventNotification{
				OldEntry:     oldEntry.ToProtoEntry(),
				NewEntry:     newEntry.ToProtoEntry(),
				DeleteChunks: deleteChunks,
			},
		)

	}
}
