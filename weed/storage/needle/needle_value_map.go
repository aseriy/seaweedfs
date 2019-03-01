package needle

import (
	. "../types"
)

type NeedleValueMap interface {
	Set(key NeedleId, offset Offset, size uint32) (oldOffset Offset, oldSize uint32)
	Delete(key NeedleId) uint32
	Get(key NeedleId) (*NeedleValue, bool)
	Visit(visit func(NeedleValue) error) error
}
