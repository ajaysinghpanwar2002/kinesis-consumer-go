package consumer

import "context"

// Delivery is a record with an explicit acknowledgment handle. Copies share
// acknowledgment state. Changing Record or ShardID does not change that state.
// Consumers do not yet expose an explicit handler mode.
type Delivery struct {
	Record  Record
	ShardID string
	state   *deliveryState
}

// Ack accepts completion; success does not mean a checkpoint has been persisted.
// Calls may be concurrent and out of order. Repeated calls succeed while the
// delivery remains valid. A zero Delivery, an unfinished delivery from a failed
// attempt, or a delivery from an invalidated session returns ErrStaleDelivery.
// Ownership validation failures leave the acknowledgment unaccepted.
func (d Delivery) Ack(ctx context.Context) error {
	if d.state == nil {
		return ErrStaleDelivery
	}
	return d.state.tracker.ack(ctx, d.state)
}
