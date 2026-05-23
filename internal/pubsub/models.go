package pubsub

type SimpleQueueType string

func (sq SimpleQueueType) durable() bool {
	return sq == "durable"
}

func (sq SimpleQueueType) autoDelete() bool {
	return sq == "transient"
}

func (sq SimpleQueueType) exlusive() bool {
	return sq == "transient"
}
