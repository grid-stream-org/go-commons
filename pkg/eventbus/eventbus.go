package eventbus

import "sync"

type EventBus interface {
	Subscribe(capacity int) chan any
	Publish(event any)
	Unsubscribe(ch chan any)
	Subscribers() []chan any
	Close()
}

type eventBus struct {
	subscribers []chan any
	mu          sync.Mutex
}

func New() EventBus {
	return &eventBus{
		subscribers: []chan any{},
	}
}

func (eb *eventBus) Subscribe(capacity int) chan any {
	eb.mu.Lock()
	defer eb.mu.Unlock()

	ch := make(chan any, capacity)
	eb.subscribers = append(eb.subscribers, ch)
	return ch
}

func (eb *eventBus) Publish(event any) {
	eb.mu.Lock()
	defer eb.mu.Unlock()

	for _, ch := range eb.subscribers {
		select {
		case ch <- event:
		default:
		}
	}
}

func (eb *eventBus) Unsubscribe(ch chan any) {
	eb.mu.Lock()
	defer eb.mu.Unlock()

	for i, sub := range eb.subscribers {
		if sub == ch {
			eb.subscribers = append(eb.subscribers[:i], eb.subscribers[i+1:]...)
			close(ch)
			break
		}
	}
}

func (eb *eventBus) Subscribers() []chan any {
	return eb.subscribers
}

func (eb *eventBus) Close() {
	eb.mu.Lock()
	defer eb.mu.Unlock()

	for _, ch := range eb.subscribers {
		close(ch)
	}
	eb.subscribers = nil
}
