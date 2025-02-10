package eventbus

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
)

type EventBusTestSuite struct {
	suite.Suite
}

func (s *EventBusTestSuite) TestNew() {
	eb := New()
	s.NotNil(eb)
	s.Empty(eb.Subscribers())
}

func (s *EventBusTestSuite) TestSubscribe() {
	eb := New()

	testCases := []struct {
		name     string
		capacity int
	}{
		{"Zero capacity", 0},
		{"Small capacity", 5},
		{"Large capacity", 100},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			ch := eb.Subscribe(tc.capacity)
			s.NotNil(ch)
			s.Equal(tc.capacity, cap(ch))
			s.Len(eb.Subscribers(), 1)
			eb.Close() // Cleanup
		})
	}
}

func (s *EventBusTestSuite) TestPublish() {
	testCases := []struct {
		name     string
		events   []interface{}
		capacity int
	}{
		{
			name:     "String events",
			events:   []interface{}{"event1", "event2"},
			capacity: 2,
		},
		{
			name:     "Mixed type events",
			events:   []interface{}{42, "str", true},
			capacity: 3,
		},
		{
			name:     "Full buffer handling",
			events:   []interface{}{1, 2, 3, 4, 5},
			capacity: 2, // Smaller than number of events to test buffer full scenario
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			eb := New()
			ch := eb.Subscribe(tc.capacity)

			// Publish events
			for _, event := range tc.events {
				eb.Publish(event)
			}

			// Verify received events up to buffer capacity
			receivedCount := 0
			timeout := time.After(100 * time.Millisecond)

		receiveLoop:
			for receivedCount < cap(ch) {
				select {
				case received := <-ch:
					s.Equal(tc.events[receivedCount], received)
					receivedCount++
				case <-timeout:
					break receiveLoop
				}
			}

			eb.Close()
		})
	}
}

func (s *EventBusTestSuite) TestUnsubscribe() {
	eb := New()

	// Create multiple subscribers
	ch1 := eb.Subscribe(1)
	ch2 := eb.Subscribe(1)
	ch3 := eb.Subscribe(1)

	s.Len(eb.Subscribers(), 3)

	// Unsubscribe middle channel
	eb.Unsubscribe(ch2)

	// Verify ch2 is closed
	_, ok := <-ch2
	s.False(ok, "Unsubscribed channel should be closed")

	// Verify other channels still work
	eb.Publish("test")

	select {
	case msg := <-ch1:
		s.Equal("test", msg)
	case <-time.After(100 * time.Millisecond):
		s.Fail("Should receive message on ch1")
	}

	select {
	case msg := <-ch3:
		s.Equal("test", msg)
	case <-time.After(100 * time.Millisecond):
		s.Fail("Should receive message on ch3")
	}

	s.Len(eb.Subscribers(), 2)
	eb.Close()
}

func (s *EventBusTestSuite) TestClose() {
	eb := New()

	// Create multiple subscribers
	channels := make([]chan any, 3)
	for i := range channels {
		channels[i] = eb.Subscribe(1)
	}

	// Close the event bus
	eb.Close()

	// Verify all channels are closed
	for i, ch := range channels {
		_, ok := <-ch
		s.False(ok, "Channel %d should be closed", i)
	}

	s.Nil(eb.Subscribers(), "Subscribers slice should be nil after close")
}

func (s *EventBusTestSuite) TestConcurrentOperations() {
	eb := New()
	var wg sync.WaitGroup

	// Number of concurrent operations
	numPublishers := 10
	numSubscribers := 5
	numUnsubscribers := 3
	eventsPerPublisher := 100

	// Create initial subscribers
	channels := make([]chan any, numSubscribers)
	for i := range channels {
		channels[i] = eb.Subscribe(eventsPerPublisher)
	}

	// Start publishers
	for i := 0; i < numPublishers; i++ {
		wg.Add(1)
		go func(publisherID int) {
			defer wg.Done()
			for j := 0; j < eventsPerPublisher; j++ {
				eb.Publish(publisherID*eventsPerPublisher + j)
				time.Sleep(time.Microsecond) // Small delay to increase chance of concurrent operations
			}
		}(i)
	}

	// Start unsubscribers
	for i := 0; i < numUnsubscribers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			time.Sleep(time.Millisecond * time.Duration(i*50))
			eb.Unsubscribe(channels[i])
		}(i)
	}

	// Wait for all operations to complete
	wg.Wait()
	time.Sleep(100 * time.Millisecond) // Give time for final events to be processed

	// Verify remaining channels are still functional
	for i := numUnsubscribers; i < len(channels); i++ {
		select {
		case _, ok := <-channels[i]:
			s.True(ok, "Channel %d should still be open", i)
		default:
			// Channel might be empty, which is fine
		}
	}

	eb.Close()
}

func TestEventBusSuite(t *testing.T) {
	suite.Run(t, new(EventBusTestSuite))
}
