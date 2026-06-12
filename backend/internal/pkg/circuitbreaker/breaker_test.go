package circuitbreaker

import (
	"errors"
	"testing"
	"time"
)

func TestCircuitBreaker_NormalOperation(t *testing.T) {
	cb := New(Config{
		FailureThreshold: 3,
		SuccessThreshold: 2,
		Timeout:          100 * time.Millisecond,
		HalfOpenMaxCalls: 2,
	})

	// Should allow requests when closed
	if !cb.AllowRequest() {
		t.Fatal("Should allow request when closed")
	}

	// Record successes
	cb.RecordSuccess()
	cb.RecordSuccess()

	if cb.State() != StateClosed {
		t.Fatalf("Expected state Closed, got %v", cb.State())
	}
}

func TestCircuitBreaker_OpensAfterFailures(t *testing.T) {
	cb := New(Config{
		FailureThreshold: 3,
		SuccessThreshold: 2,
		Timeout:          100 * time.Millisecond,
		HalfOpenMaxCalls: 2,
	})

	// Record failures
	for i := 0; i < 3; i++ {
		cb.RecordFailure()
	}

	if cb.State() != StateOpen {
		t.Fatalf("Expected state Open after %d failures, got %v", 3, cb.State())
	}

	if cb.AllowRequest() {
		t.Fatal("Should not allow request when open")
	}
}

func TestCircuitBreaker_HalfOpenAfterTimeout(t *testing.T) {
	cb := New(Config{
		FailureThreshold: 3,
		SuccessThreshold: 2,
		Timeout:          50 * time.Millisecond,
		HalfOpenMaxCalls: 2,
	})

	// Open the circuit
	for i := 0; i < 3; i++ {
		cb.RecordFailure()
	}

	if cb.State() != StateOpen {
		t.Fatal("Circuit should be open")
	}

	// Wait for timeout
	time.Sleep(60 * time.Millisecond)

	// Should transition to half-open
	if !cb.AllowRequest() {
		t.Fatal("Should allow request after timeout")
	}

	if cb.State() != StateHalfOpen {
		t.Fatalf("Expected state HalfOpen, got %v", cb.State())
	}
}

func TestCircuitBreaker_ClosesAfterSuccessesInHalfOpen(t *testing.T) {
	cb := New(Config{
		FailureThreshold: 3,
		SuccessThreshold: 2,
		Timeout:          50 * time.Millisecond,
		HalfOpenMaxCalls: 3,
	})

	// Open the circuit
	for i := 0; i < 3; i++ {
		cb.RecordFailure()
	}

	// Wait for timeout
	time.Sleep(60 * time.Millisecond)

	// Enter half-open
	cb.AllowRequest()

	// Record successes
	cb.RecordSuccess()
	cb.RecordSuccess()

	if cb.State() != StateClosed {
		t.Fatalf("Expected state Closed after successes, got %v", cb.State())
	}
}

func TestCircuitBreaker_ReopensOnFailureInHalfOpen(t *testing.T) {
	cb := New(Config{
		FailureThreshold: 3,
		SuccessThreshold: 2,
		Timeout:          50 * time.Millisecond,
		HalfOpenMaxCalls: 3,
	})

	// Open the circuit
	for i := 0; i < 3; i++ {
		cb.RecordFailure()
	}

	// Wait for timeout
	time.Sleep(60 * time.Millisecond)

	// Enter half-open
	cb.AllowRequest()

	if cb.State() != StateHalfOpen {
		t.Fatal("Should be half-open")
	}

	// Record a failure
	cb.RecordFailure()

	if cb.State() != StateOpen {
		t.Fatalf("Expected state Open after failure in half-open, got %v", cb.State())
	}
}

func TestCircuitBreaker_Call(t *testing.T) {
	cb := New(DefaultConfig())

	// Successful call
	err := cb.Call(func() error {
		return nil
	})
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	// Failed call
	testErr := errors.New("test error")
	err = cb.Call(func() error {
		return testErr
	})
	if err != testErr {
		t.Fatalf("Expected test error, got %v", err)
	}
}

func TestCircuitBreaker_HalfOpenMaxCalls(t *testing.T) {
	cb := New(Config{
		FailureThreshold: 2,
		SuccessThreshold: 2,
		Timeout:          50 * time.Millisecond,
		HalfOpenMaxCalls: 2,
	})

	// Open the circuit
	cb.RecordFailure()
	cb.RecordFailure()

	// Wait for timeout
	time.Sleep(60 * time.Millisecond)

	// Allow max calls
	if !cb.AllowRequest() {
		t.Fatal("Should allow first request")
	}
	if !cb.AllowRequest() {
		t.Fatal("Should allow second request")
	}

	// Should reject third request
	if cb.AllowRequest() {
		t.Fatal("Should not allow third request")
	}
}
