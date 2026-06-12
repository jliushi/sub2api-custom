package circuitbreaker

import (
	"errors"
	"sync"
	"time"
)

var (
	ErrCircuitOpen     = errors.New("circuit breaker is open")
	ErrCircuitHalfOpen = errors.New("circuit breaker is half-open, request rejected")
)

// State represents the circuit breaker state
type State int

const (
	StateClosed State = iota // Normal operation
	StateOpen                // Circuit is open, requests are rejected
	StateHalfOpen            // Testing if service has recovered
)

func (s State) String() string {
	switch s {
	case StateClosed:
		return "closed"
	case StateOpen:
		return "open"
	case StateHalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}

// Config holds circuit breaker configuration
type Config struct {
	FailureThreshold int           // Number of failures before opening
	SuccessThreshold int           // Number of successes in half-open to close
	Timeout          time.Duration // Time to wait before entering half-open
	HalfOpenMaxCalls int           // Max concurrent calls in half-open state
}

// DefaultConfig returns default circuit breaker configuration
func DefaultConfig() Config {
	return Config{
		FailureThreshold: 5,
		SuccessThreshold: 2,
		Timeout:          30 * time.Second,
		HalfOpenMaxCalls: 3,
	}
}

// CircuitBreaker implements the circuit breaker pattern
type CircuitBreaker struct {
	mu sync.RWMutex

	config Config
	state  State

	failures      int
	successes     int
	lastFailTime  time.Time
	lastStateTime time.Time

	halfOpenCalls int
}

// New creates a new circuit breaker with the given configuration
func New(config Config) *CircuitBreaker {
	if config.FailureThreshold <= 0 {
		config.FailureThreshold = 5
	}
	if config.SuccessThreshold <= 0 {
		config.SuccessThreshold = 2
	}
	if config.Timeout <= 0 {
		config.Timeout = 30 * time.Second
	}
	if config.HalfOpenMaxCalls <= 0 {
		config.HalfOpenMaxCalls = 3
	}

	return &CircuitBreaker{
		config:        config,
		state:         StateClosed,
		lastStateTime: time.Now(),
	}
}

// Call executes the given function if the circuit breaker allows it
func (cb *CircuitBreaker) Call(fn func() error) error {
	if !cb.AllowRequest() {
		return ErrCircuitOpen
	}

	err := fn()
	if err != nil {
		cb.RecordFailure()
		return err
	}

	cb.RecordSuccess()
	return nil
}

// AllowRequest checks if a request should be allowed
func (cb *CircuitBreaker) AllowRequest() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	now := time.Now()

	switch cb.state {
	case StateClosed:
		return true

	case StateOpen:
		// Check if timeout has passed
		if now.Sub(cb.lastStateTime) >= cb.config.Timeout {
			cb.toHalfOpen()
			cb.halfOpenCalls++
			return true
		}
		return false

	case StateHalfOpen:
		// Allow limited number of requests in half-open state
		if cb.halfOpenCalls < cb.config.HalfOpenMaxCalls {
			cb.halfOpenCalls++
			return true
		}
		return false
	}

	return false
}

// RecordSuccess records a successful request
func (cb *CircuitBreaker) RecordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.failures = 0

	switch cb.state {
	case StateClosed:
		// Already closed, nothing to do

	case StateHalfOpen:
		cb.successes++
		if cb.successes >= cb.config.SuccessThreshold {
			cb.toClosed()
		}
	}
}

// RecordFailure records a failed request
func (cb *CircuitBreaker) RecordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.failures++
	cb.lastFailTime = time.Now()

	switch cb.state {
	case StateClosed:
		if cb.failures >= cb.config.FailureThreshold {
			cb.toOpen()
		}

	case StateHalfOpen:
		// Any failure in half-open state reopens the circuit
		cb.toOpen()
	}
}

// State returns the current state of the circuit breaker
func (cb *CircuitBreaker) State() State {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return cb.state
}

// IsOpen returns true if the circuit is open
func (cb *CircuitBreaker) IsOpen() bool {
	return cb.State() == StateOpen
}

// Reset resets the circuit breaker to closed state
func (cb *CircuitBreaker) Reset() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.toClosed()
}

// Internal state transition methods (must be called with lock held)

func (cb *CircuitBreaker) toOpen() {
	cb.state = StateOpen
	cb.lastStateTime = time.Now()
	cb.successes = 0
	cb.halfOpenCalls = 0
}

func (cb *CircuitBreaker) toHalfOpen() {
	cb.state = StateHalfOpen
	cb.lastStateTime = time.Now()
	cb.successes = 0
	cb.halfOpenCalls = 0
}

func (cb *CircuitBreaker) toClosed() {
	cb.state = StateClosed
	cb.lastStateTime = time.Now()
	cb.failures = 0
	cb.successes = 0
	cb.halfOpenCalls = 0
}
