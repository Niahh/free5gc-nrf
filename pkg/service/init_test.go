package service

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestDropGraceElapsed pins the boundary: with timer 10 and suspendFactor 2
// the drop sweep stays quiet through the first 30 seconds.
func TestDropGraceElapsed(t *testing.T) {
	assert.False(t, dropGraceElapsed(30*time.Second, 10, 2))
	assert.True(t, dropGraceElapsed(31*time.Second, 10, 2))
}
