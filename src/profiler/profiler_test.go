package profiler

import (
	"github.com/magiconair/properties/assert"
	"testing"
	"time"
)

func TestProfile_Start(t *testing.T) {
	profile := profile{}

	assert.Equal(t, profile.StartedAt.IsZero(), true)
	profile.Start()
	assert.Equal(t, profile.StartedAt.IsZero(), false)
}

func TestProfile_End(t *testing.T) {
	profile := profile{}

	assert.Panic(t, profile.End, "Attempting to end an unstarted profile")
	assert.Equal(t, profile.EndedAt.IsZero(), true)
	assert.Equal(t, int(profile.Delta.Seconds()), 0)

	profile.Start()
	time.Sleep(time.Second * 1)
	profile.End()

	assert.Equal(t, profile.EndedAt.IsZero(), false)
	assert.Equal(t, int(profile.Delta.Seconds()), 1)
}

func TestNew(t *testing.T) {
	profile := New()

	assert.Panic(t, profile.Start, "Attempting to start an already started profile")
	assert.Equal(t, profile.StartedAt.IsZero(), false)
	assert.Equal(t, profile.EndedAt.IsZero(), true)
	assert.Equal(t, int(profile.Delta.Seconds()), 0)
}
