package profiler

import (
	"time"
)

type profile struct {
	StartedAt time.Time
	EndedAt   time.Time
	Delta     time.Duration
}

func (p *profile) Start() {
	if !p.StartedAt.IsZero() {
		panic("Attempting to start an already started profile")
	}

	p.StartedAt = time.Now()
}

func (p *profile) End() {
	if p.StartedAt.IsZero() {
		panic("Attempting to end an unstarted profile")
	}

	p.EndedAt = time.Now()
	p.Delta = p.EndedAt.Sub(p.StartedAt)
}

func New() *profile {
	p := profile{}
	p.Start()

	return &p
}
