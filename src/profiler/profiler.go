package profiler

import (
	"github.com/inspectadb/inspectadb/src/errs"
	"time"
)

type profile struct {
	StartedAt time.Time
	EndedAt   time.Time
	Delta     time.Duration
}

func (p *profile) Start() {
	if !p.StartedAt.IsZero() {
		panic(errs.ProfileAlreadyStarted)
	}

	p.StartedAt = time.Now()
}

func (p *profile) End() {
	if p.StartedAt.IsZero() {
		panic(errs.ProfileAlreadyEnded)
	}

	p.EndedAt = time.Now()
	p.Delta = p.EndedAt.Sub(p.StartedAt)
}

func New() *profile {
	p := profile{}
	p.Start()

	return &p
}
