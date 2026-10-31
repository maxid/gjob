package gjob

import "context"

// RunOnce performs a single scan. Run is the production loop; RunOnce exists so a
// caller can drive the scheduler from its own loop (and so tests do not have to wait
// on the wall clock).
func (s *Scheduler) RunOnce(ctx context.Context) error { return s.tick(ctx) }
