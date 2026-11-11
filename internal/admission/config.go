package admission

import (
	"fmt"
	"time"
)

// Policy is a complete account policy. An absent override inherits Defaults.
// Zero queue capacity disables waiting; zero concurrency is always invalid.
type Policy struct {
	MaxConcurrency int  `json:"max_concurrency" yaml:"max_concurrency"`
	MaxQueue       int  `json:"max_queue" yaml:"max_queue"`
	WaitTimeoutMS  int  `json:"wait_timeout_ms" yaml:"wait_timeout_ms"`
	Paused         bool `json:"paused" yaml:"paused"`
}

type Config struct {
	Version            int               `json:"version" yaml:"version"`
	Defaults           Policy            `json:"defaults" yaml:"defaults"`
	Accounts           map[string]Policy `json:"accounts" yaml:"accounts"`
	PinnedFullBehavior string            `json:"pinned_full_behavior" yaml:"pinned_full_behavior"`
	// DisableSessionAffinity is omitted while false so that a state file
	// written with affinity on stays readable by versions that predate it.
	DisableSessionAffinity bool `json:"disable_session_affinity,omitempty" yaml:"-"`
	// HostSelection leaves account selection to the host's own selector; the
	// plugin then only limits and queues on the account the host chose.
	// Omitted while false, for the same reason.
	HostSelection bool `json:"host_selection,omitempty" yaml:"-"`
}

func DefaultConfig() Config {
	return Config{Version: 1, Defaults: Policy{MaxConcurrency: 2, MaxQueue: 20, WaitTimeoutMS: 60000}, Accounts: map[string]Policy{}, PinnedFullBehavior: "queue"}
}

func (p Policy) Validate() error {
	if p.MaxConcurrency < 1 || p.MaxConcurrency > 1000 {
		return fmt.Errorf("max_concurrency must be between 1 and 1000")
	}
	if p.MaxQueue < 0 || p.MaxQueue > 1000 {
		return fmt.Errorf("max_queue must be between 0 and 1000")
	}
	if p.WaitTimeoutMS < 1 || p.WaitTimeoutMS > 600000 {
		return fmt.Errorf("wait_timeout_ms must be between 1 and 600000")
	}
	return nil
}

func (c Config) Validate() error {
	if c.Version != 1 {
		return fmt.Errorf("unsupported configuration version")
	}
	if c.PinnedFullBehavior != "" && c.PinnedFullBehavior != "queue" && c.PinnedFullBehavior != "fallback" {
		return fmt.Errorf("pinned_full_behavior must be queue or fallback")
	}
	if err := c.Defaults.Validate(); err != nil {
		return fmt.Errorf("defaults: %w", err)
	}
	if len(c.Accounts) > 4096 {
		return fmt.Errorf("too many account overrides")
	}
	for id, p := range c.Accounts {
		if id == "" || len(id) > 512 {
			return fmt.Errorf("invalid account identifier")
		}
		if err := p.Validate(); err != nil {
			return fmt.Errorf("invalid account policy: %w", err)
		}
	}
	return nil
}

func (c Config) Clone() Config {
	if c.PinnedFullBehavior == "" {
		c.PinnedFullBehavior = "queue"
	}
	n := c
	n.Accounts = make(map[string]Policy, len(c.Accounts))
	for k, v := range c.Accounts {
		n.Accounts[k] = v
	}
	return n
}

func (c Config) Policy(id string) Policy {
	if p, ok := c.Accounts[id]; ok {
		return p
	}
	return c.Defaults
}

func (p Policy) Wait() time.Duration { return time.Duration(p.WaitTimeoutMS) * time.Millisecond }
