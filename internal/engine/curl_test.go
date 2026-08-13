package engine

import (
	"testing"
	"time"
)

func TestRelaySetRotation(t *testing.T) {
	s := NewRelaySet("https://a.example", "https://b.example")
	seen := map[string]int{}
	for i := 0; i < 6; i++ {
		r := s.Pick()
		if r == nil {
			t.Fatal("Pick returned nil with healthy relays")
		}
		seen[r.Base]++
	}
	if seen["https://a.example"] != 3 || seen["https://b.example"] != 3 {
		t.Fatalf("rotation not balanced: %v", seen)
	}
}

func TestRelaySetFailover(t *testing.T) {
	s := NewRelaySet("https://a.example", "https://b.example")
	s.MarkBad("https://a.example")
	for i := 0; i < 4; i++ {
		if r := s.Pick(); r == nil || r.Base != "https://b.example" {
			t.Fatalf("expected only b.example healthy, got %+v", r)
		}
	}
	if s.AllBad() {
		t.Fatal("AllBad true with one healthy relay")
	}
	s.MarkBad("https://b.example")
	if !s.AllBad() {
		t.Fatal("AllBad false with all relays in cooldown")
	}
	if r := s.Pick(); r != nil {
		t.Fatalf("Pick should be nil when all bad, got %+v", r)
	}
}

func TestRelaySetCooldownRecovery(t *testing.T) {
	s := NewRelaySet("https://a.example")
	s.Cooldown = 50 * time.Millisecond
	s.MarkBad("https://a.example")
	if !s.AllBad() {
		t.Fatal("expected bad immediately after MarkBad")
	}
	time.Sleep(80 * time.Millisecond)
	if s.AllBad() {
		t.Fatal("relay should recover after cooldown expires")
	}
	if r := s.Pick(); r == nil {
		t.Fatal("Pick should work after cooldown expiry")
	}
}

func TestRelaySetSpeeds(t *testing.T) {
	s := NewRelaySet("https://a.example", "https://b.example")
	s.SetSpeed("https://a.example", 3.5)
	s.SetSpeed("https://b.example", 7.0)
	sp := s.Speeds()
	if sp["https://a.example"] != 3.5 || sp["https://b.example"] != 7.0 {
		t.Fatalf("speeds wrong: %v", sp)
	}
	if got := len(s.Bases()); got != 2 {
		t.Fatalf("Bases length = %d", got)
	}
}

// TestRelayPickPrefersHealthy verifies the weighted pick keeps a relay with
// recent failures out of the hot path even after its soft cooldown expires.
func TestRelayPickPrefersHealthy(t *testing.T) {
	s := NewRelaySet("https://a.example", "https://b.example")
	s.SoftCooldown = 2 * time.Millisecond
	s.MarkBadSoft("https://a.example") // a: one recent failure
	time.Sleep(5 * time.Millisecond)   // cooldown expires, history stays
	seen := map[string]int{}
	for i := 0; i < 200; i++ {
		r := s.Pick()
		if r == nil {
			t.Fatal("Pick returned nil")
		}
		seen[r.Base]++
	}
	// only the ~8% exploration picks may hit the degraded relay
	if seen["https://b.example"] < 170 {
		t.Fatalf("healthy relay underused: %v", seen)
	}
}

// TestRelayPickPrefersFast verifies that among equally clean relays the
// faster one (probe speed) carries the load, keeping a slow-but-alive relay
// out of the hot path.
func TestRelayPickPrefersFast(t *testing.T) {
	s := NewRelaySet("https://a.example", "https://b.example")
	s.SetSpeed("https://a.example", 0.5)
	s.SetSpeed("https://b.example", 6.0)
	for i := 0; i < 8; i++ {
		if r := s.Pick(); r == nil || r.Base != "https://b.example" {
			t.Fatalf("pick %d = %+v, want faster b.example", i, r)
		}
	}
}

// TestRelayNoteSuccessRecovers verifies a relay that fails then succeeds is
// restored to full health and rejoins the balanced rotation.
func TestRelayNoteSuccessRecovers(t *testing.T) {
	s := NewRelaySet("https://a.example", "https://b.example")
	s.SoftCooldown = time.Millisecond
	s.MarkBadSoft("https://a.example")
	time.Sleep(3 * time.Millisecond)
	s.NoteSuccess("https://a.example") // streak cleared -> healthy again
	seen := map[string]int{}
	for i := 0; i < 6; i++ {
		r := s.Pick()
		if r == nil {
			t.Fatal("Pick returned nil")
		}
		seen[r.Base]++
	}
	if seen["https://a.example"] != 3 || seen["https://b.example"] != 3 {
		t.Fatalf("rotation not balanced after recovery: %v", seen)
	}
}
