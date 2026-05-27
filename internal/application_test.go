package internal

import (
	"testing"
)

func TestNewApplication(t *testing.T) {
	app, err := NewApplication()
	if err != nil {
		t.Fatalf("NewApplication failed: %v", err)
	}
	if app == nil {
		t.Fatal("expected app, got nil")
	}
	if app.modules == nil {
		t.Error("modules manager should be initialized")
	}
	if app.modules.Count() != 0 {
		t.Errorf("expected 0 modules initially, got %d", app.modules.Count())
	}
}

func TestApp_Stop_BeforeInit(t *testing.T) {
	app, err := NewApplication()
	if err != nil {
		t.Fatalf("NewApplication failed: %v", err)
	}
	if err := app.Stop(); err != nil {
		t.Errorf("Stop should be safe before Init: %v", err)
	}
}

func TestApp_Version(t *testing.T) {
	app, err := NewApplication()
	if err != nil {
		t.Fatalf("NewApplication failed: %v", err)
	}
	if app.Version() == "" {
		t.Error("expected non-empty version string")
	}
}
