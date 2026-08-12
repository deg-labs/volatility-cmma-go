package main

import (
	"encoding/json"
	"math"
	"net/http/httptest"
	"testing"
)

func TestWriteJSONStatusUsesValidFallbackAfterMarshalFailure(t *testing.T) {
	recorder := httptest.NewRecorder()
	writeJSONStatus(recorder, 200, math.Inf(1))

	if recorder.Code != 500 {
		t.Fatalf("status = %d, want 500", recorder.Code)
	}
	var payload map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatalf("fallback response is not valid JSON: %v", err)
	}
}
