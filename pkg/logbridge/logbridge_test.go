package logbridge

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/square/p2/pkg/logging"
)

const newLine = byte(10)

type TrackingWriter struct {
	bytes     []byte
	numWrites int
}

func (tw *TrackingWriter) Write(p []byte) (int, error) {
	tw.numWrites++
	return len(p), nil
}

func TestLogBridge(t *testing.T) {
	type testCase struct {
		inputSize      int
		bridgeCapacity int
		expected       int
	}

	testCases := []testCase{
		{inputSize: 0, bridgeCapacity: 0, expected: 0},
		{inputSize: 10, bridgeCapacity: 10, expected: 10},
		{inputSize: 100, bridgeCapacity: 10, expected: 10},
		{inputSize: 10, bridgeCapacity: 100, expected: 10},
	}

	for i, testCase := range testCases {
		t.Logf("test case %d", i)
		input := make([]byte, 0, 2*testCase.inputSize)
		for i := 0; i < testCase.inputSize; i++ {
			input = append(input, byte('a'), newLine)
		}

		reader := bytes.NewReader(input)
		writer := &TrackingWriter{}

		LossyCopy(writer, reader, testCase.bridgeCapacity, logging.DefaultLogger)
		if writer.numWrites < testCase.expected {
			t.Errorf("Writer did not receive enough writes, got %d expected: %d", writer.numWrites, testCase.inputSize)
		}
	}
}

// This writer becomes latent after receiving capacity writes.
type LatentWriter struct {
	capacity int
	writes   int
	Bytes    []byte
}

func (sw *LatentWriter) Write(p []byte) (n int, err error) {
	if sw.writes > sw.capacity {
		time.Sleep(10 * time.Millisecond)
	}
	sw.writes++

	sw.Bytes = append(sw.Bytes, p...)
	return len(p), nil
}

func TestLogBridgeLogDrop(t *testing.T) {
	bridgeCapacity := 3
	input := []byte{
		byte('a'), newLine,
		byte('b'), newLine,
		byte('c'), newLine,
		byte('d'), newLine,
		byte('e'), newLine,
		byte('f'), newLine}
	reader := bytes.NewReader(input)
	writer := &LatentWriter{}

	LossyCopy(writer, reader, bridgeCapacity, logging.DefaultLogger)

	if writer.writes < bridgeCapacity {
		t.Errorf("Expected at least %d messages to succeed under writer latency, got %d.", bridgeCapacity, writer.writes)
	}
}

// This writer returns errors of type (*ErrorWriter).err after numSuccess calls to Write
type errorWriter struct {
	numSuccess int
	writes     int
	bytes      []byte
}

type transientErrorWriter struct {
	numSuccess int
	writes     int
	bytes      []byte
}

func (ew *errorWriter) Write(p []byte) (n int, err error) {
	if ew.writes > ew.numSuccess {
		return 0, errors.New("boom")
	}

	ew.writes++

	ew.bytes = append(ew.bytes, p...)

	return len(p), nil
}

func (tew *transientErrorWriter) Write(p []byte) (n int, err error) {
	tew.writes++

	if tew.writes > tew.numSuccess && tew.writes%2 == 0 {
		return 0, &RetriableError{err: errors.New("boom")}
	}

	tew.bytes = append(tew.bytes, p...)
	return len(p), nil
}

func TestErrorCases(t *testing.T) {
	input := []byte{
		byte('a'), newLine,
		byte('b'), newLine,
		byte('c'), newLine,
		byte('d'), newLine,
		byte('e'), newLine,
		byte('f'), newLine}
	bridgeCapacity := 6

	// we're testing errors and want the tests to be fast
	backoff = func(_ int) time.Duration { return time.Duration(0 * time.Millisecond) }

	retriableErrorWriter := &transientErrorWriter{
		numSuccess: 2,
	}

	errorWriter := &errorWriter{
		numSuccess: 4,
	}

	reader := bytes.NewReader(input)
	LossyCopy(errorWriter, reader, bridgeCapacity, logging.DefaultLogger)

	if len(errorWriter.bytes) >= len(input) {
		t.Errorf("Expected non-retriable error to cause line to be skipped.")
	}

	reader = bytes.NewReader(input)
	LossyCopy(retriableErrorWriter, reader, bridgeCapacity, logging.DefaultLogger)

	if len(retriableErrorWriter.bytes) != bridgeCapacity {
		t.Errorf("Expected bridge to successfully retry writes that result in error. Expected %d Got %d", bridgeCapacity, len(retriableErrorWriter.bytes))
	}
}
