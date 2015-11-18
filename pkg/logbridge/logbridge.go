package logbridge

import (
	"bufio"
	"io"
	"time"

	"github.com/square/p2/pkg/logging"
)

// Copy implements a buffered copy operation between dest and src.
// It returns the number of dropped messages as a result of insufficient
// capacity
// The caller is responsible for closing & flushing the reader and writer.
func LossyCopy(dest io.Writer, src io.Reader, capacity int, logger logging.Logger) {
	lines := make(chan []byte, capacity)
	droppedMessages := make(chan []byte, capacity)
	go func(dest io.Writer, src io.Reader, lines chan []byte) {
		defer close(lines)

		scanner := bufio.NewScanner(src)
		var line []byte
		for scanner.Scan() {
			line = scanner.Bytes() // consume a line regardless of the state of the writer
			select {
			case lines <- line:
			default:
				select {
				case droppedMessages <- line:
				default:
				}
				logger.WithField("dropped line", line).Errorln("Dropped was dropped due to full capacity. If this occurs frequently, consider increasing the capacity of this logbridge.")
			}
		}
		if err := scanner.Err(); err != nil {
			logger.WithError(err).Errorln("Encountered error while reading from src. Proceeding.")
		}
	}(dest, src, lines)

	go func(droppedMessages chan []byte) {
		defer close(droppedMessages)

		numDropped := 0
		for range droppedMessages {
			numDropped++
			// We rate limit these messages to avoid swamping our writer with noise.
			if numDropped%10 == 0 {
				if _, err := writeWithRetry(dest,
					[]byte("logbridge is dropping messages. This may result in a lack of visibility. Complete app logs are available on disk."),
					logger); err != nil {
					logger.WithError(err).Errorln("Failed to notify backend log system about dropped messages.")
				}
			}
		}
	}(droppedMessages)

	var n int
	var err error
	for line := range lines {
		n, err = writeWithRetry(dest, line, logger)
		if err != nil {
			select {
			case droppedMessages <- line:
			default:
			}
			logger.WithError(err).WithField("dropped line", line).WithField("retried", isRetriable(err)).WithField("bytes written", n).Errorln("Encountered a non-recoverable error. Proceeding.")
		}
	}
}

// Tee will copy to faithfulWriter without dropping messages, it will copy
// through a buffer to better handle mismatched latencies. Lines written to
// lossyWriter will be copied in a best effort way with respect to latency and
// buffered through a go channel.
func Tee(r io.Reader, faithfulWriter io.Writer, lossyWriter io.Writer, logger logging.Logger) {
	tr := io.TeeReader(r, bufio.NewWriterSize(faithfulWriter, 1<<10))

	LossyCopy(lossyWriter, tr, 1<<20, logger)
}

// This is an error wrapper type that may be used to denote an error is retriable
// RetriableError is exported so clients of this package can express their
// error semantics to this package
type RetriableError struct {
	err error
}

func (r RetriableError) Error() string {
	return r.err.Error()
}

func isRetriable(err error) bool {
	_, ok := err.(*RetriableError)
	return ok
}

var backoff = func(i int) time.Duration {
	return time.Duration(1 << uint(i) * time.Second)
}

func writeWithRetry(w io.Writer, line []byte, logger logging.Logger) (int, error) {
	var err error
	var n int
	totalAttempts := 5

	for attempt := 1; attempt <= totalAttempts; attempt++ {
		n, err = w.Write(line)
		if err == nil || !isRetriable(err) {
			return n, err
		}
		logger.WithError(err).Errorf("Retriable error, retry %d of %d", attempt, totalAttempts)
		time.Sleep(backoff(attempt))
	}

	return n, err
}
