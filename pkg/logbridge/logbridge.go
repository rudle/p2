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
func Copy(dest io.Writer, src io.Reader, capacity int, logger logging.Logger) {
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
				if err := writeDroppedMessageWarning(dest); err != nil {
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
			logger.WithError(err).WithField("dropped line", line).WithField("retried", retriableError(err)).WithField("bytes written", n).Errorln("Encountered a non-recoverable error. Proceeding.")
		}
	}
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

func retriableError(err error) bool {
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
		if err == nil || !retriableError(err) {
			return n, err
		}
		logger.WithError(err).Errorf("Retriable error, retry %d of %d", attempt, totalAttempts)
		time.Sleep(backoff(attempt))
	}

	return n, err
}

func writeDroppedMessageWarning(w io.Writer) error {
	retries := 3
	var err error
	for attempt := 0; attempt < retries; attempt++ {
		if _, err = w.Write([]byte("logbridge is dropping messages, which may result in a lack of visibility. More information can be found in logbridge's logs")); err == nil {
			return nil
		}
	}

	return err
}