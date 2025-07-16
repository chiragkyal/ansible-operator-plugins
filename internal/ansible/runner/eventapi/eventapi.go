// Copyright 2018 The Operator-SDK Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package eventapi

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-logr/logr"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

// EventReceiver monitors ansible-runner event files and forwards events
type EventReceiver struct {
	// Events is the channel used by the event API handler to send JobEvents
	// back to the runner, or whatever code is using this receiver.
	Events chan JobEvent

	// ArtifactsPath is the path to the ansible-runner artifacts directory
	ArtifactsPath string

	// stopped indicates if this receiver has permanently stopped receiving
	// events. When true, no more events will be processed.
	stopped bool

	// mutex controls access to the "stopped" bool above, ensuring that writes
	// are goroutine-safe.
	mutex sync.RWMutex

	// ident is the unique identifier for a particular run of ansible-runner
	ident string

	// logger holds a logger that has some fields already set
	logger logr.Logger

	// cancel function to stop the file monitoring goroutine
	cancel context.CancelFunc

	// lastEventNum tracks the last event number processed to avoid duplicates
	lastEventNum int
}

func New(ident string, basePath string, errChan chan<- error) (*EventReceiver, error) {
	// Create the artifacts directory path for this run
	artifactsPath := filepath.Join(basePath, "artifacts")

	rec := EventReceiver{
		Events:        make(chan JobEvent, 1000),
		ArtifactsPath: artifactsPath,
		ident:         ident,
		logger:        logf.Log.WithName("eventapi").WithValues("job", ident),
		lastEventNum:  -1,
	}

	// Start monitoring for event files
	ctx, cancel := context.WithCancel(context.Background())
	rec.cancel = cancel

	go func() {
		if err := rec.monitorEventFiles(ctx); err != nil {
			rec.logger.Error(err, "Error monitoring event files")
			errChan <- err
		}
	}()

	return &rec, nil
}

// Close ensures that appropriate resources are cleaned up
func (e *EventReceiver) Close() {
	e.mutex.Lock()
	e.stopped = true
	e.mutex.Unlock()
	e.logger.V(1).Info("Event API stopped")

	if e.cancel != nil {
		e.cancel()
	}
	close(e.Events)
}

// monitorEventFiles monitors the artifacts directory for new event files
func (e *EventReceiver) monitorEventFiles(ctx context.Context) error {
	ticker := time.NewTicker(100 * time.Millisecond) // Check for new events every 100ms
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := e.processNewEvents(); err != nil {
				e.logger.Error(err, "Error processing new events")
			}
		}
	}
}

// processNewEvents scans for new event files and processes them
func (e *EventReceiver) processNewEvents() error {
	e.mutex.RLock()
	if e.stopped {
		e.mutex.RUnlock()
		return nil
	}
	e.mutex.RUnlock()

	// Look for the job events directory
	jobEventsDir := filepath.Join(e.ArtifactsPath, e.ident, "job_events")

	// Check if the directory exists yet
	if _, err := os.Stat(jobEventsDir); os.IsNotExist(err) {
		return nil // Directory doesn't exist yet, which is normal at the start
	}

	// Read all event files
	files, err := os.ReadDir(jobEventsDir)
	if err != nil {
		return fmt.Errorf("failed to read job events directory: %w", err)
	}

	// Process event files in order
	for _, file := range files {
		if !file.IsDir() && strings.HasSuffix(file.Name(), ".json") {
			// Extract event number from filename (e.g., "1-verbose.json" -> 1)
			eventNum, err := e.extractEventNumber(file.Name())
			if err != nil {
				continue // Skip files that don't match expected pattern
			}

			// Only process events we haven't seen yet
			if eventNum > e.lastEventNum {
				if err := e.processEventFile(filepath.Join(jobEventsDir, file.Name())); err != nil {
					e.logger.Error(err, "Failed to process event file", "file", file.Name())
				} else {
					e.lastEventNum = eventNum
				}
			}
		}
	}

	return nil
}

// extractEventNumber extracts the event sequence number from the filename
func (e *EventReceiver) extractEventNumber(filename string) (int, error) {
	// Event files are typically named like "1-verbose.json", "2-verbose.json", etc.
	parts := strings.Split(filename, "-")
	if len(parts) < 2 {
		return -1, fmt.Errorf("invalid event file format: %s", filename)
	}

	eventNum, err := strconv.Atoi(parts[0])
	if err != nil {
		return -1, fmt.Errorf("failed to parse event number from %s: %w", filename, err)
	}

	return eventNum, nil
}

// processEventFile reads and processes a single event file
func (e *EventReceiver) processEventFile(filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open event file %s: %w", filePath, err)
	}
	defer file.Close()

	data, err := io.ReadAll(file)
	if err != nil {
		return fmt.Errorf("failed to read event file %s: %w", filePath, err)
	}

	var event JobEvent
	if err := json.Unmarshal(data, &event); err != nil {
		return fmt.Errorf("failed to unmarshal event from %s: %w", filePath, err)
	}

	// Only process events that have a UUID (actual job events, not status events)
	if event.UUID != "" {
		// Send event to channel with timeout to avoid blocking
		timeout := time.NewTimer(10 * time.Second)
		select {
		case e.Events <- event:
		case <-timeout.C:
			e.logger.Info("Timed out writing event to channel")
			return fmt.Errorf("timeout writing event to channel")
		}
		timeout.Stop()
	}

	return nil
}
