package server

import (
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"
)

// fdatasync syncs the file data to disk without flushing metadata.
func fdatasync(f *os.File) error {
	return syscall.Fdatasync(int(f.Fd()))
}

const (
	walFileName    = "query_log.wal"
	walRecordAlign = 4 // records are padded to 4-byte boundaries (for future use)
)

// queryLogWAL is a write-ahead log for query log entries.
// It provides crash-resilient buffering between query execution and DuckLake flush.
type queryLogWAL struct {
	file    *os.File
	mu      sync.Mutex
	offset  int64
	maxSize int64

	// Group commit: accumulate writes and fsync on interval to amortize cost.
	groupInterval time.Duration
	pendingWrites int
	syncTimer     *time.Timer
	syncCond      *sync.Cond
	syncErr       error
	closed        bool
}

// newWAL opens or creates the WAL file at dir/query_log.wal.
// maxSize limits the WAL file size; Append returns an error when exceeded.
// groupInterval batches fsync calls (0 = sync every write).
func newWAL(dir string, maxSize int64, groupInterval time.Duration) (*queryLogWAL, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("wal: mkdir %s: %w", dir, err)
	}

	path := filepath.Join(dir, walFileName)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("wal: open %s: %w", path, err)
	}

	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("wal: stat %s: %w", path, err)
	}

	w := &queryLogWAL{
		file:          f,
		offset:        info.Size(),
		maxSize:       maxSize,
		groupInterval: groupInterval,
	}
	w.syncCond = sync.NewCond(&w.mu)
	return w, nil
}

// Append writes a query log entry to the WAL. It is safe for concurrent use.
// The entry is durably persisted (fdatasync) before returning, either immediately
// or via group commit depending on groupInterval.
func (w *queryLogWAL) Append(entry QueryLogEntry) error {
	// Encode the entry
	var buf []byte
	{
		var enc encoderBuffer
		if err := gob.NewEncoder(&enc).Encode(entry); err != nil {
			return fmt.Errorf("wal: encode: %w", err)
		}
		buf = enc.Bytes()
	}

	checksum := crc32.ChecksumIEEE(buf)

	// Build the record: [4 bytes length][4 bytes CRC32][payload]
	recordLen := uint32(len(buf))
	record := make([]byte, 8+len(buf))
	binary.BigEndian.PutUint32(record[0:4], recordLen)
	binary.BigEndian.PutUint32(record[4:8], checksum)
	copy(record[8:], buf)

	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return fmt.Errorf("wal: closed")
	}

	// Check size limit
	if w.maxSize > 0 && w.offset+int64(len(record)) > w.maxSize {
		return fmt.Errorf("wal: size limit exceeded (%d bytes)", w.maxSize)
	}

	// Write the record
	n, err := w.file.Write(record)
	if err != nil {
		return fmt.Errorf("wal: write: %w", err)
	}
	w.offset += int64(n)
	w.pendingWrites++

	if w.groupInterval <= 0 {
		// Sync immediately
		if err := fdatasync(w.file); err != nil {
			return fmt.Errorf("wal: fdatasync: %w", err)
		}
		w.pendingWrites = 0
		return nil
	}

	// Group commit: start timer if not already running, then wait for sync
	if w.syncTimer == nil {
		w.syncTimer = time.AfterFunc(w.groupInterval, w.doGroupSync)
	}

	// Wait for the group sync to complete
	myWrites := w.pendingWrites
	for w.pendingWrites >= myWrites && !w.closed {
		w.syncCond.Wait()
	}
	return w.syncErr
}

// doGroupSync is called by the timer goroutine to perform a batched fsync.
func (w *queryLogWAL) doGroupSync() {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return
	}

	w.syncErr = fdatasync(w.file)
	if w.syncErr != nil {
		slog.Error("wal: group fdatasync failed", "error", w.syncErr)
	}
	w.pendingWrites = 0
	w.syncTimer = nil
	w.syncCond.Broadcast()
}

// ReadAll reads all valid records from the WAL file.
// It stops at the first corrupted or truncated record.
func (w *queryLogWAL) ReadAll() ([]QueryLogEntry, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.readAllLocked()
}

func (w *queryLogWAL) readAllLocked() ([]QueryLogEntry, error) {
	// Seek to start
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("wal: seek: %w", err)
	}

	var entries []QueryLogEntry
	header := make([]byte, 8)

	for {
		// Read header
		_, err := io.ReadFull(w.file, header)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break // End of file or truncated header
		}
		if err != nil {
			return entries, fmt.Errorf("wal: read header: %w", err)
		}

		recordLen := binary.BigEndian.Uint32(header[0:4])
		expectedCRC := binary.BigEndian.Uint32(header[4:8])

		// Sanity check record length
		if recordLen > 16*1024*1024 { // 16MB max single record
			break // Corrupted
		}

		payload := make([]byte, recordLen)
		_, err = io.ReadFull(w.file, payload)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break // Truncated record
		}
		if err != nil {
			return entries, fmt.Errorf("wal: read payload: %w", err)
		}

		// Verify CRC
		actualCRC := crc32.ChecksumIEEE(payload)
		if actualCRC != expectedCRC {
			slog.Warn("wal: CRC mismatch, stopping replay", "expected", expectedCRC, "actual", actualCRC)
			break
		}

		// Decode
		var entry QueryLogEntry
		dec := gob.NewDecoder(&decoderBuffer{data: payload})
		if err := dec.Decode(&entry); err != nil {
			slog.Warn("wal: decode error, stopping replay", "error", err)
			break
		}

		entries = append(entries, entry)
	}

	// Seek back to end for subsequent appends
	if _, err := w.file.Seek(0, io.SeekEnd); err != nil {
		return entries, fmt.Errorf("wal: seek end: %w", err)
	}

	return entries, nil
}

// Truncate clears the WAL file after all entries have been flushed to DuckLake.
func (w *queryLogWAL) Truncate() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.file.Truncate(0); err != nil {
		return fmt.Errorf("wal: truncate: %w", err)
	}
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("wal: seek after truncate: %w", err)
	}
	w.offset = 0
	return nil
}

// Size returns the current WAL file size in bytes.
func (w *queryLogWAL) Size() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.offset
}

// Close closes the WAL file.
func (w *queryLogWAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.closed = true
	if w.syncTimer != nil {
		w.syncTimer.Stop()
	}
	// Final sync before close
	_ = fdatasync(w.file)
	w.syncCond.Broadcast()
	return w.file.Close()
}

// encoderBuffer is a simple bytes.Buffer replacement for gob encoding.
type encoderBuffer struct {
	data []byte
}

func (b *encoderBuffer) Write(p []byte) (int, error) {
	b.data = append(b.data, p...)
	return len(p), nil
}

func (b *encoderBuffer) Bytes() []byte {
	return b.data
}

// decoderBuffer wraps a byte slice for gob decoding.
type decoderBuffer struct {
	data []byte
	pos  int
}

func (b *decoderBuffer) Read(p []byte) (int, error) {
	if b.pos >= len(b.data) {
		return 0, io.EOF
	}
	n := copy(p, b.data[b.pos:])
	b.pos += n
	return n, nil
}
