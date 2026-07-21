package controlplane

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"
)

var errPreReadyFrontendData = errors.New("unexpected frontend data before ReadyForQuery")

// preReadyDisconnectResult tells the connection handler why the watcher
// stopped. ClientCanceled remains true even when session creation happened to
// return success at the same time, allowing the caller to tear that session
// back down instead of handing it to a vanished client.
type preReadyDisconnectResult struct {
	ClientCanceled bool
	Err            error
}

// preReadyDisconnectWatcher owns the only read running concurrently with
// session creation. Stop must be called before the reader is handed to the
// normal PostgreSQL message loop.
type preReadyDisconnectWatcher struct {
	conn   net.Conn
	reader *bufio.Reader
	cancel context.CancelFunc
	stop   chan struct{}
	done   chan preReadyDisconnectResult

	stopOnce sync.Once
	result   preReadyDisconnectResult
}

// startPreReadyDisconnectWatcher derives the context used for session
// creation and watches for a client FIN/RST while the server is waiting to
// send ReadyForQuery. PostgreSQL clients have no valid frontend message in
// this phase, so receiving data is also treated as cancellation; Peek keeps
// the byte buffered while avoiding competing consumption from the future
// message loop.
func startPreReadyDisconnectWatcher(parent context.Context, conn net.Conn, reader *bufio.Reader) (context.Context, *preReadyDisconnectWatcher) {
	ctx, cancel := context.WithCancel(parent)
	watcher := &preReadyDisconnectWatcher{
		conn:   conn,
		reader: reader,
		cancel: cancel,
		stop:   make(chan struct{}),
		done:   make(chan preReadyDisconnectResult, 1),
	}
	go watcher.watch()
	return ctx, watcher
}

func (w *preReadyDisconnectWatcher) watch() {
	for {
		_, err := w.reader.Peek(1)
		switch {
		case err == nil:
			w.finish(preReadyDisconnectResult{ClientCanceled: true, Err: errPreReadyFrontendData})
			return
		case isPreReadyReadTimeout(err):
			select {
			case <-w.stop:
				w.finish(preReadyDisconnectResult{})
				return
			default:
				// No deadline is normally armed in this phase. If a transient
				// timeout does occur independently, retain disconnect coverage.
				continue
			}
		default:
			w.finish(preReadyDisconnectResult{ClientCanceled: true, Err: err})
			return
		}
	}
}

func (w *preReadyDisconnectWatcher) finish(result preReadyDisconnectResult) {
	if result.ClientCanceled {
		w.cancel()
	}
	w.done <- result
}

// Stop joins the watcher and clears the temporary read deadline before the
// caller may reuse reader. Calling Stop more than once is safe.
func (w *preReadyDisconnectWatcher) Stop() preReadyDisconnectResult {
	w.stopOnce.Do(func() {
		close(w.stop)

		select {
		case w.result = <-w.done:
			// The peer already disconnected or sent invalid pre-ready data.
		default:
			if err := w.conn.SetReadDeadline(time.Now()); err != nil {
				// A Conn that cannot interrupt its read cannot safely be handed
				// to another reader. Close it so the watcher is still joined.
				_ = w.conn.Close()
				w.result = <-w.done
				w.result.ClientCanceled = true
				w.result.Err = fmt.Errorf("interrupt pre-ready disconnect watcher: %w", err)
				w.cancel()
				return
			}

			w.result = <-w.done
			if err := w.conn.SetReadDeadline(time.Time{}); err != nil {
				w.result.ClientCanceled = true
				w.result.Err = fmt.Errorf("clear pre-ready disconnect watcher deadline: %w", err)
			}
		}

		// Release the derived context in both the disconnect and clean-stop
		// paths. Session creation has returned before its caller invokes Stop.
		w.cancel()
	})
	return w.result
}

func isPreReadyReadTimeout(err error) bool {
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}
