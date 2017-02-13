package api

import (
	"fmt"
	"testing"

	"gitlab.fg/otis/iris/pb"
)

func TestRemoveHandler(t *testing.T) {
	var handler1 UpdateHandler = func(u *pb.Update) error { fmt.Println("handler1"); return nil }
	var handler2 UpdateHandler = func(u *pb.Update) error { fmt.Println("handler2"); return nil }
	var handler3 UpdateHandler = func(u *pb.Update) error { fmt.Println("handler3"); return nil }
	var handlers = []*UpdateHandler{&handler1, &handler2, &handler3}

	count := len(handlers)
	for i := 0; i < count; i++ {
		handlers = removeHandler(handlers[len(handlers)-1], handlers)
	}

	if len(handlers) > 0 {
		t.Error("Handlers array should have no handlers left after removal.  Found", count)
		return
	}
}
