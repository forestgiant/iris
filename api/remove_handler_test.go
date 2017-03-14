package api

import (
	"fmt"
	"testing"

	"github.com/forestgiant/iris/pb"
)

func TestRemoveHandler(t *testing.T) {
	var handler1 UpdateHandler = func(u *pb.Update) { fmt.Println("handler1") }
	var handler2 UpdateHandler = func(u *pb.Update) { fmt.Println("handler2") }
	var handler3 UpdateHandler = func(u *pb.Update) { fmt.Println("handler3") }
	handlers := []*UpdateHandler{&handler1, &handler2, &handler3}

	count := len(handlers)
	for i := 0; i < count; i++ {
		handlers = removeHandler(handlers[len(handlers)-1], handlers)
	}

	if len(handlers) > 0 {
		t.Error("Handlers array should have no handlers left after removal.  Found", count)
		return
	}
}
