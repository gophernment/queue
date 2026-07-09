# queue

A tiny Go package for fanning a fixed-size collection out to a pool of
concurrent workers, with `context` cancellation support.

## Install

```sh
go get github.com/gophernment/queue
```

## Concepts

- **`Simpler`** — wrap any collection of yours behind two methods:

  ```go
  type Simpler interface {
      Len() int
      Pop(i int) interface{}
  }
  ```

- **`Worker`** — your business logic, called once per item:

  ```go
  type Worker interface {
      Do(v interface{}) interface{}
  }
  ```

  `Manager` may call `Do` concurrently from multiple goroutines pulling off
  the same queue, so a `Worker` implementation must be safe for concurrent
  use (e.g. use `sync/atomic` or a mutex around any shared state).

- **`Queue`** — turns a `Simpler` into a channel of items (`Pop()`), closing
  and notifying (`Empty()`) once exhausted.

- **`Manager`** — runs a `Worker` over a `Queue`. Launch as many `Do()`
  goroutines as you want workers; they share the same underlying queue.
  Cancelling the given `context.Context` stops remaining items from being
  processed.

## Usage

```go
package main

import (
	"context"
	"fmt"

	"github.com/gophernment/queue"
)

type items []string

func (i items) Len() int                { return len(i) }
func (i items) Pop(idx int) interface{} { return i[idx] }

type printer struct{}

func (printer) Do(v interface{}) interface{} {
	fmt.Println(v)
	return nil
}

func main() {
	s := items{"a", "b", "c"}
	m := queue.NewManager(context.Background(), printer{}, s)

	// spawn as many concurrent workers as you like
	go m.Do()
	go m.Do()

	<-m.End()

	for res := range m.Response() {
		fmt.Println(res)
	}
}
```

### Cancellation

```go
ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
defer cancel()

m := queue.NewManager(ctx, worker, items)
go m.Do()
go m.Do()

<-m.End() // returns once every item has been either processed or dropped
```

Once `ctx` is done, `Manager` stops calling `Worker.Do` for remaining items;
`End()` and `Response()` still resolve/close deterministically.

## Testing

```sh
go test -race ./...
```

## License

[MIT](LICENSE)
