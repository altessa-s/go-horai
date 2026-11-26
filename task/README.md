# task

```go
import "github.com/altessa-s/go-horai/task"
```

Defines the task registration contract.

`Registrar.Register` creates a task with a unique ID. A duplicate returns an error matching `horai.ErrTaskAlreadyRegistered` via `errors.Is` and preserves the existing task.
