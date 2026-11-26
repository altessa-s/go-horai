# task

```go
import "github.com/altessa-s/go-horai/task"
```

Defines the task registration contract.

`Registrar.Register` adopts persisted tasks on a new scheduler instance. Repeating an ID on the same instance returns an error matching
`horai.ErrTaskAlreadyRegistered` via `errors.Is` and preserves the existing registration.
