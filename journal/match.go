package journal

import "fmt"

type Command struct {
	Type Type
	Name string
}

func MatchCommand(recorded Event, cmd Command) error {
	if recorded.Type != cmd.Type || recorded.Name != cmd.Name {
		return fmt.Errorf("%w: recorded=%s/%s got=%s/%s",
			ErrDeterminismViolation, recorded.Type, recorded.Name, cmd.Type, cmd.Name)
	}
	return nil
}
