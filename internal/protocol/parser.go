package protocol

import (
    "strings"
)

type Command struct {
    Name string
    Args []string
}

func ParseCommand(line string) (*Command, error) {
    line = strings.TrimSpace(line)
    if line == "" {
        return nil, nil
    }
    parts := strings.SplitN(line, " ", 3)
    if len(parts) == 0 {
        return nil, nil
    }
    cmd := &Command{Name: strings.ToUpper(parts[0])}
    if len(parts) > 1 {
        cmd.Args = append(cmd.Args, parts[1])
    }
    if len(parts) > 2 {
        cmd.Args = append(cmd.Args, parts[2])
    }
    return cmd, nil
}