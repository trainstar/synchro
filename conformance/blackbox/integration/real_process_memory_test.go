package integration

import (
	"context"
	"fmt"
	"math"
	"os/exec"
	"strconv"
	"strings"
)

func readRealProcessRSSBytes(ctx context.Context, pid int) (int64, error) {
	if ctx == nil || pid <= 0 {
		return 0, fmt.Errorf("RSS process observation is invalid")
	}
	output, err := exec.CommandContext(ctx, "ps", "-o", "rss=", "-p", strconv.Itoa(pid)).Output()
	if err != nil {
		return 0, fmt.Errorf("read process RSS: %w", err)
	}
	fields := strings.Fields(string(output))
	if len(fields) != 1 {
		return 0, fmt.Errorf("process RSS output is invalid")
	}
	kibibytes, err := strconv.ParseInt(fields[0], 10, 64)
	if err != nil || kibibytes <= 0 || kibibytes > math.MaxInt64/1024 {
		return 0, fmt.Errorf("process RSS value is invalid")
	}
	return kibibytes * 1024, nil
}
