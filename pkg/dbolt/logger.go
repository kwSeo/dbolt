package dbolt

import (
	"github.com/go-kit/log"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

var _ log.Logger = (*ZapGoKitLogger)(nil)

type ZapGoKitLogger struct {
	logger *zap.Logger
}

func (z *ZapGoKitLogger) Log(keyvals ...any) error {
	msg, others, err := SplitMsgAndOthers(keyvals)
	if err != nil {
		return err
	}
	z.logger.Info(msg, others...)
	return nil
}

func SplitMsgAndOthers(keyvals []any) (string, []zap.Field, error) {
	var msg string
	var others []zap.Field

	end := len(keyvals) - 1
	for i := 0; i < end; i += 2 {
		keyval, ok := keyvals[i].(string)
		if !ok {
			return "", nil, errors.New("key of log must be string type")
		} else if keyval == "msg" {
			msg, ok = keyvals[i+1].(string)
			if !ok {
				return "", nil, errors.New("log message must be string type")
			}
		} else {
			others = append(others, zap.Any(keyval, keyvals[i+1]))
		}
	}

	return msg, others, nil
}
