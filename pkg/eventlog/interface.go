package eventlog

import "iter"

type Iterable[T any] interface {
	Iterator() iter.Seq2[T, error]
}

type Appender[T any] interface {
	Append(item T) error
}
