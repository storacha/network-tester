package eventlog

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"iter"
	"maps"
	"slices"
	"strconv"
)

type CSVWriter[T any] struct {
	writer *csv.Writer
	first  bool
}

func (cw *CSVWriter[T]) Append(item T) error {
	jsonData, err := json.Marshal(item)
	if err != nil {
		return fmt.Errorf("marshalling JSON: %w", err)
	}
	data := map[string]any{}
	err = json.Unmarshal(jsonData, &data)
	if err != nil {
		return fmt.Errorf("unmarshalling JSON: %w", err)
	}
	keys := slices.Collect(maps.Keys(data))
	slices.Sort(keys)

	if cw.first {
		err := cw.writer.Write(keys)
		if err != nil {
			return err
		}
		cw.first = false
	}

	values := make([]string, 0, len(keys))
	for _, k := range keys {
		if num, ok := data[k].(float64); ok {
			values = append(values, fmt.Sprintf("%d", int(num)))
		} else {
			values = append(values, fmt.Sprintf("%s", data[k]))
		}
	}

	return cw.writer.Write(values)
}

func (cw *CSVWriter[T]) Flush() error {
	cw.writer.Flush()
	return cw.writer.Error()
}

func NewCSVWriter[T any](dest io.Writer) *CSVWriter[T] {
	return &CSVWriter[T]{writer: csv.NewWriter(dest), first: true}
}

type CSVReader[T any] struct {
	reader *csv.Reader
}

func (cr *CSVReader[T]) Iterator() iter.Seq2[T, error] {
	var emptyItem T
	return func(yield func(T, error) bool) {
		var fields []string
		first := true
		for {
			record, err := cr.reader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				yield(emptyItem, err)
				return
			}

			if first {
				fields = record
				first = false
				continue
			}

			data := map[string]any{}
			isRepeatedFields := true
			for i, k := range fields {
				if k != record[i] {
					isRepeatedFields = false
				}
				if num, err := strconv.Atoi(record[i]); err == nil {
					data[k] = num
				} else {
					data[k] = record[i]
				}
			}
			if isRepeatedFields {
				continue
			}
			jsonData, err := json.Marshal(data)
			if err != nil {
				yield(emptyItem, fmt.Errorf("marshalling JSON: %w", err))
				return
			}
			var item T
			err = json.Unmarshal(jsonData, &item)
			if err != nil {
				yield(emptyItem, fmt.Errorf("unmarshalling JSON: %w", err))
				return
			}
			if !yield(item, nil) {
				return
			}
		}
	}
}

func NewCSVReader[T any](src io.Reader) *CSVReader[T] {
	return &CSVReader[T]{csv.NewReader(src)}
}
