package util

import (
	"errors"
	"fmt"

	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
	fdm "github.com/storacha/go-ucanto/core/result/failure/datamodel"
)

// BindFailure binds the IPLD node to a FailureModel if possible. This works
// around IPLD requiring data to match the schema _exactly_.
func BindFailure(n ipld.Node) (fdm.FailureModel, error) {
	if n.Kind() != datamodel.Kind_Map {
		return fdm.FailureModel{}, errors.New("unexpected error kind: not a map")
	}

	f := fdm.FailureModel{}
	nn, err := n.LookupByString("name")
	if err == nil {
		name, err := nn.AsString()
		if err != nil {
			return fdm.FailureModel{}, fmt.Errorf("decoding error name as string: %w", err)
		}
		f.Name = &name
	}

	mn, err := n.LookupByString("message")
	if err != nil {
		return fdm.FailureModel{}, fmt.Errorf("looking up message field: %w", err)
	}
	msg, err := mn.AsString()
	if err != nil {
		return fdm.FailureModel{}, fmt.Errorf("decoding error message as string: %w", err)
	}
	f.Message = msg

	sn, err := n.LookupByString("stack")
	if err == nil {
		stack, err := sn.AsString()
		if err != nil {
			return fdm.FailureModel{}, fmt.Errorf("decoding error stack trace as string: %w", err)
		}
		f.Stack = &stack
	}

	return f, nil
}
