package picker

import (
	"sync"

	"github.com/smallnest/weighted"
)

type (
	Choice struct {
		Item   interface{}
		Weight int
	}

	Picker interface {
		Next() interface{}
	}

	simplePicker struct {
		Item interface{}
	}

	weightedPicker struct {
		mu     *sync.Mutex
		picker *weighted.SW
	}
)

func New(choices []*Choice) Picker {
	if len(choices) == 0 {
		return &simplePicker{Item: nil}
	}

	if len(choices) == 1 {
		return &simplePicker{Item: choices[0].Item}
	}

	picker := &weighted.SW{}
	for _, choice := range choices {
		picker.Add(choice.Item, choice.Weight)
	}
	return &weightedPicker{
		mu:     new(sync.Mutex),
		picker: picker,
	}
}

func (p *simplePicker) Next() interface{} {
	return p.Item
}

func (p *weightedPicker) Next() interface{} {
	p.mu.Lock()
	choice := p.picker.Next()
	p.mu.Unlock()
	return choice
}
