package picker

import (
	"sync"

	"github.com/smallnest/weighted"
)

type (
	Choice struct {
		Item   any
		Weight int
	}

	Picker interface {
		Next() any
	}

	simplePicker struct {
		Item any
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

func (p *simplePicker) Next() any {
	return p.Item
}

func (p *weightedPicker) Next() any {
	p.mu.Lock()
	choice := p.picker.Next()
	p.mu.Unlock()
	return choice
}
