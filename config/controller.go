package config

type Handler struct {
	Type      HandlerType
	Category  string
	Instances []Instance
}

func NewController(as HandlerType, cat string) *Handler {
	control := &Handler{
		Type:     as,
		Category: cat,
	}

	return control
}
