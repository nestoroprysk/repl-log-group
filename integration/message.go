package integration

type Message struct {
	Message      string          `json:"message"`
	WriteConcern int             `json:"w"`
	Secondary1   SecondaryConfig `json:"secondary-1"`
	Secondary2   SecondaryConfig `json:"secondary-2"`
	// ID is only used for testing behavior of secondaries by direct POSTs to them (which is not allowed to users).
	ID int `json:"id"`
}

type SecondaryConfig struct {
	// Delay in seconds.
	Delay uint32 `json:"delay"`
	// NoReply forces a node to do nothing and return nothing.
	NoReply bool `json:"noreply"`
}
