package integration

type Message struct {
	Message string `json:"message"`
	// Delay in seconds.
	Delay uint32 `json:"delay"`
}
