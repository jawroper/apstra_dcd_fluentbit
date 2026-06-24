package restapi

// InjectSystem inserts a System directly into the cache.
// Used only in tests to avoid needing a live DCD server.
func (c *Client) InjectSystem(key string, sys *System) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.systems == nil {
		c.systems = make(map[string]*System)
	}
	c.systems[key] = sys
}

// InjectBlueprint inserts a Blueprint directly into the cache.
// Used only in tests to avoid needing a live DCD server.
func (c *Client) InjectBlueprint(id string, bp *Blueprint) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.blueprints == nil {
		c.blueprints = make(map[string]*Blueprint)
	}
	c.blueprints[id] = bp
}
