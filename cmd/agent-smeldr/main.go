// agent-smeldr connects to a smeldr-mcp server and summarises published posts.
//
// Required environment variables:
//
//	ANTHROPIC_API_KEY  Anthropic API key
//	SMELDR_MCP_URL     smeldr-mcp SSE endpoint (e.g. https://example.com/mcp)
//	SMELDR_TOKEN       smeldr-mcp Bearer token
package main

import (
	"context"
	"fmt"
	"os"

	"smeldr.dev/agent"
)

func main() {
	cfg := agent.Config{
		MCPURL:   os.Getenv("SMELDR_MCP_URL"),
		MCPToken: os.Getenv("SMELDR_TOKEN"),
		SystemPrompt: "You are a helpful assistant with access to a Smeldr server. " +
			"Use the available MCP tools to answer questions about the site.",
	}

	a := agent.New(cfg)
	result, err := a.Run(context.Background(),
		"List all published posts and summarize the site in two sentences.")
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	fmt.Println(result)
}
