// Package agent is a minimal Go agent runtime with native MCP support, for
// building small autonomous agent binaries (see cmd/agent-smeldr and
// cmd/agent-github in this repo for working examples).
//
// # Quick start
//
//	a := agent.New(agent.Config{
//	    MCPURL:       "http://localhost:8080/mcp",
//	    MCPToken:     "bearer-token",
//	    SystemPrompt: "You are a helpful assistant.",
//	})
//	result, err := a.Run(ctx, "List all published posts and summarize the site.")
//
// [Config] / [Agent] / [New] / [Agent.Run] drive one Anthropic-backed
// tool-use loop against an MCP server. [Config.StreamableHTTP] selects the
// transport: false (default) uses SSE, matching smeldr.dev/mcp; true uses
// Streamable HTTP, matching GitHub MCP and other 2025-03-26+ servers.
//
// Two built-in tools, named "http_get" and "http_post", are always available
// to the model alongside any MCP tools, for calling out to plain HTTP
// endpoints (webhooks, notification services) that have no MCP server of
// their own.
//
// # Scheduling
//
// [Job] / [Scheduler] / [NewScheduler] run agent tasks on cron schedules.
// [NewSweepScheduler] and [NewEvalQueueScheduler] are narrower helpers for
// driving smeldr.dev/core's own App.SweepStructural and App.DrainEvalQueue
// on a schedule.
//
// # Smeldr integration
//
// The smeldr.dev/agent/flow subpackage (AGPL) wires [Agent] as a Smeldr
// content type ([AgentJob]) with its own MCP tools and lifecycle — see its
// own package doc for details.
package agent
