// AGPL-3.0-or-later

// Package forgeagent wires smeldr-agent into a Smeldr application.
// It exposes [AgentJob] as a Smeldr content type, which gives it full
// lifecycle management (Draft → Published → Archived) and auto-generated
// MCP tools (create_agent_job, get_agent_job, list_agent_jobs,
// update_agent_job, publish_agent_job, archive_agent_job, delete_agent_job).
//
// Usage:
//
//	db := smeldr.OpenDB("smeldr.db")
//	forgeagent.CreateTable(db)
//
//	agentMod := forgeagent.New(db, forgeagent.Config{
//	    MCPURL:   "http://localhost:8080/mcp",
//	    MCPToken: os.Getenv("SMELDR_TOKEN"),
//	})
//	agentMod.Register(app)
//	defer agentMod.Stop()
package forgeagent
