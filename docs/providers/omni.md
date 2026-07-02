# Omni (Omnigent)

This guide is for people who want to use an [Omnigent](https://omnigent.com) agent as a provider in
T3 Code.

Omni is different from the other providers: instead of running a local CLI, T3 Code talks to a
running Omni **server** over HTTP, and turns execute on a **host** that Omni brings online. You need
all three pieces up before the provider works:

```text
omni server   the HTTP/SSE API T3 Code connects to (default http://127.0.0.1:6767)
omni host     brings your machine online as a host so turns have somewhere to run
an agent      the Omni agent the session runs (e.g. polly, debby)
```

## Quick Start

1. Start the server:

   ```bash
   omni server start
   ```

2. Bring your machine online as a host and note the host ID it prints:

   ```bash
   omni host ""
   ```

3. In T3 Code Settings, configure the Omni provider:

   ```text
   Display name: omnigent
   Host ID:      host_...   (from step 2)
   ```

4. Pick an Omni agent in the model picker and start a thread.

## Which Agent Should I Pick? (Important)

**Pick an API agent, not a terminal-UI agent.** This is the single most common cause of an Omni
thread that "answers once and then stops responding."

Omni exposes two very different kinds of agent through its API:

| Kind | `harness` | Examples | Use in T3 Code |
| --- | --- | --- | --- |
| **API agents** | `claude-sdk` (and similar) | `polly`, `debby` | ✅ Supported. Multi-turn works. |
| **Terminal-UI wrappers** | `*-native` | `claude-native-ui`, `cursor-native-ui`, `codex-native-ui`, `pi-native-ui` | ⚠️ Not supported. See below. |

### Why terminal-UI agents don't work here

The `*-native-ui` agents are wrappers around Omni's own terminal UI (Claude Code, Cursor, Codex,
etc. running in a shell on the host). They speak a different event protocol than the plain
Responses-style streaming T3 Code consumes:

- They emit their turn-completion signals (`session.status: idle` and `response.completed`) **about
  a second in, before any content is produced**.
- The real assistant reply arrives ~10 seconds later, streamed under a **different, nested response
  id**.

T3 Code's Omni adapter treats the first completion signal as the end of the turn. With a terminal-UI
agent that signal fires before the answer exists, so the turn looks like it finished instantly and
the real reply arrives detached from the turn. The visible result is a thread that appears to "stop
after the first message."

API agents (`polly`, `debby`, and other `claude-sdk` agents) stream content **before** their
completion signal, which is what the adapter expects, so multi-turn conversations work normally.

If you need Claude Code / Cursor / Codex specifically, use T3 Code's own Claude, Cursor, or Codex
providers rather than routing them through Omni's terminal-UI wrapper agents.

## Troubleshooting

### "Transport error (GET http://127.0.0.1:6767/health)"

The Omni server isn't running (or is on a different port). Start it with `omni server start` and
confirm:

```bash
curl -s http://127.0.0.1:6767/health   # -> {"status":"ok"}
```

### "Omni host is not configured"

No Host ID is set, or the host went offline. Register/refresh the host and set the printed Host ID in
Settings:

```bash
omni host ""
curl -s http://127.0.0.1:6767/v1/hosts   # host status should be "online"
```

The host daemon has to stay running. If your machine sleeps or the daemon is killed, the host goes
`offline` and turns will fail until you bring it back with `omni host ""`.

### The thread answers once, then stops

You almost certainly picked a `*-native-ui` agent. Switch to an API agent (`polly`, `debby`). See
[Which Agent Should I Pick?](#which-agent-should-i-pick-important) above.

### No agents in the picker

Confirm the server lists them:

```bash
curl -s http://127.0.0.1:6767/v1/agents   # note: /v1/agents, not /api/agents
```
