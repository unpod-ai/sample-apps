# Autonomous agent samples

Sample apps for [Flows & Journeys](https://docs.unpod.ai/core-engine/flows-journeys)
and the [Agent Workforce](https://docs.unpod.ai/core-engine/agent-workforce).

- A **flow** is a plan of steps: call, message, wait, branch, human approval, exit.
- A **journey** is one contact's run through one flow. Journeys pause between
  steps, branch on each step's outcome, and react to signals such as a reply.

SDK: build flows with `unpod.flows` (`Flow`, `AgentStep`, `MessageStep`), then
manage them with `client.flows` and `client.journeys`. A space's agent runs at an
approval level: `autonomous`, `human_review` (the default), `shadow`, or `blocked`.

The docs mark the journey engine as in build. Call steps can already be dispatched
as tasks.

## Planned apps

| App | What it will show | Docs |
|---|---|---|
| Make-call task | Dispatch one outbound call task to a voice agent | [Create a Task](https://docs.unpod.ai/api/execution/create-task) |
| Track runs | List runs in a space and read each task's outcome | [Runs API](https://docs.unpod.ai/api/execution/runs-overview) |
| Follow-up flow | Call, wait, then send a message if there was no answer | [Flows & Journeys](https://docs.unpod.ai/core-engine/flows-journeys) |
| Start and steer a journey | Start a journey for a contact, inspect it, then pause or stop it | [Flows & Journeys](https://docs.unpod.ai/core-engine/flows-journeys) |
| Human approval | Run in `human_review` and approve steps before they send | [Teams & Policies](https://docs.unpod.ai/core-engine/teams-policies) |
