# Voice agent samples

Sample apps for the [Speech Stack](https://docs.unpod.ai/speech-stack/introduction):
agents that talk on phone and browser calls. Unpod handles speech-to-text,
text-to-speech, turn-taking, and transport. Your agent receives text and returns
text.

An agent has one `agent_id`, one brain, and one or more voices. The brain is a
`Prompt`, a `Playbook`, an `Endpoint` you host, or your own `Runner`
(`client.agents.voice.create(..., brain=...)`).

## Planned apps

| App | What it will show | Docs |
|---|---|---|
| Prompt agent | Create an agent from a prompt, attach a number, take a call | [Quickstart](https://docs.unpod.ai/get-started/quickstart) |
| Your own brain | Run an `AgentRunner` that answers each turn with your code | [Run Your Own Brain](https://docs.unpod.ai/get-started/first-phone-call), [AgentRunner & Sessions](https://docs.unpod.ai/speech-stack/agent-runner) |
| Framework adapters | Plug in an OpenAI, Anthropic, LangChain, MCP, or HTTP brain | [Adapters Reference](https://docs.unpod.ai/speech-stack/adapters) |
| Playbook agent | Run a SuperDialog playbook as the brain | [Run a SuperDialog agent](https://docs.unpod.ai/speech-stack/level-up-superdialog) |
| Outbound calls | Have an agent call a list of numbers | [Outbound Calls](https://docs.unpod.ai/speech-stack/outbound-calls) |
| Handover and voicemail | Transfer to a human, leave voicemail, schedule follow-ups | [Handover, Voicemail & Follow-up](https://docs.unpod.ai/speech-stack/agent-tools) |
| Hooks and events | React to call lifecycle events | [Hooks & Events](https://docs.unpod.ai/speech-stack/hooks-events) |
| Recordings and transcripts | Fetch a finished call's recording and transcript | [Recordings & Transcripts](https://docs.unpod.ai/speech-stack/recordings-transcripts) |
