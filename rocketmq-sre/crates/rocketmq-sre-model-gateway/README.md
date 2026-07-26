# rocketmq-sre-model-gateway

[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](../../../LICENSE-APACHE)

`rocketmq-sre-model-gateway` defines a provider-neutral Canonical Model IR and
the Phase 00 provider capability fixtures used for planning later AI
integration.

## Responsibilities

- Model messages, tools, tool calls, response formats, finish reasons, and
  canonical requests and responses.
- Describe OpenAI-compatible, Anthropic, Gemini, Amazon Bedrock, DeepSeek,
  Zhipu GLM, Kimi/Moonshot, and local-model protocol capabilities.
- Keep provider selection based on descriptors rather than provider-specific
  types in SRE domain code.

## Phase 00 boundary

The crate contains descriptors and fixtures only. It has no provider SDK,
HTTP client, API credential, retry loop, streaming transport, or outbound model
call, and no evidence is sent to an external model.

## Validation

Run from `rocketmq-sre/`:

```powershell
cargo test --locked -p rocketmq-sre-model-gateway
```

The tests verify canonical IR serialization and the expected provider
descriptor set, including DeepSeek, Zhipu GLM, and Kimi/Moonshot.
