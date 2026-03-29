# Meibel Python SDK

The official Python SDK for the [Meibel API](https://docs.meibel.ai). Provides document parsing, datasource management, and AI agent orchestration.

## Installation

```bash
pip install meibel==0.1.0b1
```

## Quick Start

```python
from meibel import MeibelClient

client = MeibelClient(
    api_key="your-api-key",
    base_url="https://api.meibel.ai/v2",
)

# Parse a document
with open("document.pdf", "rb") as f:
    result = client.documents.parse_document(file=f)
    print(result.job_id)

# Process a document synchronously (waits for completion)
with open("document.pdf", "rb") as f:
    result = client.documents.process_document(file=f)
    print(result)

# List datasources
datasources = client.datasources.list_datasources()
for ds in datasources.items:
    print(ds.name)
```

## Async Usage

```python
from meibel import AsyncMeibelClient

client = AsyncMeibelClient(
    api_key="your-api-key",
    base_url="https://api.meibel.ai/v2",
)

result = await client.documents.process_document(file=open("doc.pdf", "rb"))
```

## Documentation

- [API Reference](https://docs.meibel.ai/api-reference/overview)
- [SDK Guide](https://docs.meibel.ai/sdk/python)

## License

MIT
