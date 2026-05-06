# Meibel Python SDK

The official Python SDK for the [Meibel API](https://docs.meibel.ai). Provides document parsing, datasource management, and AI agent orchestration.

## Installation

Install from Git (v2):

```bash
pip install git+https://github.com/meibel-ai/meibel-python.git@v2.0.0
```

## Quick Start

```python
from meibel import MeibelClient

client = MeibelClient(api_key="your-api-key")

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

## Nested Resources

Resources are organized hierarchically. Content, downloads, data elements, and table descriptions are accessed through `datasources`:

```python
# Upload content to a datasource
with open("data.csv", "rb") as f:
    result = client.datasources.content.upload_content(file=f, file_name="data.csv")

# List data elements
elements = client.datasources.data_elements.list_data_elements(datasource_id="ds-123")
```

Agent sessions (chat) are accessed through `agents`:

```python
# Create a session and send a message
session = client.agents.sessions.create_session(agent_id="agent-123")
response = client.agents.sessions.send_chat_message(session_id=session.id, message="Hello")
```

## Async Usage

```python
from meibel import AsyncMeibelClient

client = AsyncMeibelClient(api_key="your-api-key")

result = await client.documents.process_document(file=open("doc.pdf", "rb"))
```

## Documentation

- [API Reference](https://docs.meibel.ai/api-reference/overview)
- [SDK Guide](https://docs.meibel.ai/sdk/python)

## License

MIT