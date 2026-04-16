# jsoncurrent

Python Emitter for the jsoncurrent patch protocol — stream structured JSON from your LLM backend incrementally.

```
LLM tokens → [Emitter] → patch stream → [Collector] → assembled object
```

**JS/TS client and Node Emitter:** [https://github.com/richardantao/jsoncurrent-js](https://github.com/richardantao/jsoncurrent-js)

## Installation

```bash
pip install jsoncurrent
```

---

## The problem

LLMs generate JSON token by token. But if you try to parse incomplete JSON mid-stream, standard parsers throw.

jsoncurrent solves this with a patch protocol. The Emitter on your Python server parses raw tokens as they arrive and emits structured patch operations over SSE, WebSocket, or any transport you choose. The JS Collector on your client reconstructs the object incrementally.

```
// What the LLM emits (incomplete, unparseable mid-stream):
{"title": "Quarterly Report", "sections": [{"heading": "Exec

// What jsoncurrent delivers to your client as it arrives:
{ path: 'title',               value: 'Quarterly Report', op: 'add'    }
{ path: 'sections',            value: [],                 op: 'add'    }
{ path: 'sections[0]',         value: {},                 op: 'add'    }
{ path: 'sections[0].heading', value: 'Exec',             op: 'add'    }
{ path: 'sections[0].heading', value: 'utive Summary',    op: 'append' }
```

---

## Why a Python Emitter?

If your backend is Python — FastAPI, Flask, Django — there is no client-side option for structured JSON streaming. jsoncurrent is the only way to produce a consumable patch stream from a Python LLM backend.

Beyond the language boundary, the Emitter's middleware chain lets you intercept every patch before it hits the wire:

- Resolve `{{img:chart}}` placeholders to presigned S3 URLs
- Strip fields a given user has no permission to see
- Normalise inconsistent date formats from the model
- Inject values from databases or caches

---

## The wire format

Four operations. This is the entire protocol — identical across Python and JS implementations.

| `op`       | Meaning                                      | Example                                                    |
|------------|----------------------------------------------|------------------------------------------------------------|
| `add`      | Initialise or replace a value at a path      | `{ path: 'title', value: 'Hello', op: 'add' }`            |
| `append`   | Concatenate a string delta                   | `{ path: 'title', value: ' World', op: 'append' }`        |
| `insert`   | Push a new element onto an array             | `{ path: 'tags', value: 'news', op: 'insert' }`           |
| `complete` | The value at this path is fully assembled    | `{ path: 'title', value: 'Hello World', op: 'complete' }` |

Paths use dot-notation with array indices: `sections[0].heading`.

**Patches are plain JSON-serialisable objects.** How they travel is entirely up to you — SSE, WebSocket, HTTP streaming. The Emitter serialises each patch with `chunk.to_json()`; your client deserialises with `JSON.parse()`.

---

## FastAPI

```python
from fastapi import FastAPI
from fastapi.responses import StreamingResponse
from jsoncurrent import Emitter
import anthropic

app = FastAPI()
client = anthropic.Anthropic()

@app.get('/stream')
async def stream():
    queue = asyncio.Queue()

    emitter = Emitter()
    emitter.on('patch', lambda chunk: queue.put_nowait(f"data: {chunk.to_json()}\n\n"))
    emitter.on('complete', lambda: queue.put_nowait("data: [DONE]\n\n"))

    async def generate():
        with client.messages.stream(
            model="claude-opus-4-6",
            max_tokens=4096,
            messages=[{"role": "user", "content": "Generate a report as JSON..."}],
        ) as stream:
            for text in stream.text_stream:
                emitter.write(text)
        emitter.flush()

        while not queue.empty():
            yield await queue.get()

    return StreamingResponse(generate(), media_type="text/event-stream")
```

---

## Flask

```python
from flask import Flask, Response, stream_with_context
from jsoncurrent import Emitter
import anthropic

app = Flask(__name__)
client = anthropic.Anthropic()

@app.get('/stream')
def stream():
    def generate():
        emitter = Emitter()

        patches = []
        emitter.on('patch', patches.append)

        with client.messages.stream(
            model="claude-opus-4-6",
            max_tokens=4096,
            messages=[{"role": "user", "content": "Generate a report as JSON..."}],
        ) as stream:
            for text in stream.text_stream:
                emitter.write(text)
                for chunk in patches:
                    yield f"data: {chunk.to_json()}\n\n"
                patches.clear()

        emitter.flush()
        yield "data: [DONE]\n\n"

    return Response(stream_with_context(generate()), mimetype="text/event-stream")
```

---

## Middleware

```python
from jsoncurrent import Emitter

emitter = Emitter()

def resolve_images(patch, next_fn):
    if patch.op == 'add' and isinstance(patch.value, str):
        if patch.value.startswith('{{img:'):
            filename = patch.value[6:-2]
            patch = patch.replace(value=get_presigned_url(filename))
    next_fn(patch)

def strip_internal(patch, next_fn):
    if 'internal' not in patch.path:
        next_fn(patch)

emitter.use(resolve_images)
emitter.use(strip_internal)
```

Middleware runs in registration order. Call `next_fn(patch)` to pass through, call it multiple times to fan out, or return without calling it to drop the patch. Receives all four ops including `complete`.

---

## API reference

### Emitter

```python
from jsoncurrent import Emitter

emitter = Emitter(
    root="",          # namespace prefix for all emitted paths
    completions=True  # emit complete patches — set False to suppress lifecycle signals
)

emitter.write(token: str)           # feed a raw LLM token
emitter.flush()                     # end of stream — flushes, emits 'complete', resets
emitter.reset()                     # reset without emitting 'complete'
emitter.use(fn: MiddlewareFn)       # register middleware — chainable
emitter.on(event: str, fn)          # register event listener
emitter.off(event: str, fn)         # remove event listener
```

**Events:**
- `patch` — fires for each `StreamingChunk`; serialise with `chunk.to_json()`
- `complete` — fires when `flush()` is called
- `error` — fires on parse errors

### StreamingChunk

```python
from jsoncurrent.types import StreamingChunk

chunk.path   # str  — dot-notation path e.g. 'sections[0].heading'
chunk.value  # Any  — patch payload; assembled snapshot for 'complete' patches
chunk.op     # str  — 'add' | 'append' | 'insert' | 'complete'

chunk.to_json()              # serialize to wire format JSON string
chunk.replace(value=x)       # return new chunk with field replaced
StreamingChunk.from_json(s)  # deserialize from wire format JSON string
```

---

## jsoncurrent-js

The JS/TS package — Collector, Node Emitter, and React hook. Patches from jsoncurrent-py are consumed by the JS Collector without any changes on the client side.

[jsoncurrent-js](https://github.com/richardantao/jsoncurrent-js)

---

## See also

- [jsonriver](https://github.com/rictic/jsonriver) — client-side incremental JSON parsing for pure JS stacks where the server forwards the raw LLM stream unchanged and no server-side transformation is needed
- [Anthropic streaming docs](https://docs.anthropic.com/en/api/messages-streaming)
- [OpenAI streaming docs](https://platform.openai.com/docs/api-reference/streaming)

---

## Contributing

For development and contribution guidelines, see [CONTRIBUTING.md](./CONTRIBUTING.md).

---

## License

MIT