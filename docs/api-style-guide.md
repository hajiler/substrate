# Substrate gRPC API Style Guide

This document is the authoritative style guide for Substrate public APIs. Today this only includes `ate-apiserver` API.

This guide is derived from [Google's AIP (API Improvement Proposals)](https://google.aip.dev/) and adopts the large majority of its conventions. Divergences are called out explicitly with rationale.

---

## 1. Resource-Oriented Design

*Follows [AIP-121](https://google.aip.dev/121).*

APIs are structured around **resources** (nouns) and a small set of standard **methods** (verbs). Standard methods are Get, List, Create, Update, and Delete. **Custom methods** handle operations that don't fit these patterns (e.g., Suspend, Resume, Pause for Actors).

Rules:
- Every primary noun the API exposes is a resource.
- Standard methods are strongly preferred. Custom methods are the exception, not the norm.
- The resource schema must be identical across all standard methods that reference it (i.e., Get, Create, and Update all return the same `Actor` message).

---

## 2. Resource Naming and Identity

**Diverges from [AIP-122](https://google.aip.dev/122).**

AIP-122 identifies resources by a single opaque path string (e.g., `publishers/123/books/les-miserables`). Substrate uses a **two-field identity** instead: an `atespace` (namespace) and a `name`. This is analogous to how Kubernetes identifies objects and avoids the ambiguity of parsing hierarchical path strings.

Resources are either **atespace-scoped** or **global-scoped**. Scope is a fixed property of the resource type, not of individual instances. For example, `Actor` resources
are **atespace-scoped**, whereas `Atespace` resources are naturally **global-scoped**.

### 2.1 Atespace-scoped resources

* Atespace-scoped resources belong to an atespace. Their identity is `(atespace, name)`, unique within the resource type.
* Global-scoped resources are global across the entire deployment and do not belong to any atespace. For these, the identity is `name` alone.

In both cases, a `metadata` field contains both `atespace` and `name`. For global resources, the `atespace` must always be empty.

```proto
message Actor {
  // The atespace this actor belongs to.
  ResourceMetadata metadata = 1;

  // ... other fields
}


message ResourceMetadata {
  // The atespace this resource belongs to. Empty if the resource has global-scope.
  string atespace = 1;
  // The name of this resource, unique within its atespace.
  string name = 2;

  // ... other common resource fields
}
```

- All resources in Substrate must have a `ResourceMetadata metadata = 1` field to hold common fields, which includes both `atespace` and `name`.
- If the resource type has global-scope, the `atespace` field must be always empty.

### 2.3 Character constraints

Both `atespace` and `name` must be valid resource names.
A valid resource name must comply with the following rules:

- Lowercase alphanumeric characters and hyphens only.
- Must start with a lowercase alphanumeric character.
- Must end with a lowercase alphanumeric character.
- Maximum 63 characters.
- Regex: `^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$`

Resource names are valid RFC-1123 DNS labels.

### 2.4 `ObjectRef` — reference type

The `ObjectRef` message represents a *pointer* to a Substrate resource.

```proto
message ObjectRef {
  string atespace = 1;
  string name     = 2;
}
```

Use `ObjectRef` in places where you need to reference a resource. For example:

**1. Request messages** — to identify which specific resource to act on:

```proto
message GetActorRequest {
  ObjectRef actor = 1;
}

message DeleteActorRequest {
  ObjectRef actor   = 1;

  // ... other fields
}
```

**2. Cross-resource references** — when one resource's fields refer to another atespace-scoped resource:

```proto
message Actor {
  string atespace = 1;
  string name     = 2;

  // The ActorTemplate this actor was derived from.
  ObjectRef actor_template = 3;

  // ... other fields
}
```

The field name is the logical name of the reference (e.g., `actor_template`), not `actor_template_name` or `actor_template_ref`.
Note that this assumes that ActorTemplates are also resources in the substrate gRPC API (not in KRM).

- Do not embed the full resource message as a reference field.
- Do not use a single combined string like `"atespace/name"`. Callers would have to parse it.
- Do not use a plain `string {resource}_name` field for global-scoped references — use `ObjectRef` for consistency and type safety.

---

## 3. Standard Methods

The following sections cover each standard method. The primary adaptation from AIP-13x is how resources are identified in requests: an `ObjectRef` field instead of a `name` path string.

### 3.1 Get

*Follows [AIP-131](https://google.aip.dev/131).*

```proto
rpc GetActor(GetActorRequest) returns (Actor) {}

message GetActorRequest {
  ObjectRef actor = 1;
}
```

Rules:
- RPC name **must** begin with `Get` followed by the singular resource name.
- Request message name **must** match the RPC name with a `Request` suffix.
- Response **must** be the resource itself — not a `GetActorResponse` wrapper.
- Request **must** identify the resource with a single `ObjectRef` field (for both atespace-scoped and global-scoped resources).
- If the resource does not exist: return `NOT_FOUND`.

### 3.2 List

*Follows [AIP-132](https://google.aip.dev/132).*

```proto
rpc ListActors(ListActorsRequest) returns (ListActorsResponse) {}

message ListActorsRequest {
  // The atespace to list actors from.
  string atespace = 1;

  // Maximum number of actors to return. The server may return fewer.
  // If unspecified, defaults to a server-chosen value.
  // The maximum value is 1000; values above 1000 are coerced to 1000.
  int32 page_size = 2;

  // Pagination token from a previous ListActors response.
  // Omit or leave empty for the first request.
  string page_token = 3;
}

message ListActorsResponse {
  repeated Actor actors = 1;

  // Pagination token for the next page.
  // Empty if this is the last page.
  string next_page_token = 2;
}
```

Rules:
- RPC name **must** begin with `List` followed by the **plural** resource name.
- Both the request and response message names **must** match the RPC name with `Request`/`Response` suffixes. (Unlike Get/Create/Update, List responses are not the resource itself.)
- `next_page_token` **must** be present on every List response message. It **must** be empty when there are no further pages.
- The repeated resource field **must** use the plural form of the resource name (e.g., `actors`, not `actor`).
- If a user provides a `page_size` above the maximum, coerce it silently. If a user provides a negative value, return `INVALID_ARGUMENT`.
- Sorting and filtering as specified in AIP-132 are not supported.
- Clients must iterate over all pages until an empty `next_page_token` is returned. Clients should not assume that an empty result (i.e. len(actors) == 0) means the end of the stream.

### 3.3 Create

*Adapted from [AIP-133](https://google.aip.dev/133).*

```proto
rpc CreateActor(CreateActorRequest) returns (Actor) {}

message CreateActorRequest {
  // The actor to create.
  // actor.metadata.atespace and actor.metadata.name together specify the resource's identity
  // and must both be set by the caller.
  Actor actor = 1;
}
```

Rules:
- RPC name **must** begin with `Create` followed by the singular resource name.
- Response **must** be the resource itself — not a `CreateActorResponse` wrapper.
- `actor.metadata.atespace` and `actor.metadata.name` are **required** and caller-specified. The server does not generate them.
- Other meta fields such as `uid`, timestamps, `version`, etc, are server side generated, and ignored when specified.
- If a resource already exists with the same `(atespace, name)`: return `ALREADY_EXISTS`.
- `actor.metadata.atespace` must be specified iff the resource type is atespace-scoped, otherwise the service must return `INVALID_ARGUMENT`.

**Divergence from AIP-133:** AIP-133 separates `parent` + `{resource}_id` from the resource body because AIP-122 makes the resource `name` field output-only (constructed by the server from the parent path). In Substrate's model, `atespace` and `name` are directly caller-specified identity fields on the resource, so duplicating them at the top level of the request adds no information and creates ambiguity about which one wins. The embedded resource is the single source of truth for identity on create.

### 3.4 Update

*Follows [AIP-134](https://google.aip.dev/134). Diverges on `update_mask` requirement and `*` support.*

Updates use a **partial-update model** (equivalent to HTTP PATCH). The mask is always required.

```proto
rpc UpdateActor(UpdateActorRequest) returns (Actor) {}

message UpdateActorRequest {
  // The actor to update.
  // The actor's atespace and name fields identify which resource to update.
  Actor actor = 1;

  // The set of fields to update. Required.
  //
  // Field paths are relative to the Actor message (e.g., "worker_selector").
  google.protobuf.FieldMask update_mask = 2;
}
```

Rules:
- RPC name **must** begin with `Update` followed by the singular resource name.
- Response **must** be the resource itself — not an `UpdateActorResponse` wrapper.
- `update_mask` **must** be of type `google.protobuf.FieldMask` and **must** be named `update_mask`.
- `update_mask` is **required**. An absent or empty mask **must** return `INVALID_ARGUMENT`.
- If `update_mask` includes an immutable field (e.g. output only), **must** return `INVALID_ARGUMENT`.
- The special value `*` is **not supported**. Clients must enumerate the exact fields to update.
- The resource's `atespace` and `name` identify the resource to update; they are not themselves updatable.
- If the resource does not exist: return `NOT_FOUND`.
- Additional "control" fields can be added to the request message to control the semantics of the operation (e.g. dry-run).

**Divergence from AIP-134:** AIP-134 makes `update_mask` optional (omission implies updating all populated fields) and requires support for `*`. Substrate requires an explicit mask.

### 3.5 Delete

*Follows [AIP-135](https://google.aip.dev/135). Diverges on return type.*

```proto
rpc DeleteActor(DeleteActorRequest) returns (Actor) {}

message DeleteActorRequest {
  ObjectRef actor   = 1;

  // ... other fields
}
```

Rules:
- RPC name **must** begin with `Delete` followed by the singular resource name.
- Response **must** be the deleted resource.
- Request **must** identify the resource with an `ObjectRef` field (for both atespace-scoped and global-scoped resources).
- If the resource does not exist: return `NOT_FOUND`.
- Additional "control" fields can be added to the request message to control the semantics of the operation (e.g. dry-run, or UID precondition check).


TODO: Delete operations are synchronous, but might revisit this and introduce the concept of soft-deletion to 
allow external controllers and upstream systems / automations to react to resource deletion (e.g. via some mechanism like k8s finalizers).

---

## 4. Custom Methods

*Follows [AIP-136](https://google.aip.dev/136).*

Custom methods are for operations that don't map cleanly to CRUD: lifecycle transitions (Suspend, Resume, Pause), long-running actions, or commands with side effects that standard Update semantics would misrepresent.

```proto
rpc SuspendActor(SuspendActorRequest) returns (Actor) {}

message SuspendActorRequest {
  ObjectRef actor = 1;
}
```

Rules:
- RPC name **must** be a verb phrase: `{Verb}{Resource}` (e.g., `SuspendActor`, `ResumeActor`).
- Request message name **must** match the RPC name with a `Request` suffix.
- The request **must** identify the target resource using an `ObjectRef` field (for both atespace-scoped and global-scoped resources).
- Custom methods should return a response message matching the RPC name, with a Response suffix. When operating on a specific resource, a custom method may return the resource itself.

---

## 5. Field Naming

*Follows [AIP-140](https://google.aip.dev/140) and [AIP-149](https://google.aip.dev/149).*

- Field definitions in proto files **must** use `lower_snake_case`.
- Boolean fields **must** omit the `is_` prefix: use `disabled`, not `is_disabled`. Exception: use `is_` when the bare word would be a reserved keyword in common languages.
- Repeated fields **must** use the plural noun form: `containers`, not `container`.
- Non-repeated fields **must** use the singular form: `container`, not `containers`.
- Field names **must** be nouns, not verbs: `worker_selector`, not `select_workers`.
- Use standard abbreviations where well-established: `config`, `spec`, `id`, `info`, `stats`.
- Adjectives come before the noun: `suspended_actors`, not `actors_suspended`.
- Avoid prepositions in field names: `error_reason`, not `reason_for_error`.

### 5.1 Field presence (`optional`)

Use the `optional` keyword on a scalar field **only** when null and the zero value (`false`, `0`, `""`) are semantically distinct for that field's meaning. Do not use `optional` universally or as a workaround for update semantics — the required `update_mask` handles that.

```proto
// Only if "unrated" is meaningfully different from "rated 0":
optional int32 priority = 5;

// No optional needed — false and unset mean the same thing here:
bool cordoned = 6;
```

Because `update_mask` is required, the server always knows which fields the client intends to change. `optional` is reserved for cases where the resource itself has a three-state semantic (set-to-zero, set-to-nonzero, not-set).

---

## 6. Standard Fields

*Follows [AIP-148](https://google.aip.dev/148) and [AIP-142](https://google.aip.dev/142). Diverges in that a shared message contains all standard fields.*

All resources in Substrate must have a `ResourceMetadata metadata = 1` field to hold common fields.

```proto
message ResourceMetadata {
  string atespace = 1;
  string name     = 2;

  // uid is a server-assigned, globally unique identifier for this resource.
  // Immutable throughout the lifecycle of the resource.
  string uid = 3;

  // version is increased on every mutation.
  int64 version = 4;

  // create_time is the time the resource was created.
  google.protobuf.Timestamp create_time = 5;

  // update_time is the time the resource was last updated by a user action.
  google.protobuf.Timestamp update_time = 6;
}
```

### 6.1 `uid`

- Type: `string`.
- A server-assigned [UUID4](https://en.wikipedia.org/wiki/Universally_unique_identifier#Version_4_(random)).
- Useful for correlation across logs, events, and audit trails where the resource `name` may not be available. Also useful
for controllers that need to do bookkeeping and track state associated with a resource.

### 6.2 `version`

- Type: `int64`.
- Monotonically increasing number which increments on every resource update. Allows clients to do opportunistic locking
on resource updates. Also establishes a total order on "snapshots" of a given resource. See section #7.

### 6.3 `create_time`

- Type: `google.protobuf.Timestamp`.
- Records when the resource was created.
- Set once at creation; never updated.

### 6.4 `update_time`

- Type: `google.protobuf.Timestamp`.
- Records when the resource was last modified by a user action (Create, Update, or a custom mutating method).
- Updated on every mutation. Internal state changes made by the system (e.g., a scheduler assigning a worker) **may** also update this field, but are not required to.

---

## 7. Resource Freshness and Optimistic Concurrency

*Inspired by [AIP-154](https://google.aip.dev/154). Diverges in field name and type.*

When two clients update the same resource concurrently, the second write may silently overwrite the first. Freshness validation lets a client prove it is operating on the state it thinks it is, so the server can reject stale writes.

Substrate uses a field named **`version`** of type `int64` for this. AIP-154 uses an opaque `etag` string; we diverge for a concrete reason: Substrate maintains an in-memory worker cache that guards against applying stale watch events over newer cached state using a numeric `>=` comparison. An opaque string cannot serve this role — the ordering guarantee IS the implementation contract, so the field type should reflect it.

The name `version` is intentional: it increments on every write (like Kubernetes's `resourceVersion`) and is a transparent, comparable integer (like Kubernetes's `generation`). It serves both roles in one field, so neither Kubernetes name fits cleanly.

### 7.1 The `version` field

`version` is a standard output-only field on every resource:

```proto
message ResourceMetadata {

  // ... other fields

  // version is increased on every mutation.
  int64 version = 4;
}
```

- Type: `int64`.
- Output-only: the server sets it. Starts at `1` on creation, incremented by `1` on every mutation.
- Monotonically increasing. A higher value is always newer.
- Updated on every mutation — both user-visible changes and system-internal ones (e.g. the scheduler binding a worker).

### 7.2 Using `version` to guard writes

A client that wants to guard against concurrent modification echoes back the `version` it last observed. The server rejects the request if the value no longer matches.

**Update:** `version` rides in the embedded resource body:

```proto
// Client read actor at version 5, now updating:
UpdateActorRequest {
  actor: Actor {
    metadata: ResourceMetadata {
      atespace: "my-space"
      name:     "my-actor"
      version:  5            // guard: fail if server is not at 5
    }
    worker_selector: ...
  }
  update_mask: "worker_selector"
}
```

- Note that the `version` field doesn't need to be included in the update mask, as it's a control field managed by the server, not
a mutable field.

**Delete and custom methods:** add an optional `int64 version` field to the request:

```proto
message DeleteActorRequest {
  ObjectRef actor  = 1;
  // Optional. If non-zero, the deletion is rejected with ABORTED if the
  // server's current version does not match.
  int64 version   = 2;
}
```

**Server behavior:**
- If the client provides a non-zero `version` that does not match the server's current value: return `ABORTED`.
- If the client omits `version` (zero value, the proto3 default): skip the freshness check and proceed.
- The freshness check is always optional from the client's perspective.
