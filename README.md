## Redis Rate Limiter

A service which enforces that events occur only **once** in a period of time (`event-duration`).

### Modes

#### Only Once
A typical sliding window limiter, allowing only the first of duplicated events during the `event-duration`.

<details>
<summary>Example</summary>
Event Duration: 3

Times and Events: {1, 🍎}, {2, 🌮}, {3, 🌮}, {4, 🌮}, {7, 🌮}

Allowed: {1, 🍎}, {2, 🌮}, {7, 🌮}
</details>

#### Strictly Once
A sliding window limiter, where if duplicate events subsequently occur during the `event-duration`, the later event(s) are rejected **and** the conflicting earlier event is invalidated.  In short, at least `event-duration` time should separate events, otherwise no events are valid.

<details>
<summary>Example</summary>
Event Duration: 3

Times and Events: {1, 🍎}, {2, 🌮}, {3, 🌮}, {4, 🌮}, {7, 🌮}

Allowed: {1, 🍎}, {7, 🌮}
</details>

### Event Lifecycle
* **Submitted** - *Potentially* publishable at the time of receipt.  An event *will not* be submitted if it conflicts with another, previously submitted event.  This is the entry point into the event lifecycle.
* **Invalidated** - A submitted event is invalidated when a subsequently received event creates a conflict.  This is a terminal state transition, an invalidated event can never be published.
* **Published** - No rate limit rules were violated for a submitted event throughout the event duration.  This is a terminal state transition, a published event cannot be invalidated.

### Synchronization
Event status updates are pushed using STOMP over Websocket.  Snapshots of incubating events (events submitted but not yet published) are periodically pushed.  Combined, these allow real time synchronization with other services.  

All status updates and snapshots are sent with a scalar clock.  It is *guaranteed* that an event with a lesser clock value happened-before an event with a greater clock value.  The state of incubating events can therefore be modeled.  A parallel can be drawn to video compression: the snapshots can be considered key-frames and the status updates, delta-frames. 

The synchronization is not partition tolerant.



