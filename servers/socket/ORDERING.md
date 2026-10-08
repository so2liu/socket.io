# Incoming event delivery

Decoded packets are queued in receive order. Event listeners (including `OnAny`)
use a separate sequential queue, and an asynchronous middleware must call `next`
before the next event starts. ACK packets keep using the packet queue, so a
listener may wait for a client callback without preventing that ACK from arriving.
Disconnect releases a pending middleware wait and skips remaining listeners.

Keep event listeners short. Establish the operation and its cancellation handle
synchronously, then start long work in a goroutine and return. A later stop event
can then cancel it. Do not put a synchronous long job or an indefinitely blocking
middleware in this queue. The library cannot infer which business operations may
run concurrently; application code owns that choice and operation cleanup.

No wire format, callback signature, timeout value, or database changes are made.
On process restart old connections are lost and clients reconnect. Application
work must drain or cancel using the application's existing shutdown policy;
this queue does not persist work or retry side effects. Reverting the image does
not change stored data. Blade Plugin Host lifecycle is outside this library.
