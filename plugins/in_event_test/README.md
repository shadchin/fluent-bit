# Event Test Input Plugin

The following plugin aims to test the event loop notification mechanisms by register all types of collectors and callbacks to validate the proper behavior. It also use network connection through a local upstream connection, coroutine return values and coroutine sleep API. 


## Test 0: collector time

On plugin initialization, a `collector_timer` is created. The timer will expire after
__2 seconds__, the registered callback will do:

- pause the active collector
- message the event loop to trigger the next test. The next test is based on a pipe
  collector, message is written to `ctx->pipe[1]`.

## Test 1: collector event (collector_fd on pipe)

The plugin context contains a pipe `ctx->pipe`, the read end `ctx->pipe[0]` is registered
as a collector_event type, the registered callback is `cb_collector_fd` and does:

- read the message (byte sent on test #0)
- pause the collector
- return

## Test 2: collector socket server

On initialization, a socket server is created that listens for connections on TCP
port 9092.

This test is activated by another callback that performs a socket connection, this
connection happens after 4 seconds of initialization.


- server socket callback: `cb_collector_socket_server`
- client socket callback: `cb_collector_socket_client`




