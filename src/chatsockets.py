from curio import Queue, TaskGroup, run, spawn
from curio.socket import IPPROTO_TCP, TCP_NODELAY
from wsproto import ConnectionType, WSConnection
from wsproto.events import (
    AcceptConnection,
    BytesMessage,
    CloseConnection,
    Request,
    TextMessage,
)


async def ws_adapter(in_q, out_q, client, _):
    """A simple, queue-based Curio-Sans-IO websocket bridge."""
    client.setsockopt(IPPROTO_TCP, TCP_NODELAY, 1)
    wsconn = WSConnection(ConnectionType.SERVER)
    closed = False

    while not closed:
        wstask = await spawn(client.recv, 65535)
        outqtask = await spawn(out_q.get)

        async with TaskGroup([wstask, outqtask]) as g:
            task = await g.next_done()
            result = await task.join()
            await g.cancel_remaining()

        if task is wstask:
            wsconn.receive_data(result)
            payload = b""
            for event in wsconn.events():
                cl = event.__class__
                if cl in (TextMessage, BytesMessage):
                    await in_q.put(event.data)
                elif cl is Request:
                    # Auto accept. Maybe consult the handler?
                    payload += wsconn.send(AcceptConnection())
                elif cl is CloseConnection:
                    # The client has closed the connection.
                    await in_q.put(None)
                    closed = True
                else:
                    print(event)
            await client.sendall(payload)
        else:
            # We got something from the out queue.
            payload = b""
            if result is None:
                # Terminate the connection.
                print("Closing the connection.")
                payload += wsconn.send(CloseConnection)
                closed = True
            else:
                payload += wsconn.send(result)
            await client.sendall(payload)
    print("Bridge done.")


async def ws_echo_server(in_queue, out_queue):
    """Just echo websocket messages, reversed. Echo 3 times, then close."""
    for _ in range(3):
        msg = await in_queue.get()
        if msg is None:
            # The ws connection was closed.
            break
        await out_queue.put(msg[::-1])
    print("Handler done.")


def serve_ws(handler):
    """Start processing web socket messages using the given handler."""

    async def run_ws(client, addr):
        in_q, out_q = Queue(), Queue()
        ws_task = await spawn(ws_adapter, in_q, out_q, client, addr)
        await handler(in_q, out_q)
        await out_q.put(None)
        await ws_task.join()  # Wait until it's done.
        # Curio will close the socket for us after we drop off here.
        print("Master task done.")

    return run_ws


def main():
    from curio import tcp_server

    port = 5000
    print(f"Listening on port {port}.")
    run(tcp_server, "", port, serve_ws(ws_echo_server))


if __name__ == "__main__":
    main()
