"""
main.py

Main client interface.
"""


import argparse
import asyncio
import sys
from collections.abc import Sequence

from prompt_toolkit import print_formatted_text as print, HTML
from prompt_toolkit.shortcuts import clear

from client.notificationserver import BookingNotificationServerImpl
from client.repl import Repl
from client.hooks import RandomRequestReplyDropper
from rpc.common import DEFAULT_PORT
from rpc.helpers import create_and_connect_client, create_server
from rpc.protocol import AddressType
from rpc.skeleton import generate_skeleton, Skeleton
from server.bookingserver import BookingServerProxy


async def main(args: Sequence[str]) -> int:
    """
    Main client application entry point.

    :param args: program arguments a-la sys.argv.
    """
    parser = argparse.ArgumentParser(
        description="CALRPC client application.", exit_on_error=False
    )
    parser.add_argument("SERVER", help="IPv4 address of server")
    parser.add_argument(
        "--sport", help="port to connect to on server", type=int, default=DEFAULT_PORT
    )
    parser.add_argument(
        "--caddr",
        help="IPv4 address used for listening to callbacks.",
        default="0.0.0.0",
    )
    parser.add_argument(
        "--cport",
        help="port to listen for callback connections on",
        type=int,
        default=DEFAULT_PORT + 1,
    )
    parser.add_argument(
        "--ctimeout", help="initial connection timeout (seconds)", type=int, default=10
    )

    # Parse the arguments.
    try:
        args = parser.parse_args(args[1:])
    except argparse.ArgumentError:
        return 1

    # Setup the notification server.
    skel = generate_skeleton(BookingNotificationServerImpl)
    ns = BookingNotificationServerImpl()

    def skel_fac(addr: AddressType) -> Skeleton:
        so_skel = skel(ns)
        return so_skel

    def disconnect_callback(addr: AddressType):
        pass

    # Create notification server.
    s = await create_server(
        (args.caddr, args.cport),
        skel_fac,
        disconnect_callback,
    )

    # Attempt to connect to the remove server.
    print(HTML(r"<b>Connecting to RPC server</b>"))
    try:
        dropper_hooks = RandomRequestReplyDropper(), RandomRequestReplyDropper()
        c, p = await asyncio.wait_for(
            create_and_connect_client(
                (args.SERVER, args.sport),
                BookingServerProxy,
                pre_send=dropper_hooks[0],
                pre_receive=dropper_hooks[1],
            ),
            args.ctimeout,
        )
        print(HTML(r"<b>Connected</b>"))
    except asyncio.TimeoutError:
        print(HTML(r"<b><ansired>Could not connect to RPC server</ansired></b>"))
        return 1

    clear()
    repl = Repl(c, p, args.cport, ns, dropper_hooks)
    try:
        await repl.run()
    finally:
        c.close()


if __name__ == "__main__":
    exit(asyncio.run(main(sys.argv)))
