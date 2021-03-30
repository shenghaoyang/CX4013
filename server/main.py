#!/usr/bin/env python3


"""
main.py

Main server.

You may need to install poetry and use that to perform an initial install of all
dependencies.

Using ``poetry shell`` should drop you into a shell that has the correct environment
setup with installed dependencies.

Python version used for testing was 3.9.2.

Run the server:

    ``python3 server/main.py ./bookings.sqlite``

Run the client:

    ``python3 client/main.py 127.0.0.1``
"""


import sys
import logging
import asyncio
import argparse
from pathlib import Path
from rpc.common import DEFAULT_PORT
from rpc.protocol import AddressType
from rpc.helpers import create_server
from rpc.skeleton import generate_skeleton, Skeleton
from server.bookingserver import BookingServerImpl
from server.bookingtable import Table
from collections.abc import Sequence


async def main(args: Sequence[str]) -> int:
    """
    Main client application entry point.

    :param args: program arguments a-la sys.argv.
    """
    parser = argparse.ArgumentParser(
        description="CALRPC server application.", exit_on_error=False
    )
    parser.add_argument("DATABASE_PATH", help="path to the booking database")
    parser.add_argument("--laddr", help="IPv4 listen address", default="0.0.0.0")
    parser.add_argument(
        "--lport",
        help="port to listen for connections on",
        type=int,
        default=DEFAULT_PORT,
    )
    parser.add_argument(
        "--itimeout", help="client inactivity timeout (seconds)", type=int, default=300
    )
    parser.add_argument(
        "--etimeout",
        help="lifetime of an entry in the result cache",
        type=int,
        default=60,
    )
    parser.add_argument(
        "--reinit_facilities",
        help="file containing newline delimited facility names to reinitialize the database with",
        type=argparse.FileType("r"),
    )

    # Parse the arguments.
    try:
        args = parser.parse_args(args[1:])
    except argparse.ArgumentError:
        return 1

    logging.basicConfig()
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    # Load the bookings database.
    db = Path("bookings.sqlite")
    if (f := args.reinit_facilities) is not None:
        logger.info("db: reinitializing booking database")
        try:
            names = set(f for f in f.read().split("\n") if len(f) and not f.isspace())
        finally:
            f.close()

        logger.info(f"db: registering facilities {names}")
        table = Table.new(db, names)
    else:
        table = Table(db)
        logger.info("db: loaded")

    # Generate a skeleton for the server object.
    skel = generate_skeleton(BookingServerImpl)

    # Factory function that returns a skeleton to use for every new connection.
    # Return a skeleton bound to a new server object for every new connection.
    sobjects: dict[AddressType, BookingServerImpl] = dict()
    sobject_set = set()

    def skel_fac(addr: AddressType) -> Skeleton:
        # Create the server object.
        so = BookingServerImpl(table, addr, sobject_set)
        sobjects[addr] = so
        # Bind the skeleton to the server object.
        so_skel = skel(so)
        logger.info(f"server: new connection from {addr}")
        return so_skel

    # Function that accepts a skeleton and an address on disconnection.
    def disconnect_callback(addr: AddressType):
        sobjects[addr].handle_disconnect()
        del sobjects[addr]
        logger.info(f"server: client {addr} disconnected")

    logger.info(f"server: listening at {args.laddr}:{args.lport}")
    logger.info(
        f"server: inactivity timeout: {args.itimeout}s, AMO entry timeout: {args.etimeout}s"
    )

    # Create the server and wait for it to be up.
    s = await create_server(
        (args.laddr, args.lport),
        skel_fac,
        disconnect_callback,
        args.itimeout,
        args.etimeout,
    )
    # Server for one hour.
    await asyncio.sleep(3600)
    s.stop()


if __name__ == "__main__":
    exit(asyncio.run(main(sys.argv)))
