# CX4013 Lab Project

This project implements a remote facility booking system over a "toy" RPC protocol
to fulfill the laboratory project requirement for the CX4013 Distributed Systems
course in NTU.

The protocol, being a "toy", does have many limitations and vulnerabilities.

The implementation uses Python's `async` functionality to serve multiple clients
concurrently, and also allows an individual client to dispatch multiple calls
concurrently.

## Quickstart

This project uses Poetry for dependency management, and **requires** Python 3.9.1+.

1. Install Poetry if it's not already installed.
2. Install project dependencies using Poetry:
```console
$ poetry install
```

## Quickstart (without Poetry)

The project can still run without Poetry.

In that case, install the dependencies under the `[tool.poetry.dependencies]` section in
`pyproject.toml`.

However, due to less precise dependency version control (`pyproject.lock` is not used),
errors may occur.

After dependencies are installed, install the latest version of this project by
downloading a wheel / source distribution from the release page on 
[GitHub](https://github.com/shenghaoyang/CX4013/releases) and installing it using `pip`.

This project is not published on PyPI.

## Starting the server

1. Enter the virtualenv created by Poetry:
```console
$ poetry shell
```
2. Start with database initialization: (skip this step if the database has 
already been created, unless the database needs to be reset)
    1. Create a file containing facility names:
    ```console
    $ echo -e "TR+19\nTR+20\n" > facilities
    ```
    2. Start the server, specifying the path to the name file and the database file
    to be created:
    ```
    $ cx4013_server --reinit_facilities=facilities bookings.sqlite
    ```
3. Start without database initialization - specify a path to the existing database:
```console
$ cx4013_server bookings.sqlite
```

Other command line options can be discovered by specifying `--help`.
```console
$ cx4013_server --help
usage: cx4013_server [-h] [--laddr LADDR] [--lport LPORT] [--itimeout ITIMEOUT] [--etimeout ETIMEOUT] [--reinit_facilities REINIT_FACILITIES] DATABASE_PATH

CALRPC server application.

positional arguments:
  DATABASE_PATH         path to the booking database

optional arguments:
  -h, --help            show this help message and exit
  --laddr LADDR         IPv4 listen address
  --lport LPORT         port to listen for connections on
  --itimeout ITIMEOUT   client inactivity timeout (seconds)
  --etimeout ETIMEOUT   lifetime of an entry in the result cache
  --reinit_facilities REINIT_FACILITIES
                        file containing newline delimited facility names to reinitialize the database with
```

## Starting the client

1. Enter the virtualenv created by Poetry:
```console
$ poetry shell
```
2. Start the client by providing the server's IPv4 address. Unresolved names
are _not_ supported.
```console
$ cx4013_client 127.0.0.1
```
3. Interact with the REPL presented by the client.

Other command line options can be discovered by specifying `--help`.
```console
usage: cx4013_client [-h] [--sport SPORT] [--caddr CADDR] [--cport CPORT] [--ctimeout CTIMEOUT] SERVER

CALRPC client application.

positional arguments:
  SERVER               IPv4 address of server

optional arguments:
  -h, --help           show this help message and exit
  --sport SPORT        port to connect to on server
  --caddr CADDR        IPv4 address used for listening to callbacks.
  --cport CPORT        port to listen for callback connections on
  --ctimeout CTIMEOUT  initial connection timeout (seconds)
```

### Port conflicts

If multiple clients are being run on the same host, they may attempt to
listen for callbacks on the same address (defaulting to `0.0.0.0:5001`).
This conflict can also occur if other services are also listening on
`0.0.0.0:5001`.

This can be resolved by specifying a custom listen address and/or
port using the `--caddr` and `--cport` options, e.g. `--cport 5002`.

The same problem can occur for multiple server instances. In that case,
equivalent `--laddr` and `--lport` options can be used to avoid
address conflicts.

## Directory contents
Path | Description
-----|------------
`cx4013/`|`cx4013` Python package.
`cx4013/client`|Booking client & callback service implementation
`cx4013/rpc`|Protocol implementation (proxy/skeleton, routing, packet generation)
`cx4013/serialization`|Serialization & Deserialization routines, data types
`cx4013/server`|Booking service & callback client implementation
`diagrams`|Source for report diagrams
`tests`|Tests for `cx4013`.

## Developers

Celine Wong Si Lin, Ang Zhan Phung, Shenghao Yang
