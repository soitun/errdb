# errdb-rb

A Ruby client library for the [Errdb](http://github.com/erylee/errdb).

## Getting started

You can connect to Errdb by instantiating the `Redis` class:

    require "errdb"

    errdb = Errdb.new("host", 7272)

Once connected, you can start running commands against Errdb:

    >> errdb.fetch "key", 0, 999999999
    => {}

