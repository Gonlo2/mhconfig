# Python client
A simple client to use the mhconfig server from a python program and a basic checker.

## Checker
Basic client to inspect the origin of the returned values, it has two parts:

- A git blame like logic to know the file/column/line where the returned value
  is obtained. In the left is present the first eight hexadecimal characters
  of the sha256 of the file with the file path, column and line and, in the right,
  the returned configuration in a like YAML syntax.
- A logger of the config build/composition to inspect problems. It has always a
  log level of five (`error`, `warn`, `info`, `debug` and `trace`) and a message.
  It usually has the file position off the related value and sometimes the file
  position of the source of the value.

The asked log level filter all the values with a higher level so for example with
`error`, the default one, it retrieve only the error logs but with `trace` it return
all the logs. The git blame logic is only available with values greater that or equal
to `info`, like `debug` and `trace`.

### Setup
First is necessary install the necessary modules and create the protobuf/grpc stub,
to do it with a virtual environment run:

```bash
virtualenv .env
. .env/bin/activate
./prepare.sh
```

### Execution
To execute the checker run

```bash
. .env/bin/activate
MHCONFIG_AUTH_TOKEN=test python checker.py /mnt/data/mhconfig test low/priority high/priority --log-level trace
```

It will return something like
```text
f3ce5ea3 low=priority/test.yaml:2:3                1) "hola":
a1f39c0f low=priority/mio.flavor.yaml:1:1          2)     "aaaaa":
a1f39c0f low=priority/mio.flavor.yaml:1:11         3)         "database": "test_copy"
f3ce5ea3 low=priority/test.yaml:6:10               4)     "adios": "hola"
12f8bd15 high/priority/test.yaml:5:10              5)     "mundo": "tuyo"
12f8bd15 high/priority/test.yaml:7:7               6)     "o2": !undefined ~
f3ce5ea3 low=priority/test.yaml:3:10               7)     "perro": "o_no"
12f8bd15 high/priority/test.yaml:6:8               8)     "seq":
12f8bd15 high/priority/test.yaml:6:9               9)         - null
f3ce5ea3 low=priority/test.yaml:7:16              10)     "some_binary": !!binary |-
f3ce5ea3 low=priority/test.yaml:7:16              11)         TG9yZW0gaXBzdW0gZG9sb3Igc2l0IGFtZXQsIGNvbnNldGV0dXIgc2FkaXBzY2luZyBlbGl0ciwg
f3ce5ea3 low=priority/test.yaml:7:16              12)         c2VkIGRpYW0gbm9udW15IGVpcm1vZCB0ZW1wb3IgaW52aWR1bnQgdXQgbGFib3JlIGV0IGRvbG9y
f3ce5ea3 low=priority/test.yaml:7:16              13)         ZSBtYWduYSBhbGlxdXlhbSBlcmF0LCBzZWQgZGlhbSB2b2x1cHR1YS4gQXQgdmVybyBlb3MgZXQg
f3ce5ea3 low=priority/test.yaml:7:16              14)         YWNjdXNhbSBldCBqdXN0byBkdW8gZG9sb3JlcyBldCBlYSByZWJ1bS4gU3RldCBjbGl0YSBrYXNk
f3ce5ea3 low=priority/test.yaml:7:16              15)         IGd1YmVyZ3Jlbiwgbm8gc2VhIHRha2ltYXRhIHNhbmN0dXMgZXN0IExvcmVtIGlwc3VtIGRvbG9y
f3ce5ea3 low=priority/test.yaml:7:16              16)         IHNpdCBhbWV0Lgo=
75ea914e low=priority/_text.name.flavor.ext:1:1   17)     "some_external_file": "I'm a text file\n"
f3ce5ea3 low=priority/test.yaml:3:10              18)     "test": "o_no"
12f8bd15 high/priority/test.yaml:3:5              19)     "to_delete":
12f8bd15 high/priority/test.yaml:3:7              20)         -
12f8bd15 high/priority/test.yaml:4:14             21)             "adios": 2

d7c5a0dd: mhconfig.yaml:3:13: trace: Created int64 value from plain scalar
    weight: 1
            ^
d7c5a0dd: mhconfig.yaml:3:5: trace: Created map value
    weight: 1
    ^
d7c5a0dd: mhconfig.yaml:5:13: trace: Created int64 value from plain scalar
    weight: 2
            ^
d7c5a0dd: mhconfig.yaml:5:5: trace: Created map value
    weight: 2
    ^

...

12f8bd15: high/priority/test.yaml:7:7: debug: Applied ref
  o2: !format "{mio/host}:{mio/port} (database: {mio/database}, table: {mio/table})"
      ^---|
database: test_copy  # a1f39c0f: low=priority/mio.flavor.yaml:1:11

12f8bd15: high/priority/test.yaml:7:7: debug: Applied ref
  o2: !format "{mio/host}:{mio/port} (database: {mio/database}, table: {mio/table})"
      ^
12f8bd15: high/priority/test.yaml:7:7: debug: Obtained template parameter
  o2: !format "{mio/host}:{mio/port} (database: {mio/database}, table: {mio/table})"
      ^
12f8bd15: high/priority/test.yaml:7:7: error: The format tag references must be scalars
  o2: !format "{mio/host}:{mio/port} (database: {mio/database}, table: {mio/table})"
      ^
12f8bd15: high/priority/test.yaml:3:13: warn: Removing an unused deletion node
    - hola: !delete
            ^
f3ce5ea3: low=priority/test.yaml:4:10: debug: Applied ref
  aaaaa: !ref [mio]
|--------^
database: test_copy  # a1f39c0f: low=priority/mio.flavor.yaml:1:1

```
