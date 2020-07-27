This request files can be used with grpcurl, for example

```bash
cat get.json | grpcurl -H 'mhconfig-auth-token: test' --plaintext --import-path /source/path/src/mhconfig/proto/ -proto mhconfig.proto -d @ 127.0.0.1:2222 mhconfig.proto.MHConfig/Get
```

Remember change the `root_path` of the requests files.
