# Blobby
- - -

Blobby is a lightweight binary blob store designed to make inserting data
extremely cheap and simple while making fetches of that data support full
auditing. Blobby was intended to work with small objects but will work just
as well with very large binary blobs. Blobby will bundle writes into AWS
S3 in order to reduce overall cost of operations and improve performance
which means that it can also be used as an object aggregator much like
AWS Fire Hose.

## License

Blobby is released under the Apache 2.0 license. (See the LICENSE file)

## Packaging/Deployment

* LaunchPad: [https://launchpad.net/~liquidgecka](https://launchpad.net/~liquidgecka)
