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

* CircleCI: [https://app.circleci.com/settings/project/github/Iterable/blobby](https://app.circleci.com/settings/project/github/Iterable/blobby)
* LaunchPad: [https://launchpad.net/~iterable-com](https://launchpad.net/~iterable-com)

Packages are generated from tags. Tag a commit with a version in the form
`v1.0.0` and it will automatically be built and uploaded to the packaging
repositories. For now this is limited to an Ubuntu PPA but this may include
other repositories in the future.

To create a release that will only go to the test and staging environments, add `devX` (where `X` is a number) to the tag: `v1.0.0-dev1`

## Development

### Docker builds

* Build a Docker container of blobby:

```bash
docker build . -t blobby:latest
```

* Remove old and intermediate build stage images:

```bash
docker rmi $(docker images -f "dangling=true" -q)
docker image prune -f
```

### Docker Compose environment

Build the Blobby Docker container first, then start the 3 Blobby instances:

```bash
docker-compose up blobby1 blobby2 blobby3
```

Note:
1. Any namespace directories must be created in docker-compose/data prior to starting Blobby.
2. If S3 is not used, all files must be removed prior to restarting Blobby or it will enter a fail loop trying
   to upload.

#### Replicated setup

A set of configurations and a compose file for a locally replicated setup exists in docs/compose-setups/replicated
