# Pluk
Pluk is a simple git large file system implementation done in 2 parts: git data and the data itself.

Data in git contains only links to the data chunks while data is separated by chunks and named after its SHA512 hash.

## Installation and running

### Using docker image

For simple running pluk in docker container, just use image *kuberlab/pluk:latest*:

```
docker run -it --rm kuberlab/pluk:latest
```

### Using this git repo

**Prerequisites:**

   * git
   * go (1.7/1.8/1.9)
   * golang-glide (see https://github.com/Masterminds/glide or just run `curl https://glide.sh/get | sh` to install)

**Installation steps:**

* clone the repository:
* run `glide install -v`
* run `go install -v ./...`
* binaries are saved in `$GOPATH/bin` and named **pluk** and **kdataset**

**Note**: Paths marked as env variables `GIT_BARE_DIR`,`GIT_LOCAL_DIR` and `DATA_DIR`
 (by default `/git`, `/git-local` and `/data` accordingly, see below) must be available for write.

## Configuration variables

There are a couple of environment variables for configuration of authentication, master-slave communication and other:

* `DEBUG`: if set to `true`, enables debug log level. Defaults to `false`.
* `AUTH_VALIDATION`: if set, this URL can be used to proxy authentication to third-party service.
Currently, **pluk** sends `Authorization` and `Cookie` headers to that URL. If response status code not in *4xx/5xx* codes,
then authentication process succeeds and then will be cached for future requests. Currently it is used with **cloud-dealer** service auth.
* `MASTERS`: this variable may contain **pluk** instance(s) master URL(s). Those **pluk** instances which have masters specified are
treated as *slaves* and usually slaves re-request auth for mounting **webdav** and re-request datasets file structure and also
 file chunks if they are absent on this slave. If some data is pushed to slave, then slave reports it to master to keep data consistence.
* `INTERNAL_KEY`: used for internal slave-to-master requests to skip authentication on master. The key on the master must be equal to the key on each slave in this case.

* `DATA_DIR`: directory which contains real file chunks. Defaults to `/data`.
* `GIT_BARE_DIR`: directory which contains git bare repositories for versioning datasets. Defaults to `/git`.
* `GIT_LOCAL_DIR`: directory which contains git local repositories. Defaults to `/git-local`.

## CLI reference

CLI simplifies download, upload and authentication processes.

Once you have installed CLI, you will have `kdataset` entry in you `PATH` so it can be easily called by typing `kdataset`.

To see the help, type `kdataset --help`.

`kdataset` provides the following commands:
 * `kdataset push <workspace> <dataset-name>:<version>`
 * `kdataset pull <workspace> <dataset-name>:<version>`
 * `kdataset dataset-list <workspace>`
 * `kdataset version-list <workspace> <dataset-name>`
 * `kdataset dataset-delete <workspace> <dataset-name>`
 * `kdataset version-delete <workspace> <dataset-name>:<version>`

### CLI Configuration

In order to pass authentication on server and get the right pluk url, there must be a config file located at `~/.kuberlab/config`
by default. If a config file doesn't exist, it needs to be created. It contains simple yaml with the following values:

```yaml
base_url: https://go.kuberlab.io/api/v0.2
token: <your-user-token>
# pluk_url: https://go.kuberlab.io/pluk/v1 (optional, need in case you want to use another pluk instance)
```

By default, Pluk URL is calculated automatically using `base_url` from yaml config. Also, pluk url can be passed to CLI via:
* config value `pluk_url`
* `--url` parameter of `kdataset` CLI, e.g. `kdataset --url http://host:port/pluk/v1 push workspace dataset:1.0.0`

**Note**: `--url` parameter takes precedence over config value.

