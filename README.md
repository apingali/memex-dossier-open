# Memex Dossier

Get an account at [https://diffeo.com](https://diffeo.com)

NB: this is a redacted version that has had sensitive data removed.  The deleted content is 1,622 lines of code (8% of 21,226 lines) and 188,800 lines of data (43% of 430,835)

```bash
  find . -type f -print0 | xargs -0 cat | wc -l
  find -E . -type f -not -path "*data*" -regex ".*(py|js)$"  -print0 | xargs -0 cat | wc -l
```

For simplified installation instructions, see docker/README.md

Memex Dossier is a system that helps you find personas of interest on
the Web, including the Dark Web.  

Memex Dossier is free and open source software (FOSS) released under
the MIT/X11 license.  The web services and python interfaces provided
by Memex Dossier can support a variety of frontend applications.  It
evolved from the [Dossier Stack
components](https://github.com/dossier) initially released on public
GitHub.  As Memex has evolved, the focus in this effort switched from
general machine learning algorithms to explicitly searching
Dark-to-Open Web for personas of interest, so we pushed Memex Dossier
into a private git repo.  You can request access by emailing
`support@diffeo.com`.

The primary component application in Memex Dossier is the AKA Graph,
which utilizes the underlying `memex_dossier.streamcorpus_structured`
for extracting and normalizing phone numbers, email addresses, and
similar so-called ``hard`` selectors, and `memex_dossier.handles`
tools for scoring of ``soft`` selectors like username strings.

## AKA Graph Algorithm -- graph-based data clustering

AKA means "Also Known As"

The AKA Graph is a probabilistic union find algorithm implemented on
top of Elasticsearch.  [Union Find is also known as the Disjoint-set
data
structure](https://en.wikipedia.org/wiki/Disjoint-set_data_structure)
and can be optimized to have one of the [slowest growing order
complexities of any known non-trivial
algorithm](https://www.cs.princeton.edu/~rs/AlgsDS07/01UnionFind.pdf)

The goal of Union Find is structured data clustering.  Typically,
Union Find is not probabilistic.  Any two records connected by an edge
are in the same cluster, and the edge is either on or off.  Union Find
enables fast lookups by *losing* information about the individual
edges and retaining only the entire connected component.  This speed
gain makes Union Find attractive, however the restriction to
true-false edges is frustrating to many use cases.

One can put probability-weighted edges into Union Find using multiple
replicas of the disjoint-set data structure.  Instead of trying to put
the weights into a single union find instance, this approach maintains
k-replicas, with `k` equal to say, 10, and uses edge weight
information to decide how many of the replicas will receive the edge.
When a replica receive an edge, it applies any cluster merges implied
by the edge, which expands its connected components.  Since the merge
process loses information, the exact details of the original edge are
no longer available.  However, by only applying the merge in some of
the replicas, the total system retains knowledge of how likely that
edge was.  

To query the AKA Graph, one searches for a record, and then asks the
k-replicas union find system what other records are connected to that
record.  All records are in all replicas, however each replica can
have different connected components, so the response is structured
like this:

```
 query_record --> [(rec1, num_replicas1), (rec2, num_replicas2), ...]
```

where the `num_replicas_i` indicates the number of replicas that show
`rec_i` being connected to the `query_record`.  The probability of the
link is simply `num_replicas_i` divided by the total number of
replicas.

We have instrumented the ETL process in `memex_dossier.akagraph.etl`
to apply edge weights based on scoring name strings from records that
look like this:
```json
    {
        "url": "https://example.com/foo", 
        "username": [
          "foo"
        ], 
        "confidence": 1.0, 
        "name": [
          "foo"
        ]
    }
```


## Evaluation Techniques

... to write today


### Tests, Development, and Documentation

While the old API documentation is available at
[http://dossier-stack.readthedocs.org]
(http://dossier-stack.readthedocs.org#module-dossier.models), this
repo contains additional components that are only released to DARPA
and not made public.  To get documentation, read the doc strings in
the code.

To do development on Ubuntu, you'll want to do something like this:
```bash
sudo aptitude install -y \
     emacs git \
     htop docker.io postgresql redis-server \
     make libz-dev libxslt1-dev libxml2-dev \
     python-dev python-virtualenv g++ xz-utils gfortran liblzma-dev \
     libpq-dev libfreetype6-dev libblas-dev liblapack-dev \
     libboost-python-dev libsnappy1 libsnappy-dev \
     libjpeg-dev zlib1g-dev libpng12-dev \
     python-numpy python-scipy python-sklearn python-matplotlib \
     python-gevent uwsgi 
```

Then, install the actual python package called `dossier`:
```bash
cd memex-dossier
pip install -e .
py.test -vvs memex_dossier -n 8 --elastic-address <your elasticsearch instance>
```

You will probably also want `docker-compose`; see the [official docker
docs for install guidance]
(http://docs.docker.com/engine/installation/ubuntulinux/).  See
further details below for using `docker/docker-compose.yml`.

To test a running instance after ingesting the example data described
below, you can hit the suggest endpoint to see AKA Graph results that
should match REDACTED
```bash
wget -O - 'https://localhost/dossier/v1/suggest/foo'
```

To test that highlights are working, you can POST data to the
highlights endpoint:
```bash
curl -X POST -d@data/example-highlights-post-body.json 'http://localhost/dossier/v1/highlights'
```
which should return quickly saying that it is processing
asynchronously and you can check its progress in two ways:
```bash
wget -O - 'http://localhost/dossier/v1/highlights/0ae6517964aed7b51b0bd17f6324c8da-0-113400000602000c0842208050000000450001020b9201004110008000002040'
```
or
```bash
coordinate -c configs/local.yaml summary
```

### Ingesting Data

To ingest data into the AKA Graph, you should use the
`memex_dossier.akagraph` command line tool that is created when you install
the `memex_dossier` python package.  To get a small example data file, run
`data/REDACTED`, which will create `REDACTED`, then run:

```bash
memex_dossier.akagraph -c configs/config.yaml --ingest REDACTED  --k-replicas 1
```

To delete the indexes, run:
```bash
memex_dossier.akagraph -c configs/config.yaml --delete --k-replicas 1
```

The `--k-replicas` flag can be set to values higher than 1, however
that functionality is not yet fully developed in the current release.
Stay tuned for an update that use replica graphs to enable k-sampling
that robustly and scalably estimates graph edge weights.


### Installation Locally (non-Docker)

`memex_dossier` provides RESTful webservices.  The easiest way to run these
is with uwsgi.  This has been tested on `ubuntu`.  See
docker/Dockerfile.  `memex_dossier` requires Python 2.7, a running
Elasticsearch system, and also a database supported by kvlayer, such
as postgres or redis.

To run these web services without using a docker container, run these
commands:

```bash
virtualenv ve
source ./ve/bin/activate
pip install .
```

NB: there is no `-e` flag on that `pip` command.

Note that old versions of subcomponents inside memex_dossier.* may still be
available in public PyPI.  If these packages get into your
environment, they will probably break everything.  If that happens use

```bash
or a in fc store web labels models; 
do
	pip uninstall memex_dossier.$a -y
done;
```

### building a Docker container image

Given all the third-party library dependencies in `memex_dossier`, it is
easiest to run it in a Docker container.  To build the container
image, do these steps:

 1. Make a build directory:
```bash
mkdir md-build
```

 1. Populate the build directory with the build components:
```bash
cd md-build
~/memex-dossier/docker/setup.sh
```

 1. Build the container image
```bash
sudo docker build -t memex-dossier .
```

 1. Run a container from the image
```bash
sudo docker run -dit --name memex-dossier -p 80:57312 -v $HOME/memex-dossier/configs/config.yaml:/config.yaml memex-dossier
```


### Running Full System (`docker-compose`)

To run all of the functionality in `memex_dossier`, you need to run two
other containers in addition to the `memex_dossier` container, for a total
of three containers.  The other two containers will run a `coordinate`
daemon and `coordinate` worker.  The `coordinate` package is a FOSS
offering from Diffeo, see https://github.com/diffeo/coordinate.

Included in this repo is a config file for `docker-compose`, which
makes it easy to run all three containers from the one image built
above, see `docker/docker-compose.yml`.

Running the deployment is very simple:

```bash
cd memex-dossier/docker
docker-compose up -d
```

NB: the `docker-compose.yml` executes `coordinate flow` to push the
WorkSpecs defined in `configs/flow.yaml` into the `coordinate` daemon.

This will pull all of the images and bring up all of the containers in the
background. You can look at what's running with:

```bash
docker-compose ps
```

All containers should be "up." You can stop everything with a clean slate:

```bash
docker-compose stop && docker-compose rm --force
```

Finally, you can inspect the logs of any running container, e.g.,

```bash
docker-compose logs coordinated
docker-compose logs coordinate_worker
docker-compose logs dossier
```

#### Files you will need

config.yaml

    A standard Memex Dossier Stack config. This has config blocks for
    `coordinate`. It also has a new config option in the
    `memex_dossier.models` block called `google_api_search_key`.  To run
    feature extraction, you must modify the
    `memex_dossier.models/tfidf_path` option in this file to point to the
    `memex-50000.tfidf` file in this directory.

flow.yaml

    A `coordinate` flow file that specifies the work specs available
    to be run. Currently, it has: `ingest`, `highlight`, and `dragnet`

data/background-50000.tfidf

    A tf-idf background model. This is necessary to get the `bowNP_sip`
    feature populated. This must be configured in your `config.yaml`.



#### Run the backend

To run the backend, you will need to run *three* processes (this used to be
only one):

1. The web server.
2. The coordinated scheduler.
3. A worker.

We are in the middle of a transition to a new unified scheduler, but for now,
we use what works today: `coordinated` and `rejester_worker`.

To install the web server, create a new virtualenv and install
`memex_dossier`:

```bash
    virtualenv fresh
    source fresh/bin/activate
    pip install memex_dossier.models
```

Confirm that you have the correct version:

    pip list | grep memex_dossier

If the output is empty, then that means you need to upgrade your version of
`memex_dossier.models`.

Now run the web server with:

    uwsgi --http-socket localhost:8080 \
          --wsgi memex_dossier.models.web.wsgi \
          --py-autoreload=2 \
          --processes 1 \
          --pyargv "-c config.yaml"

NOTE: The config in this directory assumes that you have a local Redis
server running on its standard port.  You can change the kvlayer block
to point at a different backend.

This particular command is useful for development, because it will restart the
web server whenever changes are made to the files. Also, the `config.yaml` file
should be the same as the one in this directory.

Now you need to run `coordinated`, which is the job sheduler:

    mkdir -p /tmp/diffeo
    coordinated -c config.yaml

Now you need to teach the scheduler about the types of asynchronous jobs you'll
be running. These are described in the `flow.yaml` file in this directory.
There is only one type of job: `ingest`.

    coordinate -c config.yaml flow flow.yaml

The above command will need to be run every time you restart `coordinated`
(which should not be often).

And finally, a worker process. This is the process that will do keyword
extraction, Google searches and ingest.

    coordinate_worker -c config.yaml --foreground

You should not need to restart this process often, even if you've made changes
to the work unit code (which is in
`memex_dossier/models/web/routes.py:rejester_run_extract`). This is because the
process will dynamically load the work unit code every time a new job is
submitted.
