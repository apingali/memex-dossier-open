# Memex Dossier

The full source code of memex-dossier and web-navigator are included
in this archive.  The source code is licensed under a permissive
license, see LICENSE file in memex-dossier-source-code.tar

Note that the data and specific algorithms in the source code involve
forensic and cyber threat analysis.

If you have any questions, please contact support@diffeo.coom


# Memex Dossier on Mac

To run this all of this on a Mac, you can do the following:

1) Get a Google API key from your Google Apps account and insert it
`config.yaml` where it says "PUT A VALID KEY HERE"


2) Download and installer docker:

    https://download.docker.com/mac/stable/Docker.dmg


3) You can read more background material here:

    https://docs.docker.com/docker-for-mac/install/


4) Load the docker images for Memex Dossier using this command:

    cat memex-dossier-docker-images.tar  | docker load 


5) Install docker-compose:

    curl -L https://github.com/docker/compose/releases/download/1.13.0/docker-compose-`uname -s`-`uname -m` > /usr/local/bin/docker-compose


6) Launch the containers using docker compose, which relies on the
docker-compose.yml file in this directory.

    docker-compose up -d


7) To see what is happening the docker containers, you can run:

    docker-compose logs
	

8) To shutdown the docker containers, you can run:

    docker-compose down
	

9) Install the browser extension by opening Chrome and navigating to
the extensions panel and dragging-and-dropping the .crx file from this
directory.

10) You'll see a Diffeo badge icon in the upper right corner.  Click
it to open a menu and set the "Primary Memex server" to
http://localhost:9978
