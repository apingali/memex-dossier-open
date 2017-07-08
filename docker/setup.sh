#!/bin/sh

# This setup.sh script prepares a directory to be a build directory
# for a docker container that runs the dossier web services.  See
# README.md for details.

set -e

EXT_REPOS="memex-dossier"
BUILD_FILES="Dockerfile run.sh ../requirements.txt"

D=$(cd $(dirname "$0") && pwd -P)
O=$(pwd -P)
B=${1:-0}
R=$(cd "$D/../.." && pwd -P)

# Build number suffix (if any)
B="${1}"
V=$(cd "$D" && git describe HEAD)
echo "$V$B" > "$O/container-version"

# Everything should be checked out at the correct branch, so make
# an archive of everything.
for package in $EXT_REPOS; do
    cd "$R/$package"
    git archive -o "$O/$package.tar" HEAD
    touch -t $(git show -s --format=%ci HEAD | \
        sed -e 's/ [+-]....$//' -e 's/[- :]//g' -e 's/\(..\)$/.\1/') \
        "$O/$package.tar"
done

# if source and target are different copy build files to target
if [ "$D" != "$O" ]; then
    cd "$O"
    for f in $BUILD_FILES; do
        cp -a "$D/$f" "$O"
    done
fi

NOW=$(TZ=UTC date +%Y-%m-%dT%H:%M:%SZ)
cat >>"$O/Dockerfile" <<EOF
LABEL name="memex-dossier" \\
      version="$V" \\
      release="$B" \\
      architecture="x86_64" \\
      build_date="$NOW" \\
      vendor="Diffeo, Inc." \\
      summary="Memex Dossier AKA Graph" \\
      description="Memex Dossier AKA Graph" \\
EOF

exit 0
