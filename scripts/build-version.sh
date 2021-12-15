#!/bin/bash
set -euxo pipefail

# Shamelessly stolen from https://github.com/alsuren/cargo-quickinstall/blob/main/build-version.sh

cd "$(dirname "$0")/.."

CRATE="kache"
VERSION="$(git describe HEAD)"

date

# FIXME: make a signal handler that cleans this up if we exit early.
if [ ! -d "${TEMPDIR:-}" ]; then
    TEMPDIR="$(mktemp -d)"
fi

TARGET_ARCH=$(rustc --version --verbose | sed -n 's/host: //p')

cargo install --path=.

BINARIES=$(
    cat ~/.cargo/.crates2.json | jq -r '
        .installs | to_entries[] | select(.key|startswith("'${CRATE}' ")) | .value.bins | .[]
    ' | tr '\r' ' '
)


cd ~/.cargo/bin
for file in $BINARIES; do
    if file $file | grep ': data$'; then
        echo "something wrong with $file. Should be recognised as executable."
        exit 1
    fi
done

# Package up the binaries so that they can be untarred in ~/.cargo/bin
tar -czf "${TEMPDIR}/${CRATE}-${VERSION}-${TARGET_ARCH}.tar.gz" $BINARIES

echo "${TEMPDIR}/${CRATE}-${VERSION}-${TARGET_ARCH}.tar.gz"
