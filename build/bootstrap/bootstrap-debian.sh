#!/usr/bin/env bash
#
# On a Debian/Ubuntu system, bootstraps a docker install and the cockroach
# repo.

set -euxo pipefail

curl -fsSL https://deb.nodesource.com/gpgkey/nodesource.gpg.key | sudo apt-key add -
echo "deb https://deb.nodesource.com/node_12.x xenial main" | sudo tee /etc/apt/sources.list.d/nodesource.list

curl -fsSL https://dl.yarnpkg.com/debian/pubkey.gpg | sudo apt-key add -
echo "deb https://dl.yarnpkg.com/debian/ stable main" | sudo tee /etc/apt/sources.list.d/yarn.list

sudo apt-get update
sudo DEBIAN_FRONTEND=noninteractive apt-get dist-upgrade -y -o Dpkg::Options::="--force-confdef" -o Dpkg::Options::="--force-confold"
sudo apt-get install -y --no-install-recommends \
  mosh \
  autoconf \
  cmake \
  ccache \
  docker.io \
  libncurses-dev \
  make \
  gcc \
  g++ \
  git \
  yarn \
  bison

# Install nodejs on 20.04.
curl -sL https://deb.nodesource.com/setup_12.x | sudo -E bash -
sudo apt-get install -y nodejs

sudo adduser "${USER}" docker

# Configure environment variables.
echo 'export PATH="/usr/lib/ccache:${PATH}:/usr/local/go/bin"' >> ~/.bashrc_bootstrap
echo 'export COCKROACH_BUILDER_CCACHE=1' >> ~/.bashrc_bootstrap
echo '. ~/.bashrc_bootstrap' >> ~/.bashrc
. ~/.bashrc_bootstrap

# Upgrade cmake.
trap 'rm -f /tmp/cmake.tgz' EXIT
curl -fsSL https://github.com/Kitware/CMake/releases/download/v3.17.0/cmake-3.17.0-Linux-x86_64.tar.gz > /tmp/cmake.tgz
sha256sum -c - <<EOF
b44685227b9f9be103e305efa2075a8ccf2415807fbcf1fc192da4d36aacc9f5  /tmp/cmake.tgz
EOF
sudo tar -C /usr -zxf /tmp/cmake.tgz && rm /tmp/cmake.tgz

# Install Go.
trap 'rm -f /tmp/go.tgz' EXIT
curl -fsSL https://dl.google.com/go/go1.13.14.linux-amd64.tar.gz > /tmp/go.tgz
sha256sum -c - <<EOF
32617db984b18308f2b00279c763bff060d2739229cb8037217a49c9e691b46a /tmp/go.tgz
EOF
sudo tar -C /usr/local -zxf /tmp/go.tgz && rm /tmp/go.tgz

# Clone CockroachDB.
git clone https://github.com/cockroachdb/cockroach "$(go env GOPATH)/src/github.com/cockroachdb/cockroach"

# Install the Unison file-syncer.
. bootstrap/bootstrap-unison.sh
