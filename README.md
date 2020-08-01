# diycrate
box.com for linux

[![Build](https://github.com/jheld/diycrate/workflows/Build/badge.svg)](https://github.com/jheld/diycrate/actions?query=workflow%3ABuild+branch%3Amaster)

## Installation on Ubuntu
```bash
sudo apt install libffi-dev libssl-dev python3-pip
# you may want to create a virtual environment
sudo python3 setup.py install
```
## Configuration

Installation will create an empty ~/.config/diycrate/box.ini
This file will be overwritten and will contain run time specific information!

## Self-signed Certificate

Currently, in order for your machine to operate as a listener/hook against the oauth2 process with Box, you will
have to run a webserver -- which we provide for you in this application, to handle all of that, automatically.

However, in order for your browser (and your sanity), you will want to create a certificate. When running on your
local  machine/localhost, you will need to create a self-signed certificate.

Let's Encrypt has a nice write-up on how to do that: https://letsencrypt.org/docs/certificates-for-localhost/#making-and-trusting-your-own-certificates


## Install Redis

This application requires the use of redis oauth2 credential "storage" as well as meta data for the caching & states
of files against box.

```bash
sudo apt install redis-server
```

## Run the app

Currently there is no default out-of-the-box working configuration file. This is partly due to currently (should the user not setup their own) using `diycrate.xyz` (I host it) for a part of the oauth2 dance. It isn't "hard" to setup your own, nor is it hard to use the "always running, you can plug into" mine, but it's not exactly documented.

```bash
diycrate_app --cacert_pem_path /path/to/cert.pem --privkey_pem_path /path/to/privkey.pem
```

Run `diycrate_app --help` for more CLI information
