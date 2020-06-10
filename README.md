# diycrate
box.com for linux

## Installation on Ubuntu 16.04
```bash
sudo apt install libffi-dev libssl-dev python3-pip
# you may want to create a virtual environment
sudo python3 setup.py install
```
## Configuration

Installation will create an empty ~/.config/diycrate/box.ini
This file will be overwritten and will contain run time specific information!

## Self-signed Certificate
```bash
openssl req -nodes -x509 -newkey rsa:2048 -keyout diycrate-key.pem -out diycrate-cert.pem -days 365
```
## Install Redis

This application requires the use of redis oauth2 credential "storage" as well as meta data for the caching & states
of files against box.

```bash
sudo apt install redis-server
```

## Run the app

Currently there is no default out-of-the-box working configuration file. This is partly due to currently (should the user not setup their own) using `diycrate.xyz` (I host it) for a part of the oauth2 dance. It isn't "hard" to setup your own, nor is it hard to use the "always running, you can plug into" mine, but it's not exactly documented.

```bash
diycrate_app --cacert_pem_path diycrate-cert.pem --privkey_pem_path diycrate-key.pem 
```

Run `diycrate_app --help` for more CLI information