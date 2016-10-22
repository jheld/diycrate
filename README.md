# diycrate
box.com for linux

## Installation on Ubuntu 16.04
```
sudo apt-get install libffi-dev libssl-dev
sudo apt install python3-pip
sudo python3 setup.py install
```
## Configuration

Installation will create an empty ~/.config/diycrate/box.ini
This file will be overwritten and will contain run time specific information!

## Self-signed Certificate
```
openssl req -nodes -x509 -newkey rsa:2048 -keyout diycrate-key.pem -out diycrate-cert.pem -days 365
```
## Install Redis
```
sudo apt install redis-server
```

## Run the app

```
diycrate_app --cacert_pem_path diycrate-cert.pem --privkey_pem_path diycrate-key.pem 
```

Run `diycrate_app --help` for more CLI information