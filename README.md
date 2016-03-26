# diycrate
box.com for linux

We now support SSL (self signed cert's yo)! Unfortunately, this means getting openssl, ffi, and python dev libraries installed, beforehand.
Also, you will need redis, but I do supply that inside the source code, so all you have to do is run "make" and "sudo make install" on the [untar'd] redis directory.

I have not documented explicitly how to get this thing running, yet, so please give me time, or make an Issue on this project to let me know someone is actually trying to use it. If we want, I can even try packaging this up as deb and rpm to make the setup easier.
