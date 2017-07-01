# Aya
A lunatic seckill server, pursuing the extreme throughput.

# Build

pull the submodules first by doing:
```
git submodule init
git submodule update --init --recursive
```

patch the seastar with files under patch
```
./patch.sh
```

Installing required packages:
```
sudo ./seastar/install-dependencies.sh
```

```
./configure.py
ninja
```
