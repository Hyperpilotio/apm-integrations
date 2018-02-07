# Graphite

## Quick start

```{shell}
docker run -d\
 --name graphite\
 --restart=always\
 -p 80:80\
 -p 2003-2004:2003-2004\
 -p 2023-2024:2023-2024\
 -p 8125:8125/udp\
 -p 8126:8126\
 graphiteapp/graphite-statsd


 echo "foo:1|c" | nc -u -w0 127.0.0.1 8125

 python metrics.py
```
