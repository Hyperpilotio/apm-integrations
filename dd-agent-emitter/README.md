# dd-agent-emitter

To build a custom emitter for dd-agent to derive metrics

# Using

These instructions assume that the DataDog Agent is already installed on the machine you want to emit data from, and that it is configured to use the Forwarder as per the [default configuration](https://github.com/DataDog/dd-agent/wiki/Agent-Architecture).

1. Place `node_agent_emitter.py` somewhere on the file system, e.g. `/opt/node_agent`
2. Edit `/etc/dd-agent/datadog.conf` and add these 2 lines:

  ```
  na_host: proxy-host
  custom_emitters: /opt/node_agent/node_agent.py
  ```
  
  where `proxy-host` is the hostname of the machine running the Node Agent Proxy.
3. `sudo /etc/init.d/datadog-agent restart`

# Reference

* [custom emitter](https://github.com/DataDog/dd-agent/wiki/Using-custom-emitters)
* [custom emitter from Wavefront](https://github.com/wavefrontHQ/custom-emitter/blob/master/wavefrontEmitter.py)
