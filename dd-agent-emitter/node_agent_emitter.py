"""
A custom emitter to broadcast metrics to hyperpilot/node-agent
"""

import socket
import sys


class Emitter(object):
    """
    Custom emitter for DataDog to submit metrics to the Node agent proxy.
    This emitter requires that the configuration have 2 additional items:
       na_host  the name/ip of the hyperpilot/node_agent host
                (required: emitter will do nothing if not set)
       na_port  the port that the proxy is listening on in Node agent format
                (optional: default is 2878)
       na_dry_run (yes|true) means "dry run" (just print the data and don't
                actually send
                (optional: default is no)
       na_meta_tags comma separated list of tags to extract as point tags
                from meta dictionary in collector JSON
                (optional: default is empty list)
    From the custom emitter documentation in datadog.conf:
    If the name of the emitter function is not specified, 'emitter' is assumed.
    We are naming the class "emitter" to keep things as simple as possible for
    configuration.
    """

    def __init__(self):
        self.proxy_dry_run = True
        self.sock = None
        self.point_tags = {}
        self.source_tags = []
        self.meta_tags = []

    # pylint: disable=too-many-branches
    def __call__(self, message, log, agent_config):
        """
        __call__ is called by DataDog when executing the custom emitter(s)
        Arguments:
        message - a JSON object representing the message sent to datadoghq
        log - the log object
        agent_config - the agent configuration object
        """

        # configuration
        if 'na_host' not in agent_config:
            log.error(
                'Agent config missing na_host (the Node agent proxy host)')
            return
        proxy_host = agent_config['na_host']
        if 'na_port' in agent_config:
            proxy_port = int(agent_config['na_port'])
        else:
            proxy_port = 2878
        self.proxy_dry_run = ('na_dry_run' in agent_config
                              and (agent_config['na_dry_run'] == 'yes'
                                   or agent_config['na_dry_run'] == 'true'))
        if log:
            log.debug('Node Agent Emitter %s:%d ', proxy_host, proxy_port)

        if 'na_meta_tags' in agent_config:
            self.meta_tags = [
                tag.strip() for tag in agent_config['na_meta_tags'].split(',')
            ]

        try:
            # connect to the proxy
            if not self.proxy_dry_run:
                self.sock = socket.socket()
                self.sock.settimeout(10.0)
                try:
                    self.sock.connect((proxy_host, proxy_port))
                except socket.error as sock_err:
                    err_str = (
                        'Node agent Emitter: Unable to connect %s:%d: %s' %
                        (proxy_host, proxy_port, str(sock_err)))
                    if log:
                        log.error(err_str)
                    else:
                        print err_str
                    return
            else:
                self.sock = None

            # parse the message
            # FIXME
            if 'series' in message:
                self.parse_dogstatsd(message)
            elif isinstance(message, list):
                self.parse_health_check(message)
            else:
                self.parse_host_tags(message)
                self.parse_meta_tags(message)
                self.parse_collector(message)
        except:
            exc = sys.exc_info()
            log.error('Unable to parse message: %s\n%s', str(exc[1]),
                      str(message))

        finally:
            # close the socket (if open)
            if self.sock is not None and not self.proxy_dry_run:
                self.sock.shutdown(socket.SHUT_RDWR)
                self.sock.close()

    def parse_dogstatsd(self, message):
        """
        Parses the JSON that was sent by dogstatsd
        Arguments:
        message - a JSON object representing the message sent to datadoghq
        """

        metrics = message['series']
        for metric in metrics:
            metric['time_stamp'] = metric['points'][0][0]
            metric['value'] = metric['points'][0][1]
            metric.pop('points', None)
            # FIXME
            # self.send_metric_to_socket(metric)

    # pylint: disable=too-many-arguments
    def send_metric(self, metric):
        """
        Sends a metric to the proxy
        """

        line = ('%s' % (metric))
        if self.proxy_dry_run or not self.sock:
            print line
        else:
            self.sock.sendall('%s\n' % (line))

    @staticmethod
    def build_tag_string(tags, skip_tag_key):
        """
        Builds a string of tag_key=tag_value ... for all tags in the tags
        dictionary provided.  If tags is None or empty, an empty string is
        returned.
        Arguments:
        tags - dictionary of tag key => tag value
        skip_tag_key - skip tag named this (None to not skip any)
        """

        if not tags:
            return ''

        tag_str = ''
        for tag_key, tag_value in tags.iteritems():
            if not isinstance(tag_value,
                              basestring) or tag_key == skip_tag_key:
                continue
            tag_str = tag_str + ' "%s"="%s"' % (tag_key, tag_value)

        return tag_str

    @staticmethod
    def convert_key_to_dotted_name(key):
        """
        Convert a key that is camel-case notation to a dotted equivalent.
        This is best described with an example: key = "memPhysFree"
        returns "mem.phys.free"
        Arguments:
        key - a camel-case string value
        Returns:
        dotted notation with each uppercase containing a dot before
        """

        buf = []
        for char in key:
            if char.isupper():
                buf.append('.')
                buf.append(char.lower())
            else:
                buf.append(char)
        return ''.join(buf)

    def parse_health_check(self, message):
        for health in message:
            health['metric'] = health['check']
            health['value'] = health['status']
            health['time_stamp'] = health['timestamp']
            health['host'] = health['host_name']

            health.pop('check')
            health.pop('status')
            health.pop('timestamp')
            health.pop('host_name')
            health.pop('id')
            self.send_metric(health)

    # pylint: disable=too-many-locals
    def parse_collector(self, message):
        """
        Parses the JSON that was sent by the collector.
        Each metric in the metrics array is considered a metric and is sent
        to the proxy.  The metric array element is made up of:
        (0):  metric name
        (1):  timestamp (epoch seconds)
        (2):  value (assuming float for all values)
        (3):  tags (including host); all tags are converted to tags except
              hostname which is sent on its own as the source for the point.

        In addition to the metric array elements, all top level elements that
        begin with : cpu* mem* are captured and the value is sent.  These items
        are in the form of:
        {
           ...
           "collection_timestamp": 1451409092.995346,
           "cpuGuest": 0.0,
           "cpuIdle": 99.33,
           "cpuStolen": 0.0,
           ...
           "internalHostname": "mike-ubuntu14",
           ...
        }
        The names are retrieved from the JSON key name splitting the key on
        upper case letters and adding a dot between to form a metric name like
        this example: "cpuGuest" => "cpu.guest" The value comes from the JSON
        key's value.

        Other metrics retrieved:
           - ioStats group.
           - processes count
           - system.load.*

        Arguments:
        message - a JSON object representing the message sent to datadoghq
        """

        tstamp = long(message['collection_timestamp'])
        host_name = message['internalHostname']

        # cpu* mem*
        for key, value in message.iteritems():
            if key[0:3] == 'cpu' or key[0:3] == 'mem':
                dotted = 'system.' + Emitter.convert_key_to_dotted_name(key)
                self.send_metric({
                    'source_type_name': 'System',
                    'metric': dotted,
                    'value': value,
                    'time_stamp': tstamp,
                    'host': host_name
                })

        # iostats
        iostats = message['ioStats']
        for disk_name, stats in iostats.iteritems():
            for name, value in stats.iteritems():
                name = (name.replace('%', '').replace('/', '_'))

                self.send_metric({
                    'metric': ('system.io.%s' % (name)),
                    'value': value,
                    'time_stamp': tstamp,
                    'host': host_name,
                    'tags': {
                        'disk': disk_name
                    }
                })

        # count processes
        processes = message['processes']
        # don't use this name since it differs from internalHostname on ec2
        host_name = processes['host']
        self.send_metric({
            'metric': 'system.processes.count',
            'value': len(processes['processes']),
            'time_stamp': tstamp,
            'host': host_name
        })

        # system.load.*
        load_metric_names = [
            'system.load.1', 'system.load.15', 'system.load.5',
            'system.load.norm.1', 'system.load.norm.15', 'system.load.norm.5'
        ]
        for metric_name in load_metric_names:
            if metric_name not in message:
                continue

            self.send_metric({
                'metric': metric_name,
                'value': message[metric_name],
                'time_stamp': tstamp,
                'host': host_name
            })

    def parse_meta_tags(self, message):
        """
        Parses the meta dict from the JSON message, looking for any existing
        keys from the na_meta_tags user configuration. Stores any as key
        value pairs in an instance variable
        NOTE: these are only passed on the first request (or perhaps
        only periodically?).  If nothing is in the mta dictionary then
        this function does nothing.
        Arguments:
        message - the JSON message object from the request
        Side Effects:
        self.point_tags set
        """
        if 'meta' not in message:
            return

        meta = message['meta']

        for tag in self.meta_tags:
            if tag in meta:
                self.point_tags[tag] = meta[tag]

    def parse_host_tags(self, message):
        """
        Parses the host-tags from the JSON message and stores them in an
        instance variable.
        NOTE: these are only passed on the first request (or perhaps
        only periodically?).  If nothing is in the host-tags, dictionary then
        this function does nothing.
        Arguments:
        message - the JSON message object from the request
        Side Effects:
        self.source_tags set
        self.point_tags set
        """

        if 'host-tags' not in message:
            return

        host_tags = message['host-tags']
        if not host_tags or 'system' not in host_tags:
            return

        for tag in host_tags['system']:
            self.source_tags.append(tag)
            if ':' in tag:
                parts = tag.split(':')
                k = self.sanitize(parts[0])
                v = self.sanitize(parts[1])
                self.point_tags[k] = v

    @staticmethod
    def sanitize(s):
        """
        Removes any `[ ] "' characters from the input screen
        """
        replace_map = {'[': '', ']': '', '"': ''}
        for search, replace in replace_map.iteritems():
            s = s.replace(search, replace)
        return s