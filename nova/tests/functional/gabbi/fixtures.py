# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

from gabbi import fixture
from oslo_messaging import conffixture as messaging_conffixture

import nova.conf
from nova import config
from nova import rpc
from nova import service
from nova import wsgi


API_SERVICE = 'osapi_compute'
# Hack around our need to have a global oslo config.
CONF = None

def setup_app():
    global CONF
    loader = wsgi.Loader()
    return loader.load_app(API_SERVICE)


class ConfigFixture(fixture.GabbiFixture):

    def __init__(self):
        self.conf = None

    def start_fixture(self):
        global CONF
        config.parse_args([], default_config_files=[], configure_db=False,
                          init_rpc=False)
        self.conf = CONF = nova.conf.CONF
        self.messaging_conf = messaging_conffixture.ConfFixture(self.conf)
        self.messaging_conf.transport_driver = 'fake'
        rpc.init(self.conf)

        self.conf.set_override('rpc_backend', 'fake')

    def stop_fixture(self):
        rpc.cleanup()
        if self.conf:
            self.conf.reset()
