#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.
"""Handlers for placement API"""

import hashlib
import hmac
import os

import jsonschema
from oslo_config import cfg
from oslo_log import log as logging
from oslo_utils import secretutils as secutils
from oslo_utils import uuidutils
from oslo_serialization import jsonutils
import selector
import six
import webob.dec
import webob.exc

from nova.api.metadata import base
from nova import cache_utils
from nova import context as nova_context
from nova import exception
from nova.i18n import _
from nova.i18n import _LE
from nova.i18n import _LW
from nova import wsgi
from nova import objects

CONF = cfg.CONF
CONF.import_opt('use_forwarded_for', 'nova.api.auth')

LOG = logging.getLogger(__name__)


RESOURCE_POOL_SCHEMA = {
    "type": "object",
    "properties": {
        "name": {
            "type": "string"
        },
        "uuid": {
            "type": "string"
        },
        "aggregates": {
            "type": "array",
            "items": {
                "type": "string"
            }
        },
        "resources": {
            "type": "object",
            "patternProperties": {
                "^(/[^/]+)+$": {
                    "type": "object",
                    "properties": {
                        "total": {
                            "type": "integer"
                        },
                        "reserved": {
                            "type": "integer"
                        },
                        "min_unit": {
                            "type": "integer"
                        },
                        "max_unit": {
                            "type": "integer"
                        },
                        "step_size": {
                            "type": "integer"
                        },
                        "allocation_ratio": {
                            "type": "number"
                        },
                    },
                    "additionalProperties": False,
                }
            },
            "additionalProperties": False,
        }
    },
    "additionalProperties": False
}


@webob.dec.wsgify
def list_resource_pools(req):
    context = req.environ['nova.context']
    #resource_pools = objects.ResourcePoolList.get_resource_pools(context)
    req.response.body = 'resource pools\n%s' % context
    return req.response


@webob.dec.wsgify
def create_resource_pool(req):
    context = req.environ['nova.context']
    try:
        data = jsonutils.loads(req.body)
        jsonschema.validate(data, RESOURCE_POOL_SCHEMA)
    except (ValueError, jsonschema.ValidationError):
        raise  # XXX

    resource_provider = objects.ResourceProvider(
        context, name=data.get('name'),
        uuid=data.get('uuid', uuidutils.generate_uuid()))
    resource_provider.create()

    aggregate_data = data.get('aggregates', [])
    aggregate_list = [objects.Aggregate.get_by_uuid(context, aggregate_uuid)
                      for aggregate_uuid in aggregate_data]
    aggregate_list = objects.AggregateList(context,
                                           objects=aggregate_list)

    resource_pool = objects.ResourcePool(context,
                                         resource_provider=resource_provider,
                                         aggregates=aggregate_list)

    req.response.body = jsonutils.dumps(resource_pool.obj_to_primitive())
    return req.response


class PlacementHomeRequestHandler(wsgi.Application):
    """Serve Placement Root."""

    @webob.dec.wsgify(RequestClass=wsgi.Request)
    def __call__(self, req):
        req.response.body = 'hi'
        return req.response


class ResourcePoolsHandler(wsgi.Application):
    """Serve Resource Pools."""

    def __init__(self):
        self._selector = selector.Selector()
        self._selector.add('[/]', GET=list_resource_pools, POST=create_resource_pool)

    def __call__(self, environ, start_response):
        return self._selector(environ, start_response)
