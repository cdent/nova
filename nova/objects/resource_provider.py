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

from oslo_db import exception as db_exc
from oslo_utils import versionutils
from sqlalchemy.orm import joinedload
from sqlalchemy.sql import func

from nova.db.sqlalchemy import api as db_api
from nova.db.sqlalchemy import models
from nova import exception
from nova import objects
from nova.objects import base
from nova.objects import fields


@base.NovaObjectRegistry.register
class ResourceProvider(base.NovaObject):
    # Version 1.0: Initial version
    # Version 1.1: Added name field
    VERSION = '1.1'

    fields = {
        'id': fields.IntegerField(read_only=True),
        'uuid': fields.UUIDField(nullable=False),
        'name': fields.StringField(nullable=True),
    }

    @base.remotable
    def create(self):
        if 'id' in self:
            raise exception.ObjectActionError(action='create',
                                              reason='already created')
        if 'uuid' not in self:
            raise exception.ObjectActionError(action='create',
                                              reason='uuid is required')
        updates = self.obj_get_changes()
        db_rp = self._create_in_db(self._context, updates)
        self._from_db_object(self._context, self, db_rp)

    @base.remotable_classmethod
    def get_by_uuid(cls, context, uuid):
        db_resource_provider = cls._get_by_uuid_from_db(context, uuid)
        return cls._from_db_object(context, cls(), db_resource_provider)

    def obj_make_compatible(self, primitive, target_version):
        super(ResourceProvider, self).obj_make_compatible(primitive,
                                                          target_version)
        target_version = versionutils.convert_version_to_tuple(target_version)
        if target_version < (1, 1):
            if 'name' in primitive:
                del primitive['name']

    @staticmethod
    @db_api.main_context_manager.writer
    def _create_in_db(context, updates):
        db_rp = models.ResourceProvider()
        db_rp.update(updates)
        context.session.add(db_rp)
        return db_rp

    @staticmethod
    def _from_db_object(context, resource_provider, db_resource_provider):
        for field in resource_provider.fields:
            setattr(resource_provider, field, db_resource_provider[field])
        resource_provider._context = context
        resource_provider.obj_reset_changes()
        return resource_provider

    @staticmethod
    @db_api.main_context_manager.reader
    def _get_by_uuid_from_db(context, uuid):
        result = context.session.query(models.ResourceProvider).filter_by(
            uuid=uuid).first()
        if not result:
            raise exception.NotFound()
        return result


class _HasAResourceProvider(base.NovaObject):
    """Code shared between Inventory and Allocation

    Both contain a ResourceProvider.
    """

    @staticmethod
    def _make_db(updates):
        try:
            resource_provider = updates.pop('resource_provider')
            updates['resource_provider_id'] = resource_provider.id
        except (KeyError, NotImplementedError):
            raise exception.ObjectActionError(
                action='create',
                reason='resource_provider required')
        try:
            resource_class = updates.pop('resource_class')
        except KeyError:
            raise exception.ObjectActionError(
                action='create',
                reason='resource_class required')
        updates['resource_class_id'] = fields.ResourceClass.index(
            resource_class)
        return updates

    @staticmethod
    def _from_db_object(context, target, source):
        for field in target.fields:
            if field not in ('resource_provider', 'resource_class'):
                setattr(target, field, source[field])

        if 'resource_class' not in target:
            target.resource_class = (
                target.fields['resource_class'].from_index(
                    source['resource_class_id']))
        if ('resource_provider' not in target and
            'resource_provider' in source):
            target.resource_provider = ResourceProvider()
            ResourceProvider._from_db_object(
                context,
                target.resource_provider,
                source['resource_provider'])

        target._context = context
        target.obj_reset_changes()
        return target


@base.NovaObjectRegistry.register
class Inventory(_HasAResourceProvider):
    # Version 1.0: Initial version
    VERSION = '1.0'

    fields = {
        'id': fields.IntegerField(read_only=True),
        'resource_provider': fields.ObjectField('ResourceProvider'),
        'resource_class': fields.ResourceClassField(read_only=True),
        'total': fields.IntegerField(),
        'reserved': fields.IntegerField(),
        'min_unit': fields.IntegerField(),
        'max_unit': fields.IntegerField(),
        'step_size': fields.IntegerField(),
        'allocation_ratio': fields.FloatField(),
    }

    @base.remotable
    def create(self):
        if 'id' in self:
            raise exception.ObjectActionError(action='create',
                                              reason='already created')
        updates = self._make_db(self.obj_get_changes())
        db_inventory = self._create_in_db(self._context, updates)
        self._from_db_object(self._context, self, db_inventory)

    @base.remotable
    def save(self):
        if 'id' not in self:
            raise exception.ObjectActionError(action='save',
                                              reason='not created')
        updates = self.obj_get_changes()
        updates.pop('id', None)
        self._update_in_db(self._context, self.id, updates)

    @staticmethod
    @db_api.main_context_manager.writer
    def _create_in_db(context, updates):
        db_inventory = models.Inventory()
        db_inventory.update(updates)
        context.session.add(db_inventory)
        return db_inventory

    @staticmethod
    @db_api.main_context_manager.writer
    def _update_in_db(context, id_, updates):
        result = context.session.query(
            models.Inventory).filter_by(id=id_).update(updates)
        if not result:
            raise exception.NotFound()


@base.NovaObjectRegistry.register
class InventoryList(base.ObjectListBase, base.NovaObject):
    # Version 1.0: Initial Version
    VERSION = '1.0'

    fields = {
        'objects': fields.ListOfObjectsField('Inventory'),
    }

    @staticmethod
    @db_api.main_context_manager.reader
    def _get_all_by_resource_provider(context, rp_uuid):
        return context.session.query(models.Inventory).\
            options(joinedload('resource_provider')).\
            filter(models.ResourceProvider.uuid == rp_uuid).all()

    @base.remotable_classmethod
    def get_all_by_resource_provider_uuid(cls, context, rp_uuid):
        db_inventory_list = cls._get_all_by_resource_provider(context,
                                                              rp_uuid)
        return base.obj_make_list(context, cls(context), objects.Inventory,
                                  db_inventory_list)


@base.NovaObjectRegistry.register
class Allocation(_HasAResourceProvider):
    # Version 1.0: Initial version
    VERSION = '1.0'

    fields = {
        'id': fields.IntegerField(),
        'resource_provider': fields.ObjectField('ResourceProvider'),
        'consumer_id': fields.UUIDField(),
        'resource_class': fields.ResourceClassField(),
        'used': fields.IntegerField(),
    }

    @base.remotable
    def create(self):
        if 'id' in self:
            raise exception.ObjectActionError(action='create',
                                              reason='already created')
        updates = self._make_db(self.obj_get_changes())
        db_allocation = self._create_in_db(self._context, updates)
        self._from_db_object(self._context, self, db_allocation)

    @base.remotable
    def destroy(self):
        self._destroy(self._context, self.id)

    @staticmethod
    @db_api.main_context_manager.writer
    def _create_in_db(context, updates):
        db_allocation = models.Allocation()
        db_allocation.update(updates)
        context.session.add(db_allocation)
        return db_allocation

    @staticmethod
    @db_api.main_context_manager.writer
    def _destroy(context, id):
        result = context.session.query(models.Allocation).filter_by(
            id=id).delete()
        if not result:
            raise exception.NotFound()

    @staticmethod
    @db_api.main_context_manager.reader
    def _get_allocations_from_db(context,
                                 resource_provider_id,
                                 resource_class_id):
        return context.session.query(models.Allocation).filter_by(
            resource_provider_id = resource_provider_id,
            resource_class_id = resource_class_id).all()


@base.NovaObjectRegistry.register
class AllocationList(base.ObjectListBase, base.NovaObject):
    # Version 1.0: Initial Version
    VERSION = '1.0'

    fields = {
        'objects': fields.ListOfObjectsField('Allocation'),
    }

    @base.remotable_classmethod
    def get_allocations(cls, context, resource_provider, resource_class):
        resource_provider_id = resource_provider.id
        resource_class_id = Allocation.fields['resource_class'].index(
            resource_class)
        db_allocation_list = cls._get_allocations_from_db(
            context, resource_provider_id, resource_class_id)
        return base.obj_make_list(
            context, cls(context), objects.Allocation, db_allocation_list)

    @staticmethod
    @db_api.main_context_manager.reader
    def _get_allocations_from_db(context,
                                 resource_provider_id,
                                 resource_class_id):
        return context.session.query(models.Allocation).filter_by(
            resource_provider_id = resource_provider_id,
            resource_class_id = resource_class_id).all()


@base.NovaObjectRegistry.register
class ResourcePool(base.NovaObject):
    # Version 1.0: Initial Version
    VERSION = '1.0'

    fields = {
        'resource_provider': fields.ObjectField('ResourceProvider',
                                                nullable=False),
        'inventories': fields.ObjectField(
            'InventoryList', nullable=False,
            default=objects.InventoryList(objects=[])),
        'aggregates': fields.ObjectField(
            'AggregateList', nullable=False,
            default=objects.AggregateList(objects=[])),
    }

    @property
    @base.remotable
    def resources(self):
        """Join inventories with allocations to get used."""
        # NOTE(cdent): Does this mean we need yet another object?
        resource_data = self._get_resource_data(self._context,
                                                self.resource_provider)
        return resource_data

    @base.remotable
    def create(self):
        """Associate the resource_provider with the aggregates."""
        self.obj_set_defaults()
        try:
            self._make_resource_provider_aggregates(self._context,
                                                    self.resource_provider,
                                                    self.aggregates)
        except db_exc.DBDuplicateEntry:
            raise exception.ObjectActionError(
                action='create', reason='aggregate already associated')

    @staticmethod
    @db_api.main_context_manager.reader
    def _get_resource_data(context, resource_provider):
        # NOTE(cdent): Is this yet another object we need here?
        query = (context.session.query(
            models.Inventory, func.sum(models.Allocation.used).label('used')).
            join(models.Allocation,
                 models.Inventory.resource_provider_id ==  # noqa
                 models.Allocation.resource_provider_id).
            filter(models.Inventory.resource_provider_id ==  # noqa
                   resource_provider.id).
            group_by(models.Allocation.resource_class_id))
        # TODO(cdent): This is noisy and annoying and should be
        # changed before we finish this process, but doing it this
        # way for now to keep moving.
        return [dict(total=inventory.total,
                     resource_class=fields.ResourceClass.from_index(
                         inventory.resource_class_id),
                     reserved=inventory.reserved,
                     allocation_ration=inventory.allocation_ratio,
                     min_unit=inventory.min_unit,
                     max_unit=inventory.max_unit,
                     step_size=inventory.step_size,
                     used=used)
                for inventory, used in query.all()]

    @staticmethod
    @db_api.main_context_manager.writer
    def _make_resource_provider_aggregates(context, resource_provider,
                                           aggregates):
        for aggregate in aggregates:
            db_rpa = models.ResourceProviderAggregate(
                resource_provider_id=resource_provider.id,
                aggregate_id=aggregate.id)
            context.session.add(db_rpa)

    @base.remotable_classmethod
    def get_by_resource_provider_uuid(cls, context, rp_uuid):
        return cls._build_from_resource_provider_uuid(context, cls(), rp_uuid)

    @staticmethod
    @db_api.main_context_manager.reader
    def _build_from_resource_provider_uuid(context, resource_pool, rp_uuid):
        resource_pool.resource_provider = objects.ResourceProvider.get_by_uuid(
            context, rp_uuid)

        aggregates = (context.session.query(models.Aggregate).
            join(models.ResourceProviderAggregate,
                 models.ResourceProviderAggregate.aggregate_id ==  # noqa
                 models.Aggregate.id).
            filter(models.ResourceProviderAggregate
                   .resource_provider_id ==  # noqa
                   resource_pool.resource_provider.id).all())
        resource_pool.aggregates = base.obj_make_list(
            context, objects.AggregateList(context),
            objects.Aggregate, aggregates)

        resource_pool.inventories = (
            objects.InventoryList.get_all_by_resource_provider_uuid(
                context, resource_pool.resource_provider.uuid))

        resource_pool._context = context
        resource_pool.obj_reset_changes()
        return resource_pool
