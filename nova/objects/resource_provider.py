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

import itertools

from oslo_db import exception as db_exc
from oslo_utils import versionutils
from sqlalchemy.orm import joinedload
from sqlalchemy import sql
from sqlalchemy.sql import func

from nova.db.sqlalchemy import api as db_api
from nova.db.sqlalchemy import models
from nova import exception
from nova import objects
from nova.objects import base
from nova.objects import fields


RESOURCE_PROVIDER_LAZY_FIELDS = ['aggregates', 'resources']


@db_api.main_context_manager.writer
def _create_rp_in_db(context, updates):
    db_rp = models.ResourceProvider()
    db_rp.update(updates)
    context.session.add(db_rp)
    context.session.flush()
    return db_rp


@db_api.main_context_manager.reader
def _get_rp_by_uuid_from_db(context, uuid):
    result = context.session.query(models.ResourceProvider).filter_by(
        uuid=uuid).first()
    if not result:
        raise exception.NotFound()
    return result


@base.NovaObjectRegistry.register
class ResourceProvider(base.NovaObject):
    # Version 1.0: Initial version
    # Version 1.1: Added name field
    # Version 1.2: Added resources and aggregates fields
    VERSION = '1.2'

    fields = {
        'id': fields.IntegerField(read_only=True),
        'uuid': fields.UUIDField(nullable=False),
        'name': fields.StringField(nullable=True),
        'aggregates': fields.ListOfObjectsField('Aggregate',
                                                default=[]),
        'resources': fields.ListOfObjectsField('ResourceUse',
                                               default=[]),
    }

    obj_relationships = {
        'aggregates': [('1.2', '1.3')],
        'resources': [('1.2', '1.0')],
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
        with db_api.main_context_manager.writer.using(self._context):
            try:
                db_rp = self._create_in_db(self._context, updates)
                self._from_db_object(self._context, self, db_rp)
                if 'aggregates' in updates:
                    self._update_aggregates(
                        self._context, self.id, updates['aggregates'])
                if 'resources' in updates:
                    self._update_resources(
                        self._context, self, updates['resources'], {}, {})
            except db_exc.DBDuplicateEntry as exc:
                if 'aggregate_id' in exc.columns:
                    reason = 'aggregate already associated'
                else:
                    reason = 'duplicate resource provider'
                raise exception.ObjectActionError(
                    action='create', reason=reason)

    @base.remotable
    def save(self):
        updates = self.obj_get_changes()
        existing_resources = {}
        existing_inventories = {}
        if 'resources' in updates:
            # Get the existing resources to compare the updates
            # with. We need this so we can have usage information
            # for conflict detection.
            existing_resources = {
                resource.resource_class: resource for resource in
                    self.__class__.get_by_uuid(
                        self._context, self.uuid).resources
            }
            existing_inventories = {
                inv.resource_class: inv for inv in
                    objects.InventoryList.get_all_by_resource_provider_uuid(
                        self._context, self.uuid)
            }

        with db_api.main_context_manager.writer.using(self._context):
            if 'name' in updates:
                self._update_in_db(self._context, self.id,
                                   {'name': updates['name']})
            if 'aggregates' in updates:
                self._drop_all_aggregates(self._context, self.id)
                try:
                    self._update_aggregates(
                        self._context, self.id, updates['aggregates'])
                except db_exc.DBDuplicateEntry:
                    # TODO(cdent): check on context provided by error, do we
                    # need to provide rp's uuid?
                    raise exception.ObjectActionError(
                        action='save', reason='aggregate already associated')
            if 'resources' in updates:
                self._update_resources(
                    self._context, self, updates['resources'],
                    existing_resources, existing_inventories)

    @base.remotable
    def destroy(self):
        """Destroy the ResourceProvider

        Remove the resource provider, resource provider aggregates,
        inventories and allocations.
        """
        return self._destroy(self._context, self.id)

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
        # NOTE(cdent): resources and aggregates fields are handled
        # by the obj_relationships mappings so we don't need
        # explicit handling of those fields here.

    def obj_load_attr(self, attrname):
        if attrname not in RESOURCE_PROVIDER_LAZY_FIELDS:
            raise exception.ObjectActionError(
                action='obj_load_attr', reason='unable to load %s' % attrname)

        if attrname == 'aggregates':
            self.aggregates = self._get_aggregates(self._context, self.id)
            self.obj_reset_changes(['aggregates'])
        if attrname == 'resources':
            self.resources = self._get_resource_data(self._context, self.id)
            self.obj_reset_changes(['resources'])

    @staticmethod
    def _create_in_db(context, updates):
        return _create_rp_in_db(context, updates)

    @classmethod
    def _destroy(cls, context, resource_provider_id):
        # Get resource information inside this transaction to avoid
        # minor race.
        with db_api.main_context_manager.writer.using(context):
            # NOTE(cdent): No cascades are configured so we do this in
            # the simple but lumpy way.
            resources = cls._get_resource_data(context, resource_provider_id)
            if any(resource.used > 0 for resource in resources):
                raise exception.ObjectActionError(
                    action='destroy', reason='resources still in use')

            rpa_query = (
                context.session.query(models.ResourceProviderAggregate)
                .filter_by(resource_provider_id=resource_provider_id))
            inv_query = (
                context.session.query(models.Inventory)
                .filter_by(resource_provider_id=resource_provider_id))
            result = (context.session.query(models.ResourceProvider)
                      .filter_by(id=resource_provider_id)).delete()

            if not result:
                raise exception.NotFound()
            for query in (rpa_query, inv_query):
                query.delete()

    @staticmethod
    def _drop_all_aggregates(context, resource_provider_id):
        # NOTE(cdent): We don't need to count the result.
        (context.session.query(models.ResourceProviderAggregate)
         .filter_by(resource_provider_id=resource_provider_id).delete())

    @staticmethod
    def _from_db_object(context, resource_provider, db_resource_provider):
        for field in resource_provider.fields:
            if field not in RESOURCE_PROVIDER_LAZY_FIELDS:
                setattr(resource_provider, field, db_resource_provider[field])
        resource_provider._context = context
        resource_provider.obj_reset_changes()
        return resource_provider

    @staticmethod
    @db_api.main_context_manager.reader
    def _get_aggregates(context, resource_provider_id):
        query = (context.session.query(models.Aggregate)
                 .join(models.ResourceProviderAggregate,
                       models.ResourceProviderAggregate.aggregate_id
                       == models.Aggregate.id)
                 .filter(models.ResourceProviderAggregate.resource_provider_id
                         == resource_provider_id))
        results = query.all()
        return [objects.Aggregate._from_db_object(
            context, objects.Aggregate(), aggregate)
                for aggregate in results]

    @staticmethod
    def _get_by_uuid_from_db(context, uuid):
        return _get_rp_by_uuid_from_db(context, uuid)

    @staticmethod
    @db_api.main_context_manager.reader
    def _get_resource_data(context, resource_provider_id):
        query = (context.session.query(
            models.Inventory,
            func.coalesce(func.sum(models.Allocation.used),
                          0).label('used')).
            outerjoin(models.Allocation,
                 sql.and_(models.Inventory.resource_provider_id ==
                          models.Allocation.resource_provider_id,
                          models.Inventory.resource_class_id ==
                          models.Allocation.resource_class_id)).
            filter(models.Inventory.resource_provider_id ==
                   resource_provider_id).
            group_by(models.Inventory.resource_class_id))
        # TODO(cdent): This is noisy and annoying and should be
        # changed before we finish this process, but doing it this
        # way for now to keep moving. Perhaps some kind of method on
        # ResourceUse itself?
        results = [ResourceUse(total=inventory.total,
                     resource_class=fields.ResourceClass.from_index(
                         inventory.resource_class_id),
                     reserved=inventory.reserved,
                     allocation_ratio=inventory.allocation_ratio,
                     min_unit=inventory.min_unit,
                     max_unit=inventory.max_unit,
                     step_size=inventory.step_size,
                     used=used)
                for inventory, used in query.all()]
        return results

    @staticmethod
    def _update_aggregates(context, resource_provider_id,
                                           aggregates):
        for aggregate in aggregates:
            db_rpa = models.ResourceProviderAggregate(
                resource_provider_id=resource_provider_id,
                aggregate_id=aggregate.id)
            context.session.add(db_rpa)
        # Flush to resolve conflicts before commit.
        context.session.flush()

    @db_api.main_context_manager.writer
    def _update_in_db(context, id_, updates):
        result = context.session.query(
            models.ResourceProvider).filter_by(id=id).update(updates)
        if not result:
            raise exception.NotFound()

    @staticmethod
    def _update_resources(context, resource_provider, resources,
                          existing_resources, existing_inventories):
        """Set the resources associated with this resource provider

        This means creating, updating or deleting Inventory objects to
        reflect the ResourceUse objects.
        """
        # TODO(cdent): This seems way overcomplicated, but I don't
        # know a more correct way to do it that happily satisfies
        # the somewhat unaligned desires of the SQL, OVO, and HTTP
        # APIs.
        for resource in resources:
            if resource.resource_class in existing_resources:
                current_resource = existing_resources[resource.resource_class]
                if current_resource.used > (
                        (resource.total - resource.reserved)
                        * resource.allocation_ratio):
                    raise exception.ObjectActionError(
                        action='save',
                        reason='resource capacity mismatch for %s'
                            % resource.resource_class)
                else:
                    try:
                        inventory = existing_inventories[
                            resource.resource_class]
                    except KeyError:
                        inventory = objects.Inventory(
                            context, resource_provider=resource_provider,
                            resource_class=resource.resource_class)
                    for field in resource.fields:
                        if field not in ('used', 'resource_provider',
                                         'resource_class'):
                            setattr(inventory, field, getattr(resource, field))
                    try:
                        inventory.save()
                    except exception.ObjectActionError:
                        inventory.create()
                del existing_resources[resource.resource_class]
            else:
                inventory = objects.Inventory(
                    context, resource_provider=resource_provider,
                    resource_class=resource.resource_class)
                for field in resource.fields:
                    if field not in ('used', 'resource_provider',
                                     'resource_class'):
                        setattr(inventory, field, getattr(resource, field))
                inventory.create()
        # Get rid of any remaining resources that are no
        # longer associated with this resource.
        for resource in existing_resources.values():
            if resource.used > 0:
                raise exception.ObjectActionError(
                    action='save',
                    reason='resource class %s has allocations'
                            % resource.resource_class)
            inventory = existing_inventories[resource.resource_class]
            with inventory.obj_alternate_context(context):
                inventory.destroy()


class _HasAResourceProvider(base.NovaObject):
    """Code shared between Inventory and Allocation

    Both contain a ResourceProvider.
    """

    @staticmethod
    def _make_db(updates):
        try:
            resource_provider = updates.pop('resource_provider')
            updates['resource_provider_id'] = resource_provider.id
        except (KeyError, NotImplementedError, exception.ObjectActionError):
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


@db_api.main_context_manager.writer
def _create_inventory_in_db(context, updates):
    db_inventory = models.Inventory()
    db_inventory.update(updates)
    context.session.add(db_inventory)
    context.session.flush()
    return db_inventory


@db_api.main_context_manager.writer
def _update_inventory_in_db(context, id_, updates):
    result = context.session.query(
        models.Inventory).filter_by(id=id_).update(updates)
    if not result:
        raise exception.NotFound()


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
    def destroy(self):
        self._destroy_in_db(self._context, self.id)

    @base.remotable
    def save(self):
        if 'id' not in self:
            raise exception.ObjectActionError(action='save',
                                              reason='not created')
        updates = self.obj_get_changes()
        updates.pop('id', None)
        self._update_in_db(self._context, self.id, updates)

    @staticmethod
    def _create_in_db(context, updates):
        return _create_inventory_in_db(context, updates)

    @staticmethod
    @db_api.main_context_manager.writer
    def _destroy_in_db(context, id):
        result = context.session.query(models.Inventory).filter_by(
            id=id).delete()
        if not result:
            raise exception.NotFound()

    @staticmethod
    def _update_in_db(context, id_, updates):
        return _update_inventory_in_db(context, id_, updates)


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
class ResourceUse(base.NovaObject):
    """A read only representation of resource usage."""
    # Version 1.0: Initial version
    VERSION = '1.0'

    fields = {
        'resource_class': fields.ResourceClassField(read_only=True),
        'total': fields.IntegerField(read_only=True),
        'reserved': fields.IntegerField(read_only=True),
        'min_unit': fields.IntegerField(read_only=True),
        'max_unit': fields.IntegerField(read_only=True),
        'step_size': fields.IntegerField(read_only=True),
        'allocation_ratio': fields.FloatField(read_only=True),
        'used': fields.IntegerField(read_only=True),
    }

    @property
    def available(self):
        """Convenience property to calculate availability."""
        return int(((self.total - self.reserved) * self.allocation_ratio) -
                   self.used)


@base.NovaObjectRegistry.register
class ResourceProviderList(base.ObjectListBase, base.NovaObject):
    # Version 1.0: Initial Version
    VERSION = '1.0'

    fields = {
        'objects': fields.ListOfObjectsField('ResourceProvider'),
    }

    @staticmethod
    @db_api.main_context_manager.reader
    def _get_all_from_db(context):
        query = (context.session.query(models.ResourceProvider,
                                       models.Aggregate)
                 .outerjoin(models.ComputeNode,
                            sql.and_(models.ComputeNode.uuid ==
                                     models.ResourceProvider.uuid,
                                     models.ResourceProvider.uuid is None))
                 .outerjoin(models.ResourceProviderAggregate,
                       models.ResourceProvider.id ==
                       models.ResourceProviderAggregate.resource_provider_id)
                 .join(models.Aggregate,
                       models.ResourceProviderAggregate.aggregate_id ==
                       models.Aggregate.id)
                 .group_by(
                     models.ResourceProviderAggregate.resource_provider_id))

        results = itertools.groupby(query.all(), lambda x: x[0])
        resource_providers = []
        for resource_provider, aggregates in results:
            aggregates = [aggregate[1] for aggregate in aggregates]
            resource_provider.aggregates = [
                objects.Aggregate._from_db_object(context, objects.Aggregate(),
                                                  aggregate)
                for aggregate in aggregates]
            resource_providers.append(resource_provider)
        return resource_providers

    @base.remotable_classmethod
    def get_all(cls, context):
        resource_providers = cls._get_all_from_db(context)
        return base.obj_make_list(context, cls(context),
                                  objects.ResourceProvider, resource_providers)
