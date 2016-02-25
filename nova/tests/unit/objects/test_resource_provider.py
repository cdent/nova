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

import mock
import operator

from nova.db.sqlalchemy import api as db_api
from nova.db.sqlalchemy import models
from nova import exception
from nova import objects
from nova.tests.unit.objects import test_objects
from nova.tests import uuidsentinel as uuids


_RESOURCE_CLASS_NAME = 'DISK_GB'
_RESOURCE_CLASS_ID = 2
_RESOURCE_PROVIDER_ID = 1
_RESOURCE_PROVIDER_UUID = uuids.resource_provider
_RESOURCE_PROVIDER_NAME = uuids.resource_name
_RESOURCE_PROVIDER_DB = {
    'id': _RESOURCE_PROVIDER_ID,
    'uuid': _RESOURCE_PROVIDER_UUID,
    'name': _RESOURCE_PROVIDER_NAME,
    'aggregates': [],
    'resources': [],
}
_INVENTORY_ID = 2
_INVENTORY_DB = {
    'id': _INVENTORY_ID,
    'resource_provider_id': _RESOURCE_PROVIDER_ID,
    'resource_class_id': _RESOURCE_CLASS_ID,
    'total': 16,
    'reserved': 2,
    'min_unit': 1,
    'max_unit': 8,
    'step_size': 1,
    'allocation_ratio': 1.0,
}
_ALLOCATION_ID = 2
_ALLOCATION_DB = {
    'id': _ALLOCATION_ID,
    'resource_provider_id': _RESOURCE_PROVIDER_ID,
    'resource_class_id': _RESOURCE_CLASS_ID,
    'consumer_id': uuids.fake_instance,
    'used': 8,
}


class _TestResourceProviderNoDB(object):

    @mock.patch('nova.objects.ResourceProvider._get_by_uuid_from_db',
                return_value=_RESOURCE_PROVIDER_DB)
    def test_object_get_by_uuid(self, mock_db_get):
        resource_provider_object = objects.ResourceProvider.get_by_uuid(
            mock.sentinel.ctx, _RESOURCE_PROVIDER_UUID)
        self.assertEqual(_RESOURCE_PROVIDER_ID, resource_provider_object.id)
        self.assertEqual(_RESOURCE_PROVIDER_UUID,
                         resource_provider_object.uuid)

    @mock.patch('nova.objects.ResourceProvider._create_in_db',
                return_value=_RESOURCE_PROVIDER_DB)
    def test_create(self, mock_db_create):
        obj = objects.ResourceProvider(context=self.context,
                                       uuid=_RESOURCE_PROVIDER_UUID)
        obj.create()
        self.assertEqual(_RESOURCE_PROVIDER_UUID, obj.uuid)
        self.assertIsInstance(obj.id, int)
        mock_db_create.assert_called_once_with(
            self.context, {'uuid': _RESOURCE_PROVIDER_UUID})

    def test_create_id_fail(self):
        obj = objects.ResourceProvider(context=self.context,
                                       uuid=_RESOURCE_PROVIDER_UUID,
                                       id=_RESOURCE_PROVIDER_ID)
        self.assertRaises(exception.ObjectActionError,
                          obj.create)

    def test_create_no_uuid_fail(self):
        obj = objects.ResourceProvider(context=self.context)
        self.assertRaises(exception.ObjectActionError,
                          obj.create)

    def test_make_compatible_10(self):
        rp_obj = objects.ResourceProvider(context=self.context,
                                       uuid=_RESOURCE_PROVIDER_UUID,
                                       name=_RESOURCE_PROVIDER_NAME)
        self.assertEqual(_RESOURCE_PROVIDER_NAME, rp_obj.name)
        primitive = rp_obj.obj_to_primitive(target_version='1.0')
        self.assertNotIn('name', primitive)


class TestResourceProviderNoDB(test_objects._LocalTest,
                               _TestResourceProviderNoDB):
    USES_DB = False


class TestRemoteResourceProviderNoDB(test_objects._RemoteTest,
                                     _TestResourceProviderNoDB):
    USES_DB = False


class TestResourceProvider(test_objects._LocalTest):

    def _make_aggregate(self, name):
        aggregate = objects.Aggregate(self.context, name=name)
        aggregate.create()
        return aggregate

    def _make_resource_provider(self, name=None, uuid=None, aggregates=None,
                                create=True):
        if not name:
            name = uuids.rp_name
        if not uuid:
            uuid = uuids.rp_uuid

        if not aggregates:
            aggregates = [self._make_aggregate(name=uuids.ag_name)]

        resource_provider = objects.ResourceProvider(
            self.context, name=name, uuid=uuid,
            aggregates=aggregates)
        if create:
            resource_provider.create()
        return resource_provider

    @staticmethod
    @db_api.main_context_manager.reader
    def _read_rp_collaborators(context, model, rp_id):
        return (context.session.query(model).
                filter_by(resource_provider_id=rp_id).all())

    def test_create_in_db(self):
        updates = {'uuid': _RESOURCE_PROVIDER_UUID,
                   'name': _RESOURCE_PROVIDER_NAME}
        db_rp = objects.ResourceProvider._create_in_db(
            self.context, updates)
        self.assertIsInstance(db_rp.id, int)
        self.assertEqual(_RESOURCE_PROVIDER_UUID, db_rp.uuid)
        self.assertEqual(_RESOURCE_PROVIDER_NAME, db_rp.name)

    def test_duplicate_provider(self):
        resource_provider = objects.ResourceProvider(
            self.context, name=uuids.rp_name,
            uuid=uuids.rp_uuid)
        resource_provider.create()
        resource_provider = objects.ResourceProvider(
            self.context, name=uuids.rp_name,
            uuid=uuids.rp_uuid)
        error = self.assertRaises(exception.ObjectActionError,
                                  resource_provider.create)
        self.assertIn('duplicate resource provider', str(error))

    def test_make_compatible_11(self):
        rp_obj = objects.ResourceProvider(context=self.context,
                                       uuid=_RESOURCE_PROVIDER_UUID,
                                       name=_RESOURCE_PROVIDER_NAME)
        rp_obj.create()

        self.assertEqual(_RESOURCE_PROVIDER_NAME, rp_obj.name)
        self.assertEqual([], rp_obj.aggregates)
        self.assertEqual([], rp_obj.resources)
        primitive = rp_obj.obj_to_primitive(target_version='1.1')
        self.assertNotIn('aggregates', primitive)
        self.assertNotIn('resources', primitive)

    def test_get_by_uuid_from_db(self):
        rp = objects.ResourceProvider(context=self.context,
                                      uuid=_RESOURCE_PROVIDER_UUID,
                                      name=_RESOURCE_PROVIDER_NAME)
        rp.create()
        retrieved_rp = objects.ResourceProvider._get_by_uuid_from_db(
            self.context, _RESOURCE_PROVIDER_UUID)
        self.assertEqual(rp.uuid, retrieved_rp.uuid)
        self.assertEqual(rp.name, retrieved_rp.name)

    def test_make_resource_provider(self):
        resource_provider = self._make_resource_provider()
        self.assertEqual(uuids.rp_name,
                         resource_provider.name)
        self.assertEqual(uuids.ag_name,
                         resource_provider.aggregates[0].name)
        self.assertEqual(0, len(resource_provider.resources))

    def test_resource_allocations_by_resources(self):
        resource_provider = self._make_resource_provider(create=False)
        resource = objects.ResourceUse(
            self.context,
            resource_class='DISK_GB',
            total=2048,
            reserved=100,
            allocation_ratio=1.1,
            min_unit=1,
            max_unit=100,
            step_size=1,
        )
        resource_provider.resources = [resource]
        resource_provider.create()

        allocation = objects.Allocation(
            self.context,
            resource_provider=resource_provider,
            resource_class='DISK_GB',
            consumer_id=uuids.disk_consumer,
            used=8
        )
        allocation.create()

        resource_provider = objects.ResourceProvider.get_by_uuid(self.context,
                                                         uuids.rp_uuid)
        resources = resource_provider.resources
        self.assertEqual(2048, resources[0].total)
        self.assertEqual(8, resources[0].used)
        self.assertEqual(2134, resources[0].available)

    def test_resource_allocations_by_inventory(self):
        resource_provider = self._make_resource_provider()
        inventory = objects.Inventory(
            self.context,
            resource_provider=resource_provider,
            resource_class='DISK_GB',
            total=2048,
            reserved=100,
            allocation_ratio=1.1,
            min_unit=1,
            max_unit=100,
            step_size=1,
        )
        inventory.create()
        resources = resource_provider.resources
        self.assertEqual(1, len(resources))
        self.assertEqual(2048, resources[0].total)
        self.assertEqual(0, resources[0].used)
        self.assertEqual(2142, resources[0].available)

        allocation = objects.Allocation(
            self.context,
            resource_provider=resource_provider,
            resource_class='DISK_GB',
            consumer_id=uuids.disk_consumer,
            used=8
        )
        allocation.create()

        resource_provider = objects.ResourceProvider.get_by_uuid(self.context,
                                                         uuids.rp_uuid)
        resources = resource_provider.resources
        self.assertEqual(2048, resources[0].total)
        self.assertEqual(8, resources[0].used)
        self.assertEqual(2134, resources[0].available)

    def test_mulitple_resource_classes(self):
        resource_provider = self._make_resource_provider(
            name=uuids.rp_name, uuid=uuids.rp_uuid)
        inventory_disk = objects.Inventory(
            self.context,
            resource_provider=resource_provider,
            resource_class='DISK_GB',
            total=2048,
            reserved=100,
            allocation_ratio=1.0,
            min_unit=1,
            max_unit=100,
            step_size=1
        )
        inventory_disk.create()
        inventory_address = objects.Inventory(
            self.context,
            resource_provider=resource_provider,
            resource_class='IPV4_ADDRESS',
            total=253,
            reserved=3,
            allocation_ratio=1.0,
            min_unit=1,
            max_unit=4,
            step_size=1
        )
        inventory_address.create()

        resource_provider = objects.ResourceProvider.get_by_uuid(self.context,
                                                         uuids.rp_uuid)
        resources = resource_provider.resources
        resources = sorted(resources,
                           key=operator.attrgetter('resource_class'))

        self.assertEqual(0, resources[0].used)
        self.assertEqual('DISK_GB', resources[0].resource_class)
        self.assertEqual(0, resources[1].used)
        self.assertEqual('IPV4_ADDRESS', resources[1].resource_class)

        allocation = objects.Allocation(
            self.context,
            resource_provider=resource_provider,
            resource_class='DISK_GB',
            consumer_id=uuids.disk_consumer,
            used=8
        )
        allocation.create()

        resource_provider = objects.ResourceProvider.get_by_uuid(self.context,
                                                         uuids.rp_uuid)
        resources = sorted(resource_provider.resources,
                           key=operator.attrgetter('resource_class'))

        self.assertEqual(8, resources[0].used)
        self.assertEqual('DISK_GB', resources[0].resource_class)
        self.assertEqual(0, resources[1].used)
        self.assertEqual('IPV4_ADDRESS', resources[1].resource_class)

        # Destroy this complex resource_provider
        rp_id = resource_provider.id
        # First attempt fails because we have allocations
        error = self.assertRaises(exception.ObjectActionError,
                                  resource_provider.destroy)
        self.assertIn('resources still in use', str(error))

        # Destory all the pending allocations.
        # TODO(cdent): Meh, cumbersome.
        for resource in resource_provider.resources:
            resource_class = resource.resource_class
            for allocation in objects.AllocationList.get_allocations(
                    self.context, resource_provider, resource_class):
                allocation.destroy()

        # Second attempt good
        resource_provider.destroy()
        self.assertRaises(exception.NotFound,
                          objects.ResourceProvider.get_by_uuid,
                          self.context,
                          uuids.rp_uuid)

        allocations = self._read_rp_collaborators(
            self.context, models.Allocation, rp_id)
        self.assertEqual([], allocations)

        associated_aggregates = self._read_rp_collaborators(
            self.context, models.ResourceProviderAggregate, rp_id)
        self.assertEqual([], associated_aggregates)

        inventories = self._read_rp_collaborators(
            self.context, models.Inventory, rp_id)
        self.assertEqual([], inventories)

        # Try to destroy again
        self.assertRaises(exception.NotFound, resource_provider.destroy)

    def test_aggregate_already_associated(self):
        aggregate = self._make_aggregate(uuids.agg_name)

        resource_provider_one = self._make_resource_provider(
            name=uuids.rp_one, uuid=uuids.rp_one,
            aggregates=[aggregate])

        self.assertEqual(uuids.agg_name,
                         resource_provider_one.aggregates[0].name)

        resource_provider_two = self._make_resource_provider(
            name=uuids.rp_two, uuid=uuids.rp_two,
            aggregates=[aggregate])

        self.assertEqual(uuids.agg_name,
                         resource_provider_two.aggregates[0].name)

        # Use the same aggregate twice
        resource_provider_three = self._make_resource_provider(
            name=uuids.rp_three, uuid=uuids.rp_three,
            aggregates=[aggregate, aggregate], create=False)
        error = self.assertRaises(exception.ObjectActionError,
                                  resource_provider_three.create)
        self.assertIn('aggregate already associated', str(error))

        self.assertRaises(exception.NotFound,
                          objects.ResourceProvider.get_by_uuid,
                          self.context,
                          uuids.rp_three)

    def test_associate_aggregate_to_pool(self):
        aggregate = self._make_aggregate(uuids.agg_name_one)

        resource_provider = self._make_resource_provider(
            name=uuids.rp_one, uuid=uuids.rp_one,
            aggregates=[aggregate])

        self.assertEqual(1, len(resource_provider.aggregates))
        self.assertEqual(uuids.agg_name_one,
                         resource_provider.aggregates[0].name)

        aggregate = self._make_aggregate(uuids.agg_name_two)

        aggregates = resource_provider.aggregates
        aggregates.append(aggregate)
        resource_provider.aggregates = aggregates
        resource_provider.save()

        self.assertEqual(2, len(resource_provider.aggregates))
        names = [agg.name for agg in resource_provider.aggregates]
        self.assertIn(uuids.agg_name_two, names)

        # Associate the aggregate twice
        aggregates = resource_provider.aggregates
        aggregates.append(aggregate)
        aggregates.append(aggregate)
        resource_provider.aggregates = aggregates
        error = self.assertRaises(exception.ObjectActionError,
                                  resource_provider.save)
        self.assertIn('aggregate already associated', str(error))

        # Get the pool from db to confirm storage
        resource_provider = (
            objects.ResourceProvider.get_by_uuid(
                self.context,
                uuids.rp_one))

        self.assertEqual(2, len(resource_provider.aggregates))
        names = [agg.name for agg in resource_provider.aggregates]
        self.assertIn(uuids.agg_name_two, names)

        # remove the aggregate
        resource_provider.aggregates = [
            agg for agg in resource_provider.aggregates
            if agg.uuid != aggregate.uuid]
        resource_provider.save()


        self.assertEqual(1, len(resource_provider.aggregates))
        names = [agg.name for agg in resource_provider.aggregates]
        self.assertNotIn(uuids.agg_name_two, names)

        # Get the pool from db to confirm removal
        resource_provider = (
            objects.ResourceProvider.get_by_uuid(
                self.context,
                uuids.rp_one))

        self.assertEqual(1, len(resource_provider.aggregates))
        names = [agg.name for agg in resource_provider.aggregates]
        self.assertNotIn(uuids.agg_name_two, names)

    def test_update_resources(self):
        resource_provider = self._make_resource_provider()

        resource_classes = ['DISK_GB', 'IPV4_ADDRESS']
        resources = []
        for resource_class in resource_classes:
            resource = objects.ResourceUse(
                self.context,
                resource_class=resource_class,
                total=2048,
                reserved=100,
                allocation_ratio=1.1,
                min_unit=1,
                max_unit=100,
                step_size=1,
            )
            resources.append(resource)
        resource_provider.resources = resources
        resource_provider.save()

        self.assertEqual(2, len(resource_provider.resources))
        ordered_resource_classes = [
            resource.resource_class for resource in
            sorted(resource_provider.resources,
                   key=operator.attrgetter('resource_class'))]
        self.assertEqual(resource_classes, ordered_resource_classes)

        rp = objects.ResourceProvider.get_by_uuid(self.context,
                                                  resource_provider.uuid)
        self.assertEqual(2, len(rp.resources))
        ordered_resource_classes = [
            resource.resource_class for resource in
            sorted(rp.resources, key=operator.attrgetter('resource_class'))]
        self.assertEqual(resource_classes, ordered_resource_classes)

        # Check Inventory
        inventories = objects.InventoryList.\
            get_all_by_resource_provider_uuid(self.context,
                                              resource_provider.uuid)
        self.assertEqual(2, len(inventories))
        ordered_resource_classes = [
            inventory.resource_class for inventory in
            sorted(inventories, key=operator.attrgetter('resource_class'))]
        self.assertEqual(resource_classes, ordered_resource_classes)

        resource = objects.ResourceUse(
            self.context,
            resource_class='MEMORY_MB',
            total=2048,
            reserved=100,
            allocation_ratio=1.1,
            min_unit=1,
            max_unit=100,
            step_size=1,
        )
        resource_provider.resources = [resource]
        resource_provider.save()
        self.assertEqual(1, len(resource_provider.resources))
        self.assertEqual('MEMORY_MB',
                         resource_provider.resources[0].resource_class)

        # Reload resource provider to check resources
        rp = objects.ResourceProvider.get_by_uuid(self.context,
                                                  resource_provider.uuid)
        self.assertEqual(1, len(rp.resources))
        self.assertEqual('MEMORY_MB',
                         rp.resources[0].resource_class)

        # Check that all associated inventories have been properly
        # destroyed.
        inventories = objects.InventoryList.\
            get_all_by_resource_provider_uuid(self.context,
                                              rp.uuid)

        self.assertEqual(1, len(inventories))
        self.assertEqual('MEMORY_MB', inventories[0].resource_class)

    def test_get_pool_by_uuid(self):
        resource_provider = self._make_resource_provider()

        retrieved_resource_provider = (
            objects.ResourceProvider.get_by_uuid(
                self.context,
                uuids.rp_uuid))

        self.assertEqual(uuids.rp_uuid,
                         resource_provider.uuid)
        self.assertEqual(uuids.rp_uuid,
                         retrieved_resource_provider.uuid)

    def test_resource_provider_list_get_all(self):
        aggregate = self._make_aggregate(uuids.agg_name)

        self._make_resource_provider(name='alpha', uuid=uuids.rp_one,
            aggregates=[aggregate])
        self._make_resource_provider(name='beta', uuid=uuids.rp_two,
            aggregates=[aggregate])

        resource_providers = objects.ResourceProviderList.get_all(self.context)
        resource_providers = sorted(resource_providers,
                                key=lambda x: x.name)
        self.assertEqual(2, len(resource_providers))
        self.assertEqual(uuids.rp_one,
                         resource_providers[0].uuid)
        self.assertEqual(uuids.rp_two,
                         resource_providers[1].uuid)

    def test_resource_provider_list_empty(self):
        resource_providers = objects.ResourceProviderList.get_all(self.context)
        self.assertEqual(0, len(resource_providers))


class _TestInventoryNoDB(object):
    @mock.patch('nova.objects.Inventory._create_in_db',
                return_value=_INVENTORY_DB)
    def test_create(self, mock_db_create):
        rp = objects.ResourceProvider(id=_RESOURCE_PROVIDER_ID,
                                      uuid=_RESOURCE_PROVIDER_UUID)
        obj = objects.Inventory(context=self.context,
                                resource_provider=rp,
                                resource_class=_RESOURCE_CLASS_NAME,
                                total=16,
                                reserved=2,
                                min_unit=1,
                                max_unit=8,
                                step_size=1,
                                allocation_ratio=1.0)
        obj.create()
        self.assertEqual(_INVENTORY_ID, obj.id)
        expected = dict(_INVENTORY_DB)
        expected.pop('id')
        mock_db_create.assert_called_once_with(self.context, expected)

    @mock.patch('nova.objects.Inventory._update_in_db',
                return_value=_INVENTORY_DB)
    def test_save(self, mock_db_save):
        obj = objects.Inventory(context=self.context,
                                id=_INVENTORY_ID,
                                reserved=4)
        obj.save()
        mock_db_save.assert_called_once_with(self.context,
                                             _INVENTORY_ID,
                                             {'reserved': 4})

    @mock.patch('nova.objects.InventoryList._get_all_by_resource_provider')
    def test_get_all_by_resource_provider(self, mock_get):
        expected = [dict(_INVENTORY_DB,
                         resource_provider=dict(_RESOURCE_PROVIDER_DB)),
                    dict(_INVENTORY_DB,
                         id=_INVENTORY_DB['id'] + 1,
                         resource_provider=dict(_RESOURCE_PROVIDER_DB))]
        mock_get.return_value = expected
        objs = objects.InventoryList.get_all_by_resource_provider_uuid(
            self.context, _RESOURCE_PROVIDER_DB['uuid'])
        self.assertEqual(2, len(objs))
        self.assertEqual(_INVENTORY_DB['id'], objs[0].id)
        self.assertEqual(_INVENTORY_DB['id'] + 1, objs[1].id)


class TestInventoryNoDB(test_objects._LocalTest,
                        _TestInventoryNoDB):
    USES_DB = False


class TestRemoteInventoryNoDB(test_objects._RemoteTest,
                              _TestInventoryNoDB):
    USES_DB = False


class TestInventory(test_objects._LocalTest):

    def _make_inventory(self):
        db_rp = objects.ResourceProvider(
            context=self.context, uuid=uuids.inventory_resource_provider)
        db_rp.create()
        updates = dict(_INVENTORY_DB,
                       resource_provider_id=db_rp.id)
        updates.pop('id')
        db_inventory = objects.Inventory._create_in_db(
            self.context, updates)
        return db_rp, db_inventory

    def test_create_in_db(self):
        updates = dict(_INVENTORY_DB)
        updates.pop('id')
        db_inventory = objects.Inventory._create_in_db(
            self.context, updates)
        self.assertEqual(_INVENTORY_DB['total'], db_inventory.total)

    def test_update_in_db(self):
        db_rp, db_inventory = self._make_inventory()
        objects.Inventory._update_in_db(self.context,
                                        db_inventory.id,
                                        {'total': 32})
        inventories = objects.InventoryList.\
            get_all_by_resource_provider_uuid(self.context, db_rp.uuid)
        self.assertEqual(32, inventories[0].total)

    def test_update_in_db_fails_bad_id(self):
        db_rp, db_inventory = self._make_inventory()
        self.assertRaises(exception.NotFound,
                          objects.Inventory._update_in_db,
                          self.context, 99, {'total': 32})

    def test_get_all_by_resource_provider_uuid(self):
        db_rp, db_inventory = self._make_inventory()

        retrieved_inventories = (
            objects.InventoryList._get_all_by_resource_provider(
                self.context, db_rp.uuid)
        )

        self.assertEqual(1, len(retrieved_inventories))
        self.assertEqual(db_inventory.id, retrieved_inventories[0].id)
        self.assertEqual(db_inventory.total, retrieved_inventories[0].total)

        retrieved_inventories = (
            objects.InventoryList._get_all_by_resource_provider(
                self.context, uuids.bad_rp_uuid)
        )
        self.assertEqual(0, len(retrieved_inventories))

    def test_create_requires_resource_provider(self):
        inventory_dict = dict(_INVENTORY_DB)
        inventory_dict.pop('id')
        inventory_dict.pop('resource_provider_id')
        inventory_dict.pop('resource_class_id')
        inventory_dict['resource_class'] = _RESOURCE_CLASS_NAME
        inventory = objects.Inventory(context=self.context,
                                      **inventory_dict)
        error = self.assertRaises(exception.ObjectActionError,
                                  inventory.create)
        self.assertIn('resource_provider required', str(error))

    def test_create_requires_created_resource_provider(self):
        rp = objects.ResourceProvider(
            context=self.context, uuid=uuids.inventory_resource_provider)
        inventory_dict = dict(_INVENTORY_DB)
        inventory_dict.pop('id')
        inventory_dict.pop('resource_provider_id')
        inventory_dict.pop('resource_class_id')
        inventory_dict['resource_provider'] = rp
        inventory = objects.Inventory(context=self.context,
                                      **inventory_dict)
        error = self.assertRaises(exception.ObjectActionError,
                                  inventory.create)
        self.assertIn('resource_provider required', str(error))

    def test_create_requires_resource_class(self):
        rp = objects.ResourceProvider(
            context=self.context, uuid=uuids.inventory_resource_provider)
        rp.create()
        inventory_dict = dict(_INVENTORY_DB)
        inventory_dict.pop('id')
        inventory_dict.pop('resource_provider_id')
        inventory_dict.pop('resource_class_id')
        inventory_dict['resource_provider'] = rp
        inventory = objects.Inventory(context=self.context,
                                      **inventory_dict)
        error = self.assertRaises(exception.ObjectActionError,
                                  inventory.create)
        self.assertIn('resource_class required', str(error))

    def test_create_id_fails(self):
        inventory = objects.Inventory(self.context, **_INVENTORY_DB)
        self.assertRaises(exception.ObjectActionError, inventory.create)

    def test_save_without_id_fails(self):
        inventory_dict = dict(_INVENTORY_DB)
        inventory_dict.pop('id')
        inventory = objects.Inventory(self.context, **inventory_dict)
        self.assertRaises(exception.ObjectActionError, inventory.save)


class _TestAllocationNoDB(object):
    @mock.patch('nova.objects.Allocation._create_in_db',
                return_value=_ALLOCATION_DB)
    def test_create(self, mock_db_create):
        rp = objects.ResourceProvider(id=_RESOURCE_PROVIDER_ID,
                                      uuid=uuids.resource_provider)
        obj = objects.Allocation(context=self.context,
                                 resource_provider=rp,
                                 resource_class=_RESOURCE_CLASS_NAME,
                                 consumer_id=uuids.fake_instance,
                                 used=8)
        obj.create()
        self.assertEqual(_ALLOCATION_ID, obj.id)
        expected = dict(_ALLOCATION_DB)
        expected.pop('id')
        mock_db_create.assert_called_once_with(self.context, expected)

    def test_create_with_id_fails(self):
        rp = objects.ResourceProvider(id=_RESOURCE_PROVIDER_ID,
                                      uuid=uuids.resource_provider)
        obj = objects.Allocation(context=self.context,
                                 id=99,
                                 resource_provider=rp,
                                 resource_class=_RESOURCE_CLASS_NAME,
                                 consumer_id=uuids.fake_instance,
                                 used=8)
        self.assertRaises(exception.ObjectActionError, obj.create)


class TestAllocationNoDB(test_objects._LocalTest,
                         _TestAllocationNoDB):
    USES_DB = False


class TestRemoteAllocationNoDB(test_objects._RemoteTest,
                               _TestAllocationNoDB):
    USES_DB = False


class TestAllocation(test_objects._LocalTest):

    def _make_allocation(self):
        db_rp = objects.ResourceProvider(
            context=self.context, uuid=uuids.allocation_resource_provider)
        db_rp.create()
        updates = dict(_ALLOCATION_DB,
                       resource_provider_id=db_rp.id)
        updates.pop('id')
        db_allocation = objects.Allocation._create_in_db(self.context,
                                                         updates)
        return db_rp, db_allocation

    def test_create_in_db(self):
        updates = dict(_ALLOCATION_DB)
        updates.pop('id')
        db_allocation = objects.Allocation._create_in_db(
            self.context, updates)
        self.assertEqual(_ALLOCATION_DB['used'], db_allocation.used)
        self.assertIsInstance(db_allocation.id, int)

    def test_destroy(self):
        db_rp, db_allocation = self._make_allocation()
        allocations = objects.AllocationList.get_allocations(
            self.context, db_rp, _RESOURCE_CLASS_NAME)
        self.assertEqual(1, len(allocations))
        objects.Allocation._destroy(self.context, db_allocation.id)
        allocations = objects.AllocationList.get_allocations(
            self.context, db_rp, _RESOURCE_CLASS_NAME)
        self.assertEqual(0, len(allocations))
        self.assertRaises(exception.NotFound, objects.Allocation._destroy,
                          self.context, db_allocation.id)

    def test_get_allocations_from_db(self):
        db_rp, db_allocation = self._make_allocation()
        allocations = objects.AllocationList._get_allocations_from_db(
            self.context, db_rp.id, _RESOURCE_CLASS_ID)
        self.assertEqual(1, len(allocations))
        self.assertEqual(db_rp.id, allocations[0].resource_provider_id)
        self.assertEqual(db_allocation.resource_provider_id,
                         allocations[0].resource_provider_id)

        allocations = objects.AllocationList._get_allocations_from_db(
            self.context, uuids.bad_rp_uuid, _RESOURCE_CLASS_ID)
        self.assertEqual(0, len(allocations))
