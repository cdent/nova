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

from nova import context
from nova import exception
from nova import objects
from nova.objects import fields
from nova import test
from nova.tests import fixtures
from nova.tests import uuidsentinel

DISK_INVENTORY = dict(
    total=200,
    reserved=10,
    min_unit=2,
    max_unit=5,
    step_size=1,
    allocation_ratio=1.0,
    resource_class=fields.ResourceClass.DISK_GB
)

DISK_ALLOCATION = dict(
    consumer_id=uuidsentinel.disk_consumer,
    used=2,
    resource_class=fields.ResourceClass.DISK_GB
)


class ResourceProviderTestCase(test.NoDBTestCase):
    """Test resource-provider objects' lifecycles."""

    USES_DB_SELF = True

    def setUp(self):
        super(ResourceProviderTestCase, self).setUp()
        self.useFixture(fixtures.Database())
        self.context = context.RequestContext('fake-user', 'fake-project')

    def test_create_resource_provider_requires_uuid(self):
        resource_provider = objects.ResourceProvider(
            context = self.context)
        self.assertRaises(exception.ObjectActionError,
                          resource_provider.create)

    def test_create_resource_provider(self):
        created_resource_provider = objects.ResourceProvider(
            context=self.context,
            uuid=uuidsentinel.fake_resource_provider,
            name=uuidsentinel.fake_resource_name
        )
        created_resource_provider.create()
        self.assertIsInstance(created_resource_provider.id, int)

        retrieved_resource_provider = objects.ResourceProvider.get_by_uuid(
            self.context,
            uuidsentinel.fake_resource_provider
        )
        self.assertEqual(retrieved_resource_provider.id,
                         created_resource_provider.id)
        self.assertEqual(retrieved_resource_provider.uuid,
                         created_resource_provider.uuid)
        self.assertEqual(retrieved_resource_provider.name,
                         created_resource_provider.name)

    def test_create_inventory_with_uncreated_provider(self):
        resource_provider = objects.ResourceProvider(
            context=self.context,
            uuid=uuidsentinel.inventory_resource_provider
        )
        disk_inventory = objects.Inventory(
            context=self.context,
            resource_provider=resource_provider,
            **DISK_INVENTORY
        )
        self.assertRaises(exception.ObjectActionError,
                          disk_inventory.create)

    def test_create_and_update_inventory(self):
        resource_provider = objects.ResourceProvider(
            context=self.context,
            uuid=uuidsentinel.inventory_resource_provider
        )
        resource_provider.create()
        resource_class = fields.ResourceClass.DISK_GB
        disk_inventory = objects.Inventory(
            context=self.context,
            resource_provider=resource_provider,
            **DISK_INVENTORY
        )
        disk_inventory.create()

        self.assertEqual(resource_class, disk_inventory.resource_class)
        self.assertEqual(resource_provider,
                         disk_inventory.resource_provider)
        self.assertEqual(DISK_INVENTORY['allocation_ratio'],
                         disk_inventory.allocation_ratio)
        self.assertEqual(DISK_INVENTORY['total'],
                         disk_inventory.total)

        disk_inventory.total = 32
        disk_inventory.save()

        inventories = objects.InventoryList.get_all_by_resource_provider_uuid(
            self.context, resource_provider.uuid)

        self.assertEqual(1, len(inventories))
        self.assertEqual(32, inventories[0].total)

        inventories[0].total = 33
        inventories[0].save()
        reloaded_inventories = (
            objects.InventoryList.get_all_by_resource_provider_uuid(
            self.context, resource_provider.uuid))
        self.assertEqual(33, reloaded_inventories[0].total)

    def test_create_list_and_delete_allocation(self):
        resource_provider = objects.ResourceProvider(
            context=self.context,
            uuid=uuidsentinel.allocation_resource_provider
        )
        resource_provider.create()
        resource_class = fields.ResourceClass.DISK_GB
        disk_allocation = objects.Allocation(
            context=self.context,
            resource_provider=resource_provider,
            **DISK_ALLOCATION
        )
        disk_allocation.create()

        self.assertEqual(resource_class, disk_allocation.resource_class)
        self.assertEqual(resource_provider,
                         disk_allocation.resource_provider)
        self.assertEqual(DISK_ALLOCATION['used'],
                         disk_allocation.used)
        self.assertEqual(DISK_ALLOCATION['consumer_id'],
                         disk_allocation.consumer_id)
        self.assertIsInstance(disk_allocation.id, int)

        allocations = objects.AllocationList.get_allocations(
            context=self.context,
            resource_provider=resource_provider,
            resource_class=resource_class
        )

        self.assertEqual(1, len(allocations))

        self.assertEqual(DISK_ALLOCATION['used'],
                        allocations[0].used)

        allocations[0].destroy()

        allocations = objects.AllocationList.get_allocations(
            context=self.context,
            resource_provider=resource_provider,
            resource_class=resource_class
        )

        self.assertEqual(0, len(allocations))

    def test_create_and_retrieve_resource_provider(self):
        aggregate_0 = objects.Aggregate(self.context,
                                        name=uuidsentinel.aggregate_0_name)
        aggregate_0.create()
        aggregate_1 = objects.Aggregate(self.context,
                                        name=uuidsentinel.aggregate_1_name)
        aggregate_1.create()
        aggregates = [aggregate_0, aggregate_1]
        rp = objects.ResourceProvider(self.context,
                                  name=uuidsentinel.resource_provider_name,
                                  uuid=uuidsentinel.resource_provider_uuid,
                                  aggregates=aggregates)
        rp.create()

        self.assertEqual(uuidsentinel.resource_provider_name,
                         rp.name)
        self.assertEqual(uuidsentinel.resource_provider_uuid,
                         rp.uuid)
        self.assertIn(aggregate_0, rp.aggregates)
        self.assertIn(aggregate_1, rp.aggregates)

        retrieved_rp = objects.ResourceProvider.get_by_uuid(
            self.context, uuidsentinel.resource_provider_uuid)

        self.assertEqual(uuidsentinel.resource_provider_name,
                         retrieved_rp.name)
        self.assertEqual(uuidsentinel.resource_provider_uuid,
                         retrieved_rp.uuid)
        self.assertEqual([], retrieved_rp.resources)

        retrieved_rp.aggregates.sort(key=lambda x: x.id)
        # NOTE(cdent): I was hoping objects themselves would be
        # comparable, but it seems not, so comparing contents.
        self.assertEqual(aggregate_0.uuid, retrieved_rp.aggregates[0].uuid)
        self.assertEqual(aggregate_1.uuid, retrieved_rp.aggregates[1].uuid)

        # try to create the same rp again
        rp = objects.ResourceProvider(self.context,
                                  name=uuidsentinel.resource_provider_name,
                                  uuid=uuidsentinel.resource_provider_uuid,
                                  aggregates=aggregates)
        self.assertRaises(exception.ObjectActionError, rp.create)

    def test_resource_provider_inventories(self):

        aggregate = objects.Aggregate(self.context, name='myagg')
        aggregate.create()
        aggregates = [aggregate]

        # Make a pool for that resource provider
        resource_provider = objects.ResourceProvider(
            self.context, uuid=uuidsentinel.resource_provider_uuid,
            name=uuidsentinel.resource_provider_name,
            aggregates=aggregates)
        resource_provider.create()

        # Create a disk inventory for that resource provider
        resource_class = fields.ResourceClass.DISK_GB
        disk_inventory = objects.Inventory(
            context=self.context,
            resource_provider=resource_provider,
            **DISK_INVENTORY
        )
        disk_inventory.create()

        # Allocate a usage.
        # NOTE(cdent): There's no guard at this point to limit what
        # we can allocate (the relationship with the Inventory is
        # not checked, yet).
        allocation = objects.Allocation(
            context=self.context,
            resource_provider=resource_provider,
            consumer_id=DISK_ALLOCATION['consumer_id'],
            resource_class=DISK_ALLOCATION['resource_class'],
            used=DISK_ALLOCATION['used']
        )
        allocation.create()

        inventoried_pool = (
            objects.ResourceProvider.get_by_uuid(
                self.context, uuidsentinel.resource_provider_uuid))

        disk_resource = inventoried_pool.resources[0]
        self.assertEqual(resource_class, disk_resource.resource_class)
        self.assertEqual(DISK_ALLOCATION['used'], disk_resource.used)
        self.assertEqual(DISK_INVENTORY['total'], disk_resource.total)
        self.assertEqual(DISK_INVENTORY['max_unit'],
                         disk_resource.max_unit)

        allocation = objects.Allocation(
            context=self.context,
            resource_provider=resource_provider,
            consumer_id=DISK_ALLOCATION['consumer_id'],
            resource_class=DISK_ALLOCATION['resource_class'],
            used=DISK_ALLOCATION['used']
        )
        allocation.create()

        inventoried_pool = (
            objects.ResourceProvider.get_by_uuid(
                self.context, uuidsentinel.resource_provider_uuid))
        disk_resource = inventoried_pool.resources[0]

        self.assertEqual(resource_class, disk_resource.resource_class)
        self.assertEqual(4, disk_resource.used)
        self.assertEqual(DISK_INVENTORY['total'], disk_resource.total)
        self.assertEqual(DISK_INVENTORY['max_unit'], disk_resource.max_unit)

        # Now kill it and its associates.
        inventoried_pool.destroy()

        self.assertRaises(exception.NotFound,
                          objects.ResourceProvider.get_by_uuid,
                          self.context,
                          uuidsentinel.resource_provider_uuid)


class ResourceProviderListTestCase(test.NoDBTestCase):
    """Test resource-pool lists objects' lifecycles."""

    USES_DB_SELF = True

    def setUp(self):
        super(ResourceProviderListTestCase, self).setUp()
        self.useFixture(fixtures.Database())
        self.context = context.RequestContext('fake-user', 'fake-project')

    def test_get_resource_providers(self):
        # Create two aggregate list
        aggregate_0 = objects.Aggregate(self.context,
                                        name=uuidsentinel.aggregate_0_name)
        aggregate_0.create()
        aggregate_list_0 = [aggregate_0]
        aggregate_1 = objects.Aggregate(self.context,
                                        name=uuidsentinel.aggregate_1_name)
        aggregate_1.create()
        aggregate_list_1 = [aggregate_0, aggregate_1]

        # Create two resource pools
        for rp_uuid, aggregate in [(uuidsentinel.resource_provider_one,
                                    aggregate_list_0),
                                   (uuidsentinel.resource_provider_two,
                                    aggregate_list_1)]:
            resource_provider = objects.ResourceProvider(
                self.context, name=rp_uuid, uuid=rp_uuid,
                aggregates=aggregate)
            resource_provider.create()

        # get both of them
        resource_providers = objects.ResourceProviderList.get_all(
            self.context)

        provider_uuids = [pool.uuid for pool in resource_providers]

        self.assertEqual(2, len(resource_providers))
        self.assertEqual(set([uuidsentinel.resource_provider_one,
                              uuidsentinel.resource_provider_two]),
                         set(provider_uuids))
