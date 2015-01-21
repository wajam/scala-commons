package com.wajam.commons.hbase

import com.github.nscala_time.time.Imports._
import org.scalatest.Matchers._
import org.scalatest.{ BeforeAndAfterAll, FlatSpec }

import com.wajam.commons.hbase.HBaseSchema.{ SnappyCompression, Family, Table }
import com.wajam.commons.hbase.TestStore._

class HBaseClientSpec extends FlatSpec with BeforeAndAfterAll {
  trait Setup {
    val store = TestContext.testStore
  }

  override protected def beforeAll(): Unit = {
    TestContext.hbaseSchema.reset()

    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
  }

  "Client" should "support CRUD operations on compound key entities" in new Setup {
    val e_0_0_a = CompoundEntity(0, 0, "a", value = "00a")
    val e_0_1_a = CompoundEntity(0, 1, "a", value = "01a")
    val e_0_1_b = CompoundEntity(0, 1, "b", value = "01b")
    val e_0_1_z = CompoundEntity(0, 1, "z", value = "01z")
    val e_1_0_a = CompoundEntity(1, 0, "z", value = "10z")

    // Verify store is empty
    store.getAllCompoundEntitiesFor(0).toList should be(Nil)
    store.getAllCompoundEntitiesFor(1).toList should be(Nil)

    // Add some entities
    store.saveCompoundEntity(e_0_1_a)
    store.saveCompoundEntity(e_0_1_b)
    store.getAllCompoundEntitiesFor(0).toList should be(List(e_0_1_a, e_0_1_b))
    store.getAllCompoundEntitiesFor(1).toList should be(Nil)

    // Add some more
    store.saveCompoundEntity(e_0_0_a)
    store.saveCompoundEntity(e_0_1_z)
    store.saveCompoundEntity(e_1_0_a)
    store.getAllCompoundEntitiesFor(0).toList should be(List(e_0_0_a, e_0_1_a, e_0_1_b, e_0_1_z))
    store.getAllCompoundEntitiesFor(1).toList should be(List(e_1_0_a))

    // Delete a single entity
    store.deleteCompoundEntity(0, 1, "b")
    store.getAllCompoundEntitiesFor(0).toList should be(List(e_0_0_a, e_0_1_a, e_0_1_z))
    store.getAllCompoundEntitiesFor(1).toList should be(List(e_1_0_a))

    // Delete all entities for the first 2 levels ids
    store.deleteCompoundEntities(0, 1)
    store.getAllCompoundEntitiesFor(0).toList should be(List(e_0_0_a))
    store.getAllCompoundEntitiesFor(1).toList should be(List(e_1_0_a))
  }

  it should "support extra families" in new Setup {
    val e_0 = MultiFamiliesEntity(0, value = "0", extra = Some(ExtraFamily("0", 0, Some("0"))))
    val e_1 = MultiFamiliesEntity(1, value = "1", extra = None)
    val e_2 = MultiFamiliesEntity(2, value = "2", extra = Some(ExtraFamily("2", 2, None)))

    // Verify store is empty
    store.getMultiFamiliesEntity(0) should be(None)
    store.getMultiFamiliesEntity(1) should be(None)
    store.getMultiFamiliesEntity(2) should be(None)

    // Save and fetch entities
    store.saveMultiFamiliesEntity(e_0)
    store.saveMultiFamiliesEntity(e_1)
    store.saveMultiFamiliesEntity(e_2)
    store.getMultiFamiliesEntity(0) should be(Some(e_0))
    store.getMultiFamiliesEntity(1) should be(Some(e_1))
    store.getMultiFamiliesEntity(2) should be(Some(e_2))

    // Overwrite e_0 removing a field from its extra family
    val e_0_no_extra_value3 = e_0.copy(extra = e_0.extra.map(_.copy(value3 = None)))
    store.saveMultiFamiliesEntity(e_0_no_extra_value3)
    store.getMultiFamiliesEntity(0) should be(Some(e_0_no_extra_value3))

    // Overwrite e_2 deleting its entire extra family
    val e_2_no_extra_family = e_2.copy(value = "22", extra = None)
    store.saveMultiFamiliesEntity(e_2_no_extra_family)
    store.getMultiFamiliesEntity(2) should be(Some(e_2_no_extra_family))

    // Fetch only extra families
    store.getMultiFamiliesExtra(0) should be(e_0_no_extra_value3.extra)
    store.getMultiFamiliesExtra(1) should be(None)
    store.getMultiFamiliesExtra(2) should be(None)
  }

  // TODO: move to a specific schema test suite?
  it should "support adding extra families dynamically" in new Setup {
    val e_0 = MultiFamiliesEntity(0, value = "0", extra = Some(ExtraFamily("0", 0, Some("0"))))
    val e_1 = MultiFamiliesEntity(1, value = "1", extra = None)
    val e_2 = MultiFamiliesEntity(2, value = "2", extra = Some(ExtraFamily("2", 2, None)))

    // Save and fetch entities
    store.saveMultiFamiliesEntity(e_0)
    store.saveMultiFamiliesEntity(e_1)
    store.saveMultiFamiliesEntity(e_2)
    store.getMultiFamiliesEntity(0) should be(Some(e_0))
    store.getMultiFamiliesEntity(1) should be(Some(e_1))
    store.getMultiFamiliesEntity(2) should be(Some(e_2))

    // Create new column family
    val newSchema = TestContext.hbaseSchema.withExtra2Family
    newSchema.create()
    val newStore = new TestStore(TestContext.hbaseClient, newSchema)

    // Overwrite e_0, cloning existing extra family to extra2
    val e_0_extra2 = e_0.copy(extra2 = Some(ExtraFamily("0_2", 0, Some("0_2"))))
    newStore.saveMultiFamiliesEntity(e_0_extra2)
    newStore.getMultiFamiliesEntity(0) should be(Some(e_0_extra2))

    // Save a new row with the new extra2 family values
    val e_3 = MultiFamiliesEntity(3, value = "3", extra = None, extra2 = Some(ExtraFamily("3", 3, None)))
    newStore.saveMultiFamiliesEntity(e_3)

    // Fetch only extra/extra2 families
    newStore.getMultiFamiliesExtra(0) should be(e_0_extra2.extra)
    newStore.getMultiFamiliesExtra2(0) should be(e_0_extra2.extra2)
    newStore.getMultiFamiliesExtra(1) should be(None)
    newStore.getMultiFamiliesExtra(2) should be(e_2.extra)
    newStore.getMultiFamiliesExtra(3) should be(e_3.extra)
    newStore.getMultiFamiliesExtra2(3) should be(e_3.extra2)
  }

  it should "fail to save extra families that are not case class" in new Setup {
    val e_666 = MultiFamiliesBadEntity(666, value = "0", extra = Some("evil"))

    // Try to save
    evaluating {
      store.saveMultiFamiliesBadEntity(e_666)
    } should produce[Exception]
  }

  it should "support polymorphic entities" in new Setup {
    val e_1 = PolymorphicEntity1("1", "1")
    val e_2 = PolymorphicEntity2("2", Some(DateTime.now))

    // Verify store is empty
    store.getPolymorphicEntity("1") should be(None)
    store.getPolymorphicEntity("2") should be(None)

    // Save and fetch entities
    store.savePolymorphicEntity(e_1)
    store.savePolymorphicEntity(e_2)
    store.getPolymorphicEntity("1") should be(Some(e_1))
    store.getPolymorphicEntity("2") should be(Some(e_2))

    // Remove time value
    val e_2_no_time = e_2.copy(timeValue = None)
    store.savePolymorphicEntity(e_2_no_time)
    store.getPolymorphicEntity("2") should be(Some(e_2_no_time))
  }

  it should "support collection fields in entities" in new Setup {
    val e_1 = ListEntity(1, Nil, Set.empty)
    val e_2 = ListEntity(2, List("v1", "v2"), Set(1, 2, 3), Some(List("o1")))

    // Verify store is empty
    store.getListEntity(1) should be(None)
    store.getListEntity(2) should be(None)

    // Save and fetch entities
    store.saveListEntity(e_1)
    store.saveListEntity(e_2)
    store.getListEntity(1) should be(Some(e_1))
    store.getListEntity(2) should be(Some(e_2))

    // Save with empty collections
    val e_2_empty_collections = e_2.copy(setValue = Set(), optionalListValue = Some(Nil))
    store.saveListEntity(e_2_empty_collections)
    store.getListEntity(2) should be(Some(e_2_empty_collections.copy(optionalListValue = None)))
  }

  it should "be able read timestamp" in new Setup {
    val e_1 = TimestampEntity("1", "v1")

    // Verify store is empty
    store.scanTimestampEntities.toList should be(Nil)

    store.saveTimestampEntity(e_1)
    val actual_v1 = store.getTimestampEntity("1").get
    Thread.sleep(100)
    store.saveTimestampEntity(e_1.copy(value = "v2"))
    val actual_v2 = store.getTimestampEntity("1").get

    actual_v2.value should be("v2")
    actual_v1.updateTimestamp should be < actual_v2.updateTimestamp
  }
}
