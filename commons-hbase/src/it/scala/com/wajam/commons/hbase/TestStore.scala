package com.wajam.commons.hbase

import org.apache.hadoop.hbase.client.HBaseAdmin
import org.joda.time.DateTime
import org.json4s.ShortTypeHints

import com.wajam.commons.Closable
import com.wajam.commons.hbase.HBaseClient.{ HBaseKeyExtractor, WrappedResult }
import com.wajam.commons.hbase.HBaseSchema.{ Family, SnappyCompression, Table }

class TestSchema(namespace: String, admin: HBaseAdmin) extends HBaseSchema(admin) {

  val testCompoundTable = Table(namespace, "test_compound", List(Family("base", SnappyCompression)))
  val testMultiFamiliesTable = Table(namespace, "test_multi_families", List(Family("base", SnappyCompression), Family("extra", SnappyCompression)))
  val testPolymorphicTable = Table(namespace, "test_polymorphic", List(Family("base", SnappyCompression)))
  val testListTable = Table(namespace, "test_list", List(Family("base", SnappyCompression)))
  val testTimestampsTable = Table(namespace, "test_timestamps", List(Family("base", SnappyCompression)))

  def tables = List(
    testCompoundTable,
    testMultiFamiliesTable,
    testPolymorphicTable,
    testListTable,
    testTimestampsTable)

  def withExtra2Family: TestSchema = {
    val _testMultiFamiliesTable = testMultiFamiliesTable

    new TestSchema(namespace, admin) {
      override val testMultiFamiliesTable: Table = Table(_testMultiFamiliesTable.name,
        Family("extra2", SnappyCompression, deleteIfFamilyFieldMissing = false) :: _testMultiFamiliesTable.families)
    }
  }
}

class TestStore(hbaseClient: HBaseClient, schema: TestSchema) {

  import com.wajam.commons.hbase.TestStore._

  implicit val serializer = HBaseJsonSerializer(
    ShortTypeHints(List(classOf[PolymorphicEntity1], classOf[PolymorphicEntity2])), schema.tables)

  // CompoundEntity operations

  implicit object CompoundEntityKeyExtractor extends HBaseKeyExtractor[CompoundEntity] {
    override def key(v: CompoundEntity) = v.key.string
  }

  def getAllCompoundEntitiesFor(id1: Long): Iterator[CompoundEntity] with Closable = {
    import com.wajam.commons.hbase.KeyImplicits._

    val key = CompoundKey(id1, 0)
    hbaseClient.scanObjects[CompoundEntity](schema.testCompoundTable.name, key.min.string, key.max.string)
  }

  def saveCompoundEntity(entity: CompoundEntity): Unit = {
    hbaseClient.putObject(schema.testCompoundTable.name, entity)
  }

  def deleteCompoundEntity(id1: Long, id2: Int, id3: String): Unit = {
    val key = CompoundEntity(id1, id2, id3, value = "").key
    hbaseClient.delete(schema.testCompoundTable.name, key.string)
  }

  def deleteCompoundEntities(id1: Long, id2: Int): Unit = {
    val fake = CompoundEntity(id1, id2, id3 = "", value = "")
    hbaseClient.delete(schema.testCompoundTable.name, fake.scanFromKey.string, fake.scanToKey.string)
  }

  // MultiFamiliesEntity

  implicit object MultiFamiliesEntityKeyExtractor extends HBaseKeyExtractor[MultiFamiliesEntity] {
    override def key(v: MultiFamiliesEntity) = v.key.string
  }

  def getMultiFamiliesEntity(id: Long): Option[MultiFamiliesEntity] = {
    hbaseClient.getObject[MultiFamiliesEntity](schema.testMultiFamiliesTable.name, LongKey(id).string)
  }

  def getMultiFamiliesExtra(id: Long): Option[ExtraFamily] = {
    implicit val serializer = new HBaseJsonSerializer("extra")
    hbaseClient.getObject[ExtraFamily](schema.testMultiFamiliesTable.name, LongKey(id).string, List("extra"))
  }

  def getMultiFamiliesExtra2(id: Long): Option[ExtraFamily] = {
    implicit val serializer = new HBaseJsonSerializer("extra2")
    hbaseClient.getObject[ExtraFamily](schema.testMultiFamiliesTable.name, LongKey(id).string, List("extra2"))
  }

  def saveMultiFamiliesEntity(entity: MultiFamiliesEntity): Unit = {
    hbaseClient.putObject(schema.testMultiFamiliesTable.name, entity)
  }

  def saveMultiFamiliesBadEntity(entity: MultiFamiliesBadEntity): Unit = {
    hbaseClient.putObject(schema.testMultiFamiliesTable.name, entity.key.string, entity)
  }

  // PolymorphicEntity

  implicit object PolymorphicEntityKeyExtractor extends HBaseKeyExtractor[PolymorphicEntity] {
    override def key(v: PolymorphicEntity) = v.key.string
  }

  def getPolymorphicEntity(id: String): Option[PolymorphicEntity] = {
    hbaseClient.getObject[PolymorphicEntity](schema.testPolymorphicTable.name, StringKey(id).string)
  }

  def savePolymorphicEntity(entity: PolymorphicEntity): Unit = {
    hbaseClient.putObject(schema.testPolymorphicTable.name, entity)
  }

  // ListEntity

  def getListEntity(id: Long): Option[ListEntity] = {
    hbaseClient.getObject[ListEntity](schema.testListTable.name, HashKey(id).string)
  }

  def saveListEntity(entity: ListEntity): Unit = {
    hbaseClient.putObject(schema.testListTable.name, entity.key.string, entity)
  }

  // TimestampEntity

  def getTimestampEntity(id: String): Option[TimestampEntity] = {
    hbaseClient.get(schema.testTimestampsTable.name, id).map(resultToTimestampEntity)
  }

  def scanTimestampEntities: Iterator[TimestampEntity] with Closable = {
    val itr = hbaseClient.scan(schema.testTimestampsTable.name, StringKey.Min.string, StringKey.Max.string)
    new Iterator[TimestampEntity] with Closable {
      def hasNext: Boolean = itr.hasNext

      def next(): TimestampEntity = resultToTimestampEntity(itr.next())

      def close(): Unit = itr.close()
    }
  }

  def saveTimestampEntity(entity: TimestampEntity): Unit = {
    hbaseClient.putObject(schema.testTimestampsTable.name, entity.key.string, entity)
  }

  private def resultToTimestampEntity(result: WrappedResult)(implicit ser: HBaseSerializer): TimestampEntity = {
    val entity = ser.deserialize[TimestampEntity](schema.testTimestampsTable.name, result)
    entity.copy(updateTimestamp = Some(entity.updateTimestamp.getOrElse(result.getRowTimestamp)))
  }

}

object TestStore {

  case class CompoundEntity(id1: Long, id2: Int, id3: String, value: String) extends CompoundDataEntity {
    def key: CompoundKey = CompoundKey(LongKey(id1), LongKey(id2), StringKey(id3))
  }

  case class MultiFamiliesEntity(id: Long, value: String, extra: Option[ExtraFamily], extra2: Option[ExtraFamily] = None) extends UniqueDataEntity {
    def key: LongKey = id
  }

  // Unsupported extra column family i.e. extra is not a case class
  case class MultiFamiliesBadEntity(id: Long, value: String, extra: Option[String]) extends UniqueDataEntity {
    def key: LongKey = id
  }

  case class ExtraFamily(value1: String, value2: Long, value3: Option[String])

  trait PolymorphicEntity extends UniqueDataEntity {
    def id: String

    private val _key: StringKey = id

    def key: StringKey = _key
  }

  case class PolymorphicEntity1(id: String, stringValue: String) extends PolymorphicEntity

  case class PolymorphicEntity2(id: String, timeValue: Option[DateTime]) extends PolymorphicEntity

  case class ListEntity(id: Long, listValue: List[String], setValue: Set[Long], optionalListValue: Option[List[String]] = None) extends UniqueDataEntity {
    def key = HashKey(id)
  }

  case class TimestampEntity(id: String, value: String, updateTimestamp: Option[DateTime] = None) {
    def key = StringKey(id)
  }
}
