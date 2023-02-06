package io.univalence.dataeng._06_kafka

/**
 * In-memory database.
 *
 * This database manages a set of key-value stores, named table.
 *
 * {{{
 *   val database = new Database
 *
 *   val store = database.getOrCreateTable[String]("store")
 *   store.put("123", "Jon")
 *   store.put("456", "Mary")
 *
 *   case class User(id: String, name: String, age: Int)
 *   val userStore = database.getOrCreate[User]("user")
 *   userStore.put("123", User("123", "Jon", 23))
 *   userStore.put("456", User("456", "Mary", 34))
 * }}}
 */
class Database {

  class Table[A](name: String) {
    val data: scala.collection.mutable.Map[String, A] = scala.collection.mutable.Map.empty

    /** Add or update on entry in the table. */
    def put(key: String, value: A): Unit = data.update(key, value)

    /** Get a value from a key or an exception. */
    def get(key: String): A = data(key)

    def getWithDefault(key: String, defaultValue: A): A = data.getOrElse(key, defaultValue)

    /** Get all data in the table. */
    def getAll: Map[String, A] = Map.from(data)

    def getPrefix(prefix: String): Map[String, A] = Map.from(data.filter { case (key, _) => key.startsWith(prefix) })

    /** remove an entry in the table. */
    def delete(key: String): Unit = data.remove(key)

    /** Check if a key is present in the table. */
    def hasKey(key: String): Boolean = data.keySet.contains(key)
  }

  val data: scala.collection.mutable.Map[String, Table[Any]] = scala.collection.mutable.Map.empty

  /** Get a table or create it if it does not exist. */
  def getOrCreateTable[A](name: String): Table[A] = {
    if (!data.contains(name))
      data.update(name, new Table(name))

    data(name).asInstanceOf[Table[A]]
  }

  def getTableNames: List[String] = data.keySet.toList

  def getTable[A](name: String): Option[Table[A]] = data.get(name).map(_.asInstanceOf[Table[A]])

  def getAll: Map[String, Map[String, Any]] = Map.from(data.map { case (name, table) => name -> table.getAll })

}
