package com.redislabs.provider.redis.util

import scala.collection.{BuildFrom, IterableOps}

/**
 * @author The Viet Nguyen
 */
object CollectionUtils {

  implicit class RichCollection[A, Repr <: IterableOps[A, Iterable, Repr]](val xs: Repr) {

    def distinctBy[B, That](f: A => B)(implicit bf: BuildFrom[Repr, A, That]): That = {
      val builder = bf.newBuilder(xs)
      val iterator = xs.iterator
      var set = Set[B]()
      while (iterator.hasNext) {
        val element = iterator.next()
        val distinctField = f(element)
        if (!set(distinctField)) {
          set += distinctField
          builder += element
        }
      }
      builder.result()
    }
  }

}