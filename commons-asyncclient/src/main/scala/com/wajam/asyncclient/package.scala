package com.wajam

import scala.concurrent.Future

package object asyncclient {
  type AsyncJsonResponse[A] = Future[TypedJsonResponse[A]]
}
