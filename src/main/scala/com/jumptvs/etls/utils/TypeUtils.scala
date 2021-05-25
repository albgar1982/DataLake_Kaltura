package com.jumptvs.etls.utils

object TypeUtils

object isDigit {
  private def checkValue(x : String): Boolean = x forall Character.isDigit
  def apply(x: String): Boolean = checkValue(x)
  def unapply(x: String): Boolean = checkValue(x)
}

object isLargerThanZero {
  def unapply(x: Int): Boolean = x > 0
}

object isEqualToZero {
  def unapply(x: Int): Boolean = x == 0
}

object isSmallerThanZero {
  def unapply(x: Int): Boolean = x < 0

}