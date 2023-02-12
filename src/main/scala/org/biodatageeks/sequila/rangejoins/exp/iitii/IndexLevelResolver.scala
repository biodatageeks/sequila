package org.biodatageeks.sequila.rangejoins.exp.iitii

object IndexLevelResolver {
  def levelIndexForIndex(index: Int): Int = {
    ((index + 1) / (1 << LevelResolver.levelForIndex(index)) - 1) / 2
  }

  def indexForIndexLevelAndLevel(levelIndex: Int, level: Int): Int = {
    (1 << level) * (2 * levelIndex + 1) - 1
  }
}
