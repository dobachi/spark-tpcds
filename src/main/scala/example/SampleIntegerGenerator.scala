package example

/**
  * Helper to generate fixed size of Integers.
  */
class SampleIntegerGenerator {
  /**
    * Generate sample data for test, which contains several integers
    *
    * @param size How many integers you want to generate
    * @return array of integers
    */
  def generate(size: Int): Array[Int] = {
    0 to size - 1 toArray
  }
}

object SampleIntegerGenerator {
  def apply(): SampleIntegerGenerator = new SampleIntegerGenerator()
}
