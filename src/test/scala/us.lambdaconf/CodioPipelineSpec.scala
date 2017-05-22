package us.lambdaconf

import com.spotify.scio.{ScioContext, ScioResult}
import com.spotify.scio.testing.PipelineSpec
import org.apache.beam.runners.direct.DirectOptions

class CodioPipelineSpec extends PipelineSpec {
  override def runWithContext[T](fn: ScioContext => T): ScioResult = {
    super.runWithContext { sc =>
      sc.optionsAs[DirectOptions].setTargetParallelism(1) // otherwise we don't have enough CPU cores
      fn(sc)
    }
  }
}
