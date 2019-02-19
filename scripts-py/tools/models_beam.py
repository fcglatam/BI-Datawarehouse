import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

p = beam.Pipeline(options=PipelineOptions())


class MyOptions(PipelineOptions):
  
  @classmethod
  def _add_argparse_args(self, parser):
    parser.add_argument('--input')
    parser.add_argument('--output')
    
    