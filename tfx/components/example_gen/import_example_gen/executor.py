# Copyright 2019 Google LLC. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Generic TFX ImportExampleGen executor."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import apache_beam as beam
import tensorflow as tf

from typing import Any, Dict, List, Text
from tfx.components.example_gen import base_example_gen_executor
from tfx.utils import types


@beam.ptransform_fn
@beam.typehints.with_input_types(beam.Pipeline)
@beam.typehints.with_output_types(tf.train.Example)
def _ImportExample(  # pylint: disable=invalid-name
    pipeline: beam.Pipeline,
    input_dict: Dict[Text, List[types.TfxArtifact]],
    exec_properties: Dict[Text, Any],  # pylint: disable=unused-argument
    split_pattern: Text) -> beam.pvalue.PCollection:
  """Read TFRecord files to PCollection of TF examples.

  Note that each input split will be transformed by this function separately.

  Args:
    pipeline: beam pipeline.
    input_dict: Input dict from input key to a list of Artifacts.
      - input_base: input dir that contains tf example data.
    exec_properties: A dict of execution properties.
    split_pattern: Split.pattern in Input config, glob relative file pattern
      that maps to input files with root directory given by input_base.

  Returns:
    PCollection of TF examples.
  """
  input_base_uri = types.get_single_uri(input_dict['input_base'])
  input_split_pattern = os.path.join(input_base_uri, split_pattern)
  tf.logging.info(
      'Reading input TFExample data {}.'.format(input_split_pattern))

  # TODO(jyzhao): profile input examples.
  return (pipeline
          # TODO(jyzhao): support multiple input format.
          | 'ReadFromTFRecord' >>
          beam.io.ReadFromTFRecord(file_pattern=input_split_pattern)
          # TODO(jyzhao): consider move serialization out of base example gen.
          | 'ToTFExample' >> beam.Map(tf.train.Example.FromString))


class Executor(base_example_gen_executor.BaseExampleGenExecutor):
  """Generic TFX import example gen executor."""

  def GetInputSourceToExamplePTransform(self) -> beam.PTransform:
    """Returns PTransform for importing TF examples."""
    return _ImportExample
