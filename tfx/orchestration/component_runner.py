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
"""For component execution, includes driver, executor and publisher."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import abc
import os
from six import with_metaclass
from typing import Any, Dict, Text
from ml_metadata.proto import metadata_store_pb2
from tfx.components.base import base_component
from tfx.components.base import base_executor


class ComponentRunner(with_metaclass(abc.ABCMeta, object)):
  """Runner for component."""

  # TODO(jyzhao): consider provide another spec layer for the params.
  def __init__(
      self,
      component: base_component.BaseComponent,
      pipeline_run_id: Text,
      pipeline_name: Text,
      pipeline_root: Text,
      metadata_db_root: Text = None,
      metadata_connection_config: metadata_store_pb2.ConnectionConfig = None,
      enable_cache: bool = False,
      additional_pipeline_args: Dict[Text, Any] = None):
    self._pipeline_run_id = pipeline_run_id
    self._executor_class = component.executor

    self._input_dict = dict(
        (k, list(v.get())) for k, v in component.input_dict.items())
    self._output_dict = dict(
        (k, list(v.get())) for k, v in component.outputs.get_all().items())
    self._exec_properties = component.exec_properties

    self._project_path = os.path.join(pipeline_root, pipeline_name)
    self._additional_pipeline_args = additional_pipeline_args or {}

  def _run_driver(self) -> bool:
    """Prepare inputs, outputs and execution properties for actual execution."""
    # TODO(jyzhao): support driver after go/tfx-oss-artifact-passing.
    return False

  def _run_executor(self) -> None:
    """Execute underlying component implementation."""
    tmp_root_dir = self._additional_pipeline_args.get(
        'tmp_dir') or os.path.join(self._project_path, '.temp', '')

    executor_context = base_executor.BaseExecutor.Context(
        beam_pipeline_args=self._additional_pipeline_args.get(
            'beam_pipeline_args'),
        tmp_dir=tmp_root_dir,
        # TODO(jyzhao): change to execution id that generated in driver.
        unique_id=self._pipeline_run_id)

    # Type hint of component will cause not-instantiable error as
    # component.executor is Type[BaseExecutor] which has an abstract function.
    executor = self._executor_class(executor_context)  # type: ignore

    executor.Do(self._input_dict, self._output_dict, self._exec_properties)

  def _run_publisher(self) -> None:
    """Publish execution result to ml metadata."""
    # TODO(jyzhao): support publisher after go/tfx-oss-artifact-passing.
    pass

  def run(self) -> None:
    """Execute the component, includes driver, executor and publisher."""
    if self._run_driver():
      # Cached.
      return

    self._run_executor()

    self._run_publisher()
