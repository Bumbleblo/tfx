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
"""Tests for tfx.orchestration.airflow.airflow_component."""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import datetime
import os

from airflow import models
from airflow.operators import dummy_operator
import mock
import tensorflow as tf

from tfx.components.trainer import driver
from tfx.components.trainer import executor
from tfx.orchestration.airflow import airflow_component
from tfx.orchestration.airflow import airflow_pipeline
from tfx.utils import logging_utils
from tfx.utils.types import TfxArtifact


class AirflowComponentTest(tf.test.TestCase):

  def setUp(self):
    self._temp_dir = os.environ.get('TEST_UNDECLARED_OUTPUTS_DIR',
                                    self.get_temp_dir())
    dummy_dag = models.DAG(
        dag_id='my_component', start_date=datetime.datetime(2019, 1, 1))
    self.checkcache_op = dummy_operator.DummyOperator(
        task_id='my_component.checkcache', dag=dummy_dag)
    self.tfx_python_op = dummy_operator.DummyOperator(
        task_id='my_component.pythonexec', dag=dummy_dag)
    self.noop_sink_op = dummy_operator.DummyOperator(
        task_id='my_component.noop_sink', dag=dummy_dag)
    self.publishexec_op = dummy_operator.DummyOperator(
        task_id='my_component.publishexec', dag=dummy_dag)
    self._logger_config = logging_utils.LoggerConfig()
    self.parent_dag = airflow_pipeline.AirflowPipeline(
        pipeline_name='pipeline_name',
        start_date=datetime.datetime(2018, 1, 1),
        schedule_interval=None,
        pipeline_root='pipeline_root',
        metadata_db_root=self._temp_dir,
        metadata_connection_config=None,
        additional_pipeline_args=None,
        enable_cache=True)
    self.input_dict = {'i': [TfxArtifact('i')]}
    self.output_dict = {'o': [TfxArtifact('o')]}
    self.exec_properties = {'e': 'e'}
    self.driver_args = {'d': 'd'}

  @mock.patch('airflow.operators.python_operator.BranchPythonOperator')
  @mock.patch('airflow.operators.python_operator.PythonOperator')
  @mock.patch('airflow.operators.dummy_operator.DummyOperator')
  @mock.patch(
      'tfx.orchestration.airflow.airflow_adapter.AirflowAdapter'
  )
  def test_tfx_workflow(self, mock_airflow_adapter_class,
                        mock_dummy_operator_class, mock_python_operator_class,
                        mock_branch_python_operator_class):
    mock_airflow_adapter = mock.Mock()
    mock_airflow_adapter.check_cache_and_maybe_prepare_execution = 'check_cache'
    mock_airflow_adapter.python_exec = 'python_exec'
    mock_airflow_adapter.publish_exec = 'publish_exec'
    mock_airflow_adapter_class.return_value = mock_airflow_adapter
    mock_dummy_operator_class.side_effect = [self.noop_sink_op]
    mock_python_operator_class.side_effect = [
        self.tfx_python_op, self.publishexec_op
    ]
    mock_branch_python_operator_class.side_effect = [self.checkcache_op]
    tfx_worker = airflow_component._TfxWorker(
        component_name='component_name',
        task_id='my_component',
        parent_dag=self.parent_dag,
        input_dict=self.input_dict,
        output_dict=self.output_dict,
        exec_properties=self.exec_properties,
        driver_args=self.driver_args,
        driver_class=None,
        executor_class=None,
        additional_pipeline_args=None,
        metadata_connection_config=None,
        logger_config=self._logger_config)

    self.assertItemsEqual(self.checkcache_op.upstream_list, [])
    self.assertItemsEqual(self.tfx_python_op.upstream_list,
                          [self.checkcache_op])
    self.assertItemsEqual(self.publishexec_op.upstream_list,
                          [self.tfx_python_op])
    self.assertItemsEqual(self.noop_sink_op.upstream_list,
                          [self.checkcache_op])

    mock_airflow_adapter_class.assert_called_with(
        component_name='component_name',
        input_dict=self.input_dict,
        output_dict=self.output_dict,
        exec_properties=self.exec_properties,
        driver_args=self.driver_args,
        driver_class=None,
        executor_class=None,
        additional_pipeline_args=None,
        metadata_connection_config=None,
        logger_config=self._logger_config)

    mock_branch_python_operator_class.assert_called_with(
        task_id='my_component.checkcache',
        provide_context=True,
        python_callable='check_cache',
        op_kwargs={
            'uncached_branch': 'my_component.exec',
            'cached_branch': 'my_component.noop_sink',
        },
        dag=tfx_worker)

    mock_dummy_operator_class.assert_called_with(
        task_id='my_component.noop_sink', dag=tfx_worker)

    python_operator_calls = [
        mock.call(
            task_id='my_component.exec',
            provide_context=True,
            python_callable='python_exec',
            op_kwargs={
                'cache_task_name': 'my_component.checkcache',
            },
            dag=tfx_worker),
        mock.call(
            task_id='my_component.publishexec',
            provide_context=True,
            python_callable='publish_exec',
            op_kwargs={
                'cache_task_name': 'my_component.checkcache',
                'exec_task_name': 'my_component.exec',
            },
            dag=tfx_worker)
    ]
    mock_python_operator_class.assert_has_calls(python_operator_calls)

  @mock.patch('airflow.operators.python_operator.BranchPythonOperator')
  @mock.patch('airflow.operators.python_operator.PythonOperator')
  @mock.patch(
      'tfx.orchestration.airflow.airflow_adapter.AirflowAdapter'
  )
  def test_airflow_component(self, mock_airflow_adapter_class,
                             mock_python_operator_class,
                             mock_branch_python_operator_class):
    mock_airflow_adapter = mock.Mock()
    mock_airflow_adapter.check_cache_and_maybe_prepare_execution = 'check_cache'
    mock_airflow_adapter.python_exec = 'python_exec'
    mock_airflow_adapter.publish_exec = 'publish_exec'
    mock_airflow_adapter_class.return_value = mock_airflow_adapter

    # Ensure the new component is added to the dag
    component_count = len(self.parent_dag.subdags)
    _ = airflow_component.Component(
        parent_dag=self.parent_dag,
        component_name='test_component',
        unique_name='test_component_unique_name',
        driver=driver.Driver,
        executor=executor.Executor,
        input_dict=self.input_dict,
        output_dict=self.output_dict,
        exec_properties=self.exec_properties)
    self.assertEqual(len(self.parent_dag.subdags), component_count+1)


if __name__ == '__main__':
  tf.test.main()
