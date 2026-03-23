# -*- coding: utf-8 -*-
# Copyright 2022 Aneior Studio, SL
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import base64
import re
import zlib
import time
from typing import Any
from airflow.exceptions import AirflowException

from airflow.models import BaseOperator
from airflow.utils.context import Context
from airflow_hop.hooks import HopHook

class HopBaseOperator(BaseOperator):
    """Hop Base Operator"""

    LOG_TEMPLATE = '%s: %s, with id %s'
    FINISHED_STATUSES = ['Finished']
    ERROR_STATUSES = [
        'Stopped',
        'Finished (with errors)',
        'Stopped (with errors)'
    ]
    END_STATUSES = FINISHED_STATUSES + ERROR_STATUSES

    def _log_logging_string(self, raw_logging_string):
        logs = raw_logging_string
        cdata = re.match(r'\<\!\[CDATA\[([^\]]+)\]\]\>', logs)
        cdata = cdata.group(1) if cdata else raw_logging_string
        decoded_lines = zlib.decompress(base64.b64decode(cdata),
                                        16 + zlib.MAX_WBITS)
        if decoded_lines:
            for line in re.compile(r'\r\n|\n|\r').split(
                    decoded_lines.decode('utf-8')):
                self.log.info(line)

class HopWorkflowOperator(HopBaseOperator):
    """Hop Workflow Operator"""

    template_fields = ('task_params',)

    def __init__(self,
                 workflow,
                 project_path,
                 project_name,
                 environment_path,
                 environment_name,
                 hop_config_path,
                 log_level,
                 *args,
                 hop_params=None,
                 hop_conn_id='hop_default',
                 run_configuration='local',
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.workflow = workflow
        self.project_path = project_path
        self.project_name = project_name
        self.environment_path = environment_path
        self.environment_name = environment_name
        self.hop_config_path = hop_config_path
        self.log_level = log_level
        self.task_params = hop_params
        self.hop_conn_id = hop_conn_id
        self.run_configuration = run_configuration

    def __get_hop_client(self):
        return HopHook(
                self.project_path,
                self.project_name,
                self.environment_path,
                self.environment_name,
                self.hop_config_path,
                self.hop_conn_id,
                self.log_level,
                self.run_configuration).get_conn()

    def execute(self, context: Context) -> Any: # pylint: disable=unused-argument
        
        conn = self.__get_hop_client()
        register_rs = conn.register_workflow(self.workflow, self.task_params)
        message = register_rs['webresult']['message']
        work_id = register_rs['webresult']['id']
        # self.log.info(f'{self.workflow}: {message}')
        self.log.info(f'{self.workflow}')

        start_rs = conn.start_workflow(self.workflow, work_id)
        result = start_rs['webresult']['result']
        # self.log.info(f'{self.workflow}: Started {result}')
        self.log.info(f'Started {result}')
        
        self.log.info("pt-br: O status final da execução será exibido nos logs ao término da task.")
        self.log.info("en-us: Execution result logs (success or failure) will be displayed at the end of the run.")
        
        work_status_rs = None
        status_desc = None
        
        while not work_status_rs or status_desc not in self.END_STATUSES:
            work_status_rs = conn.workflow_status(self.workflow, work_id)

            status = work_status_rs['workflow-status']
            status_desc = status['status_desc']
            time.sleep(5)
            # self.log.info(self.LOG_TEMPLATE, status_desc, self.workflow, work_id)
            # self._log_logging_string(status['logging_string'])

            # if status_desc not in self.END_STATUSES:
            #     self.log.info('Sleeping 5 seconds before ask again')
            #     time.sleep(5)
            
        work_status_rs = conn.workflow_status(self.workflow, work_id)

        status = work_status_rs['workflow-status']
        status_desc = status['status_desc']
            
        if status_desc in self.FINISHED_STATUSES:
            self.log.info(self.LOG_TEMPLATE, status_desc, self.workflow, work_id)
            self._log_logging_string(status['logging_string'])

        if 'error_desc' in status and status['error_desc']:
            self.log.error(self.LOG_TEMPLATE, status['error_desc'], self.workflow, work_id)
            self._log_logging_string(status['logging_string'])

        if status_desc in self.ERROR_STATUSES:
            self.log.error(self.LOG_TEMPLATE, status_desc, self.workflow, work_id)
            raise AirflowException(status_desc)
        
        # Limpar ID após conclusão bem-sucedida
        self.work_id = None
        
    def on_kill(self) -> None:
        """Interrompe a execução remota no Hop Server quando a task é morta (timeout, clear, etc.)"""
        if hasattr(self, 'work_id') and self.work_id:
            self.log.warning(
                f"Task interrompida - solicitando parada do workflow '{self.workflow}' (ID: {self.work_id}) no Hop Server"
            )
            try:
                conn = self.__get_hop_client()
                stop_rs = conn.stop_workflow(self.workflow, self.work_id)
                self.log.info(
                    f"Parada solicitada com sucesso: {stop_rs['webresult'].get('message', 'OK')}"
                )
            except Exception as e:
                self.log.error(
                    f"Falha ao parar workflow '{self.workflow}' (ID {self.work_id}) no Hop Server: {str(e)}"
                )
            finally:
                self.work_id = None  # Limpar referência


class HopPipelineOperator(HopBaseOperator):
    """Hop Pipeline Operator"""

    template_fields = ('task_params',)

    def __init__(self,
                 pipeline,
                 project_path,
                 project_name,
                 log_level,
                 environment_path,
                 environment_name,
                 hop_config_path,
                 *args,
                 hop_params=None,
                 hop_conn_id='hop_default',
                 run_configuration='local',
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.pipeline = pipeline
        self.project_path = project_path
        self.project_name = project_name
        self.log_level = log_level
        self.task_params = hop_params
        self.hop_conn_id = hop_conn_id
        self.environment_path = environment_path
        self.environment_name = environment_name
        self.hop_config_path = hop_config_path
        self.run_configuration = run_configuration

    def __get_hop_client(self):
        return HopHook(
                self.project_path,
                self.project_name,
                self.environment_path,
                self.environment_name,
                self.hop_config_path,
                self.hop_conn_id,
                self.log_level,
                self.run_configuration).get_conn()

    def execute(self, context: Context) -> Any: # pylint: disable=unused-argument
        
        conn = self.__get_hop_client()
        register_rs = conn.register_pipeline(self.pipeline, self.task_params)
        message = register_rs['webresult']['message']
        pipe_id = register_rs['webresult']['id']
        # self.log.info(f'{self.pipeline}: {message}')
        self.log.info(f'{self.pipeline}')

        prepare_exec_rs = conn.prepare_pipeline_exec(self.pipeline, pipe_id)
        result = prepare_exec_rs['webresult']['result']
        # self.log.info(f'{self.pipeline}: Prepare {result}')
        self.log.info(f'Prepare {result}')

        start_exec_rs = conn.start_pipeline_execution(self.pipeline, pipe_id)
        result = start_exec_rs['webresult']['result']
        # self.log.info(f'{self.pipeline}: Started {result}')        
        self.log.info(f'Started {result}')

        self.log.info("pt-br: O status final da execução será exibido nos logs ao término da task.")
        self.log.info("en-us: Execution result logs (success or failure) will be displayed at the end of the run.")


        pipe_status_rs = None
        status_desc = None
        
        while not pipe_status_rs or status_desc not in self.END_STATUSES:
            pipe_status_rs = conn.pipeline_status(self.pipeline, pipe_id)

            status = pipe_status_rs['pipeline-status']
            status_desc = status['status_desc']
            time.sleep(5)
            # self.log.info(self.LOG_TEMPLATE, status_desc, self.pipeline, pipe_id)
            # self._log_logging_string(status['logging_string'])

            # if status_desc not in self.END_STATUSES:
            #     self.log.info('Sleeping 5 seconds before ask again')
            #     time.sleep(5)
            
        pipe_status_rs = conn.pipeline_status(self.pipeline, pipe_id)

        status = pipe_status_rs['pipeline-status']
        status_desc = status['status_desc']

        if status_desc in self.FINISHED_STATUSES:
            self.log.info(self.LOG_TEMPLATE, status_desc, self.pipeline, pipe_id)
            self._log_logging_string(status['logging_string'])
            
        if 'error_desc' in status and status['error_desc']:
            self._log_logging_string(status['logging_string'])
            self.log.error(self.LOG_TEMPLATE, status['error_desc'], self.pipeline, pipe_id)

        if status_desc in self.ERROR_STATUSES:
            self.log.error(self.LOG_TEMPLATE, status_desc, self.pipeline, pipe_id)
            raise AirflowException(status_desc)
        
        # Limpar ID após conclusão bem-sucedida
        self.pipe_id = None
        
    def on_kill(self) -> None:
        """Interrompe a execução remota no Hop Server quando a task é morta (timeout, clear, etc.)"""
        if hasattr(self, 'pipe_id') and self.pipe_id:
            self.log.warning(
                f"Task interrompida - solicitando parada do pipeline '{self.pipeline}' (ID: {self.pipe_id}) no Hop Server"
            )
            try:
                conn = self.__get_hop_client()
                stop_rs = conn.stop_pipeline_execution(self.pipeline, self.pipe_id)
                self.log.info(
                    f"Parada solicitada com sucesso: {stop_rs['webresult'].get('message', 'OK')}"
                )
            except Exception as e:
                self.log.error(
                    f"Falha ao parar pipeline '{self.pipeline}' (ID {self.pipe_id}) no Hop Server: {str(e)}"
                )
            finally:
                self.pipe_id = None  # Limpar referência
