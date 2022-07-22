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
from typing import Any
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook

import requests
import xmltodict
from requests.auth import HTTPBasicAuth
from airflow_hop.xml import XMLBuilder

class HopHook(BaseHook):
    """
    Implementation hook to interact with Hop REST API
    """

    class HopServerConnection:
        """
        Implements a Hop Server connection
        """

        PREPARE_PIPELINE_EXEC = '/hop/prepareExec/'
        START_PIPELINE_EXEC = '/hop/startExec/'
        STOP_PIPELINE_EXEC = '/hop/stopExec/'
        REGISTER_PIPELINE = '/hop/registerPipeline/'
        PIPELINE_STATUS = '/hop/pipelineStatus/'

        REGISTER_WORKFLOW = '/hop/registerWorkflow/'
        WORKFLOW_STATUS = '/hop/workflowStatus/'
        START_WORKFLOW = '/hop/startWorkflow/'
        STOP_WORKFLOW = '/hop/stopWorkflow/'


        def __init__(
                self,
                host,
                port,
                username,
                password,
                log_level,
                hop_config_path,
                project_name):
            self.host = host
            self.port = port
            self.username = username
            self.password = password
            self.log_level = log_level
            self.metastore_file = f'{hop_config_path}/projects/{project_name}/metadata.json'
            self.hop_config_file = f'{hop_config_path}/hop-config.json'
            self.project_config_file = f'{hop_config_path}/projects/' \
                                       f'{project_name}/project-config.json'

        def __get_url(self,endpoint):
            return f'http://{self.host}:{self.port}{endpoint}'

        def __get_auth(self):
            return HTTPBasicAuth(self.username,self.password)

        def register_pipeline(self, pipe_file, pipe_config):
            xml_builder = XMLBuilder(
                            self.metastore_file,
                            self.hop_config_file,
                            self.project_config_file)
            data = xml_builder.get_pipeline_xml(pipe_file, pipe_config)
            parameters = {'xml':'Y'}
            response = requests.post(self.__get_url(self.REGISTER_PIPELINE),
                                    params=parameters,auth=self.__get_auth(),
                                    data = data)
            if response.status_code >= 400:
                result = xmltodict(response.content)
                raise AirflowException('{}: {}'.format(
                    result['webresult']['result'],
                    result['webresult']['message'])
                )
            else:
                return xmltodict.parse(response.content)

        def pipeline_status(self, pipe_name, pipe_id):
            parameters = {'name':pipe_name, 'id':pipe_id,'xml':'Y'}
            response = requests.get(self.__get_url(self.PIPELINE_STATUS),
                                    params=parameters, auth=self.__get_auth())
            if response.status_code >= 400:
                result = xmltodict(response.content)
                raise AirflowException('{}: {}'.format(
                    result['webresult']['result'],
                    result['webresult']['message'])
                )
            else:
                return xmltodict.parse(response.content)

        def prepare_pipeline_exec(self, pipe_name, pipe_id):
            parameters = {'name':pipe_name, 'id':pipe_id,'xml':'Y'}
            response = requests.get(self.__get_url(self.PREPARE_PIPELINE_EXEC),
                                    params=parameters, auth=self.__get_auth())
            if response.status_code >= 400:
                result = xmltodict(response.content)
                raise AirflowException('{}: {}'.format(
                    result['webresult']['result'],
                    result['webresult']['message'])
                )
            else:
                return xmltodict.parse(response.content)

        def start_pipeline_execution(self, pipe_name, pipe_id):
            parameters = {'name':pipe_name, 'id':pipe_id,'xml':'Y'}
            response = requests.get(self.__get_url(self.START_PIPELINE_EXEC),
                                    params=parameters, auth=self.__get_auth())
            if response.status_code >= 400:
                result = xmltodict(response.content)
                raise AirflowException('{}: {}'.format(
                    result['webresult']['result'],
                    result['webresult']['message'])
                )
            else:
                return xmltodict.parse(response.content)

        def stop_pipeline_execution(self, pipe_name, pipe_id):
            parameters = {'name':pipe_name, 'id':pipe_id,'xml':'Y'}
            response = requests.get(self.__get_url(self.STOP_PIPELINE_EXEC),
                                    params=parameters, auth=self.__get_auth())
            if response.status_code >= 400:
                result = xmltodict(response.content)
                raise AirflowException('{}: {}'.format(
                    result['webresult']['result'],
                    result['webresult']['message'])
                )
            else:
                return xmltodict.parse(response.content)


        def register_workflow(self, workflow_file):
            xml_builder = XMLBuilder(
                                self.metastore_file,
                                self.hop_config_file,
                                self.project_config_file)
            data = xml_builder.get_workflow_xml(workflow_file)
            parameters = {'xml':'Y'}
            response = requests.post(self.__get_url(self.REGISTER_WORKFLOW),
                                    params=parameters,auth=self.__get_auth(), data=data)
            if response.status_code >= 400:
                result = xmltodict(response.content)
                raise AirflowException('{}: {}'.format(
                    result['webresult']['result'],
                    result['webresult']['message'])
                )
            else:
                return xmltodict.parse(response.content)

        def workflow_status(self, workflow_name, workflow_id):
            parameters = {'name':workflow_name, 'id':workflow_id,'xml':'Y'}
            response = requests.get(self.__get_url(self.WORKFLOW_STATUS),
                                    params=parameters, auth=self.__get_auth())
            if response.status_code >= 400:
                result = xmltodict(response.content)
                raise AirflowException('{}: {}'.format(
                    result['webresult']['result'],
                    result['webresult']['message'])
                )
            else:
                return xmltodict.parse(response.content)

        def start_workflow(self, workflow_name, workflow_id):
            parameters = {'name':workflow_name, 'id':workflow_id, 'xml':'Y'}
            response = requests.get(self.__get_url(self.START_WORKFLOW),
                                    params=parameters, auth=self.__get_auth())
            if response.status_code >= 400:
                result = xmltodict(response.content)
                raise AirflowException('{}: {}'.format(
                    result['webresult']['result'],
                    result['webresult']['message'])
                )
            else:
                return xmltodict.parse(response.content)

        def stop_workflow(self, workflow_name, workflow_id):
            parameters = {'name':workflow_name, 'id':workflow_id,'xml':'Y'}
            response = requests.get(self.__get_url(self.STOP_WORKFLOW),
                                    params=parameters, auth=self.__get_auth())
            if response.status_code >= 400:
                result = xmltodict(response.content)
                raise AirflowException('{}: {}'.format(
                    result['webresult']['result'],
                    result['webresult']['message'])
                )
            else:
                return xmltodict.parse(response.content)


    def __init__(
            self,
            project_name,
            conn_id = 'hop_default',
            log_level = 'Basic'):
        """Hop Hook constructor to initialize the object."""

        self.conn_id = conn_id
        self.connection = self.get_connection(conn_id)
        self.extras = self.connection.extra_dejson
        self.log_level = log_level
        self.extras = self.connection.extra_dejson
        self.project_name = project_name
        self.hop_client = None

    def get_conn(self) -> Any:
        if self.hop_client:
            return self.hop_client

        self.hop_client = self.HopServerConnection(
            host = self.connection.host,
            port = self.connection.port,
            username = self.connection.login,
            password = self.connection.password,
            log_level = self.log_level,
            hop_config_path = self.extras.get('config_path'),
            project_name = self.project_name)
        return self.hop_client