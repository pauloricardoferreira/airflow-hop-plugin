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
import gzip
import json
import os
from xml.etree import ElementTree
from xml.etree.ElementTree import Element

from airflow.exceptions import AirflowException

class XMLBuilder:
    """
    Builds an XML file to be sent through HTTP protocol
    """

    PIPE_INFO_POS = 0
    PIPE_PARAM_POS = 7
    WORKFLOW_PARAM_POS = 9

    def __init__(
                self,
                project_path,
                project_name,
                environment_path,
                environment_name,
                hop_config_path,
                task_params):

        self.project_path = project_path

        with open(f'{hop_config_path}/hop-config.json', encoding='utf-8') as file:
            config_data = json.load(file)

        self.global_variables = config_data['variables']

        project = next(item for item in config_data['projectsConfig']['projectConfigurations']
            if item['projectName'] == project_name)

        metadata_path = f'{self.project_path}/metadata'

        print(f'Metapah: {metadata_path}')

        self.metastore_file = {}

        for i in os.listdir(metadata_path):
            metastore_tmp = []

            self.metastore_file[i] = []
            
            metadata_path_node = os.listdir(f'{metadata_path}/{i}')

            print(f'Metapah Node: {metadata_path_node}')

            if metadata_path_node != []:
                for j in metadata_path_node:
                    meta_files = f'{metadata_path}/{i}/{j}'

                    print(f'Metafile: {meta_files}')

                    with open(f'{meta_files}') as meta_file:
                        meta_file_tmp = json.load(meta_file)

                    metastore_tmp.append(meta_file_tmp)
                self.metastore_file[i] = metastore_tmp
        
        # self.metastore_file = f'{project_path}/metadata.json'

        with open(f'{project_path}/{project["configFilename"]}') as file:
            project_data = json.load(file)
        self.project_variables = project_data['config']['variables']

        if task_params is None:
            self.task_params = []
        else:
            self.task_params = task_params

        self.environment_vars = []
        if environment_name is None: return

        env = next(item for item in config_data['projectsConfig']['lifecycleEnvironments']
            if item['name'] == environment_name)
        for env_file in env['configurationFiles']:
            env_file = env_file.split('/')[-1]
            with open(f'{environment_path}/{env_file}', encoding='utf-8') as file:
                env_data = json.load(file)
            self.environment_vars = self.environment_vars + env_data['variables']

    def get_workflow_xml(self, workflow_name) -> bytes:
        workflow_path = f'{self.project_path}/{workflow_name}'
        root = Element('workflow_configuration')
        try:
            workflow = ElementTree.parse(workflow_path)
            root.append(workflow.getroot())
            root.append(self.__get_workflow_execution_config(workflow_path))
            root.append(self.__generate_element('metastore_json', self.__generate_metastore()))
            return ElementTree.tostring(root, encoding='utf-8')
        except FileNotFoundError as error:
            raise AirflowException(f'ERROR: workflow {workflow_path} not found') from error


    def __get_workflow_execution_config(self, workflow_path) -> Element:
        root = Element('workflow_execution_configuration')
        root.append(self.__get_workflow_parameters(workflow_path))
        root.append(self.__get_variables())
        root.append(self.__generate_element('run_configuration','local'))
        return root

    def __get_workflow_parameters(self, workflow_path):
        tree = ElementTree.parse(workflow_path)
        tree_root = tree.getroot()
        parameters = tree_root.findall('parameters')
        root = Element('parameters')
        for parameter in parameters[0]:
            new_param = Element('parameter')
            new_param.append(self.__generate_element('name',parameter[0].text))
            new_param.append(self.__generate_element('value',parameter[1].text))
            root.append(new_param)
        return root


    def get_pipeline_xml(self, pipeline_name, pipeline_config) -> bytes:
        pipeline_path = f'{self.project_path}/{pipeline_name}'
        root = Element('pipeline_configuration')
        try:
            pipeline = ElementTree.parse(pipeline_path)
            root.append(pipeline.getroot())
            root.append(self.__get_pipeline_execution_config(pipeline_config, pipeline_path))
            root.append(self.__generate_element('metastore_json', self.__generate_metastore()))
            return ElementTree.tostring(root, encoding='utf-8')
        except FileNotFoundError as error:
            raise AirflowException(f'ERROR: pipeline {pipeline_path} not found') from error
        except StopIteration as error:
            raise AirflowException(f'ERROR: pipeline configuration {pipeline_config}'\
                ' not found') from error

    def __get_pipeline_execution_config(self, pipeline_config, pipeline_file) -> Element:
        root = Element('pipeline_execution_configuration')
        root.append(self.__get_pipe_parameters(pipeline_file))
        root.append(self.__get_variables(pipeline_config))
        root.append(self.__generate_element('run_configuration','local'))
        return root

    def __get_pipe_parameters(self, pipeline_file) -> Element:
        tree = ElementTree.parse(pipeline_file)
        tree_root = tree.getroot()
        parameters = tree_root[0].findall('parameters')
        root = Element('parameters')
        for parameter in parameters[0]:
            new_param = Element('parameter')
            new_param.append(self.__generate_element('name',parameter[0].text))
            new_param.append(self.__generate_element('value',parameter[1].text))
            root.append(new_param)
        return root

    def __get_variables(self, pipeline_config = None) -> Element:
        root = Element('variables')

        for parameter in self.task_params:
            new_variable = Element('variable')
            new_variable.append(self.__generate_element('name', parameter))
            new_variable.append(self.__generate_element('value',
                self.task_params[parameter]))
            root.append(new_variable)

        for variable in self.global_variables:
            new_variable = Element('variable')
            new_variable.append(self.__generate_element('name', variable['name']))
            new_variable.append(self.__generate_element('value', variable['value']))
            root.append(new_variable)

        if pipeline_config is not None:
            # with open(self.metastore_file, encoding='utf-8') as f::
            data = json.load(self.metastore_file)

            run_config = next(item for item in data['pipeline-run-configuration']
                if item['name'] == pipeline_config)

            pipeline_vars = run_config['configurationVariables']
            for variable in pipeline_vars:
                new_variable = Element('variable')
                new_variable.append(self.__generate_element('name',variable['name']))
                new_variable.append(self.__generate_element('value',variable['value']))
                root.append(new_variable)

        for variable in self.project_variables:
            new_variable = Element('variable')
            new_variable.append(self.__generate_element('name',variable['name']))
            new_variable.append(self.__generate_element('value',variable['value']))
            root.append(new_variable)

        for variable in self.environment_vars:
            new_variable = Element('variable')
            new_variable.append(self.__generate_element('name', variable['name']))
            new_variable.append(self.__generate_element('value', variable['value']))
            root.append(new_variable)

        project_home = Element('variable')
        project_home.append(self.__generate_element('name','PROJECT_HOME'))
        project_home.append(self.__generate_element('value',self.project_path))

        jdk_debug = Element('variable')
        jdk_debug.append(self.__generate_element('name','jdk.debug'))
        jdk_debug.append(self.__generate_element('value','release'))
        root.append(jdk_debug)
        return root

    def __generate_metastore(self) -> str:
        # with open(self.metastore_file, mode='br') as file:
        content = json.loads(self.metastore_file)
        metastore = gzip.compress(content)
        return base64.b64encode(metastore).decode('utf-8')

    def __generate_element(self, name:str, text = None) -> Element:
        element = Element(name)
        if text is not None:
            element.text = text
        return element
