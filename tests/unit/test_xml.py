# -*- coding: utf-8 -*-
# Copyright 2022 Aneior Studio, SL
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from airflow import AirflowException
from tests.operator_test_base import OperatorTestBase
from airflow_hop.xml import XMLBuilder

HOP_HOME = f'{OperatorTestBase.TESTS_PATH}/assets'
PROJECT_NAME = 'default'
PROJECT_HOME = f'config/projects/{PROJECT_NAME}'
PROJECT_FOLDER = f'{HOP_HOME}/config/projects/{PROJECT_NAME}'
ENV_NAME = 'Dev'
PARAMS = {'DATE': '25-08-2022'}
METASTORE_FILE = f'{PROJECT_FOLDER}/metadata.json'
GLOBAL_VARS = [ {
        'name' : 'HOP_MAX_LOG_SIZE_IN_LINES',
        'value' : '0',
        'description' : 'The maximum number of log lines that are kept internally by Hop. Set to 0'\
            ' to keep all rows (default)'
    }, {
        'name' : 'HOP_MAX_LOG_TIMEOUT_IN_MINUTES',
        'value' : '1440',
        'description' : 'The maximum age (in minutes) of a log line while being kept internally by'\
            ' Hop. Set to 0 to keep all rows indefinitely (default)'
    }, {
        'name' : 'HOP_LOG_TAB_REFRESH_DELAY',
        'value' : '1000',
        'description' : 'The hop log tab refresh delay.'
    }, {
        'name' : 'HOP_LOG_TAB_REFRESH_PERIOD',
        'value' : '1000',
        'description' : 'The hop log tab refresh period.'
    }, {
        'name' : 'HOP_USE_NATIVE_FILE_DIALOG',
        'value' : 'N',
        'description' : 'Set this value to \'Y\' if you want to use the system file'\
            ' open/save dialog when browsing files.'
    }, {
        'name' : 'HOP_LOG_SIZE_LIMIT',
        'value' : '0',
        'description' : 'The log size limit for all pipelines and workflows that don\'t have the'\
            ' \"log size limit\" property set in their respective properties.'
    }, {
        'name' : 'HOP_SERVER_DETECTION_TIMER',
        'value' : '',
        'description' : 'The name of the variable that defines the timer used for detecting'\
            ' server nodes'
    }, {
        'name' : 'HOP_EMPTY_STRING_DIFFERS_FROM_NULL',
        'value' : 'N',
        'description' : 'NULL vs Empty String. If this setting is set to \'Y\', an empty string'\
            ' and null are different. Otherwise they are not'
    }, {
        'name' : 'HOP_LENIENT_STRING_TO_NUMBER_CONVERSION',
        'value' : 'N',
        'description' : 'System wide flag to allow lenient string to number conversion for'\
            ' backward compatibility. If this setting is set to \'Y\', an string starting with'\
            ' digits will be converted successfully into a number. (example: 192.168.1.1 will'\
            ' be converted into 192 or 192.168 or 192168 depending on the decimal and'\
            ' grouping symbol). The default (N) will be to throw an error if non-numeric'\
            ' symbols are found in the string.'
    }, {
        'name' : 'HOP_SYSTEM_HOSTNAME',
        'value' : '',
        'description' : 'You can use this variable to speed up hostname lookup. Hostname lookup is'\
            ' performed by Hop so that it is capable of logging the server on which a workflow or'\
            ' pipeline is executed.'
    }, {
        'name' : 'HOP_SERVER_OBJECT_TIMEOUT_MINUTES',
        'value' : '1440',
        'description' : 'This project variable will set a time-out after which waiting, completed'\
            ' or stopped pipelines and workflows will be automatically cleaned up. The default'\
            ' value is 1440 (one day).'
    }, {
        'name' : 'HOP_TRANSFORM_PERFORMANCE_SNAPSHOT_LIMIT',
        'value' : '0',
        'description' : 'The maximum number of transform performance snapshots to keep in memory.'\
            ' Set to 0 to keep all snapshots indefinitely (default)'
    }, {
        'name' : 'HOP_MAX_WORKFLOW_TRACKER_SIZE',
        'value' : '5000',
        'description' : 'The maximum age (in minutes) of a log line while being kept internally by'\
            ' Hop. Set to 0 to keep all rows indefinitely (default)'
    }, {
        'name' : 'HOP_MAX_ACTIONS_LOGGED',
        'value' : '5000',
        'description' : 'The maximum number of action results kept in memory for logging purposes.'
    }, {
        'name' : 'HOP_MAX_LOGGING_REGISTRY_SIZE',
        'value' : '10000',
        'description' : 'The maximum number of logging registry entries kept in memory for logging'\
            ' purposes'
    }, {
        'name' : 'HOP_PLUGIN_CLASSES',
        'value' : '',
        'description' : 'A comma delimited list of classes to scan for plugin annotations'
    }, {
        'name' : 'HOP_PIPELINE_ROWSET_SIZE',
        'value' : '',
        'description' : 'Name of the environment variable that contains the size of the pipeline'\
            ' rowset size. This overwrites values that you set pipeline settings'
    }, {
        'name' : 'HOP_PASSWORD_ENCODER_PLUGIN',
        'value' : 'Hop',
        'description' : 'Specifies the password encoder plugin to use by ID (Hop is the default).'
    }, {
        'name' : 'HOP_ROWSET_GET_TIMEOUT',
        'value' : '50',
        'description' : 'The name of the variable that optionally contains an alternative rowset '\
            'get timeout (in ms). This only makes a difference for extremely short lived pipelines.'
    }, {
        'name' : 'HOP_ROWSET_PUT_TIMEOUT',
        'value' : '50',
        'description' : 'The name of the variable that optionally contains an alternative rowset '\
            'put timeout (in ms). This only makes a difference for extremely short lived pipelines.'
    }, {
        'name' : 'HOP_BATCHING_ROWSET',
        'value' : 'N',
        'description' : 'Set this variable to \'Y\' if you want to test a more efficient batching'\
            ' row set.'
    }, {
        'name' : 'HOP_FILE_OUTPUT_MAX_STREAM_COUNT',
        'value' : '1024',
        'description' : 'This project variable is used by the Text File Output transform. It'\
            ' defines the max number of simultaneously open files within the transform. The'\
            ' transform will close/reopen files as necessary to insure the max is not exceeded'
    }, {
        'name' : 'HOP_FILE_OUTPUT_MAX_STREAM_LIFE',
        'value' : '0',
        'description' : 'This project variable is used by the Text File Output transform. It'\
            ' defines the max number of milliseconds between flushes of files opened by'\
            ' the transform.'
    }, {
        'name' : 'HOP_DISABLE_CONSOLE_LOGGING',
        'value' : 'N',
        'description' : 'Set this variable to \'Y\' to disable standard Hop logging to the'\
            ' console. (stdout)'
    }, {
        'name' : 'HOP_DEFAULT_NUMBER_FORMAT',
        'value' : '',
        'description' : 'The name of the variable containing an alternative default number format'
    }, {
        'name' : 'HOP_DEFAULT_BIGNUMBER_FORMAT',
        'value' : '',
        'description' : 'The name of the variable containing an alternative default'\
            ' bignumber format'
    }, {
        'name' : 'HOP_DEFAULT_INTEGER_FORMAT',
        'value' : '',
        'description' : 'The name of the variable containing an alternative default integer format'
    }, {
        'name' : 'HOP_DEFAULT_DATE_FORMAT',
        'value' : '',
        'description' : 'The name of the variable containing an alternative default date format'
    }, {
        'name' : 'HOP_AGGREGATION_MIN_NULL_IS_VALUED',
        'value' : 'N',
        'description' : 'Set this variable to \'Y\' to set the minimum to NULL if NULL is within'\
            ' an aggregate. Otherwise by default NULL is ignored by the MIN aggregate and MIN is'\
            ' set to the minimum value that is not NULL. See also the variable'\
            ' HOP_AGGREGATION_ALL_NULLS_ARE_ZERO.'
    }, {
        'name' : 'HOP_AGGREGATION_ALL_NULLS_ARE_ZERO',
        'value' : 'N',
        'description' : 'Set this variable to \'Y\' to return 0 when all values within an'\
            ' aggregate are NULL. Otherwise by default a NULL is returned when all values are NULL.'
    }, {
        'name' : 'HOP_DEFAULT_TIMESTAMP_FORMAT',
        'value' : '',
        'description' : 'The name of the variable containing an alternative default '\
            'timestamp format'
    }, {
        'name' : 'HOP_SPLIT_FIELDS_REMOVE_ENCLOSURE',
        'value' : 'N',
        'description' : 'Set this variable to \'N\' to preserve enclosure symbol after splitting'\
            ' the string in the Split fields transform. Changing it to true will remove first and'\
            ' last enclosure symbol from the resulting string chunks.'
    }, {
        'name' : 'HOP_ALLOW_EMPTY_FIELD_NAMES_AND_TYPES',
        'value' : 'N',
        'description' : 'Set this variable to \'Y\' to allow your pipeline to pass \'null\' fields'\
            ' and/or empty types.'
    }, {
        'name' : 'HOP_GLOBAL_LOG_VARIABLES_CLEAR_ON_EXPORT',
        'value' : 'N',
        'description' : 'Set this variable to \'N\' to preserve global log variables defined in'\
            ' pipeline / workflow Properties -> Log panel. Changing it to \'Y\' will clear it when'\
            ' export pipeline / workflow.'
    }, {
        'name' : 'HOP_LOG_MARK_MAPPINGS',
        'value' : 'N',
        'description' : 'Set this variable to \'Y\' to precede transform/action name in log lines'\
            ' with the complete path to the transform/action. Useful to perfectly identify where'\
            ' a problem happened in our process.'
    }, {
        'name' : 'HOP_SERVER_JETTY_ACCEPTORS',
        'value' : '',
        'description' : 'A variable to configure jetty option: acceptors for Hop server'
    }, {
        'name' : 'HOP_SERVER_JETTY_ACCEPT_QUEUE_SIZE',
        'value' : '',
        'description' : 'A variable to configure jetty option: acceptQueueSize for Hop server'
    }, {
        'name' : 'HOP_SERVER_JETTY_RES_MAX_IDLE_TIME',
        'value' : '',
        'description' : 'A variable to configure jetty option: lowResourcesMaxIdleTime'\
            ' for Hop server'
    }, {
        'name' : 'HOP_DEFAULT_SERVLET_ENCODING',
        'value' : '',
        'description' : 'Defines the default encoding for servlets, leave it empty to use Java'\
            ' default encoding'
    }, {
        'name' : 'HOP_SERVER_REFRESH_STATUS',
        'value' : '',
        'description' : 'A variable to configure refresh for Hop server workflow/pipeline status'\
            ' page'
    }, {
        'name' : 'HOP_MAX_TAB_LENGTH',
        'value' : '',
        'description' : 'A variable to configure Tab size'
    }, {
        'name' : 'HOP_ZIP_MIN_INFLATE_RATIO',
        'value' : '',
        'description' : 'A variable to configure the minimum allowed ratio between de- and'\
            ' inflated bytes to detect a zipbomb'
    }, {
        'name' : 'HOP_ZIP_MIN_INFLATE_RATIO_DEFAULT_STRING',
        'value' : '',
        'description' : ''
    }, {
        'name' : 'HOP_ZIP_MAX_ENTRY_SIZE',
        'value' : '',
        'description' : 'A variable to configure the maximum file size of a single zip entry'
    }, {
        'name' : 'HOP_ZIP_MAX_ENTRY_SIZE_DEFAULT_STRING',
        'value' : '',
        'description' : ''
    }, {
        'name' : 'HOP_ZIP_MAX_TEXT_SIZE',
        'value' : '',
        'description' : 'A variable to configure the maximum number of characters of text that are'\
            ' extracted before an exception is thrown during extracting text from documents'
    }, {
        'name' : 'HOP_ZIP_MAX_TEXT_SIZE_DEFAULT_STRING',
        'value' : '',
        'description' : ''
    }, {
        'name' : 'HOP_LICENSE_HEADER_FILE',
        'value' : '',
        'description' : 'This is the name of the variable which when set should contains the path'\
            ' to a file which will be included in the serialization of pipelines and workflows'
    }, {
        'name' : 'HOP_DEFAULT_BUFFER_POLLING_WAITTIME',
        'value' : '20',
        'description' : 'This is the default polling frequency for the transforms input buffer'\
            ' (in ms)'
    }, {
        'name' : 'NEO4J_LOGGING_CONNECTION',
        'value' : '',
        'description' : 'Set this variable to the name of an existing Neo4j connection to enable'\
            ' execution logging to a Neo4j database.'
    }, {
        'name' : 'HOP_S3_VFS_PART_SIZE',
        'value' : '',
        'description' : ''
    } ]
PROJECT_VARS = [ {
        'name' : 'TESTING_VARIABLE',
        'value' : '42',
        'description' : 'This is a simple Test'
    } ]
ENVIRONMENT_VARS = [ {
        'name' : 'VARTEST',
        'value' : '420',
        'description' : 'Whatever'
    } ]
PIPELINE_CONFIG = 'remote hop server'
PIPELINE = 'pipelines/fake-data-generate-person-record.hpl'


class TestXMLBuilder(OperatorTestBase):
    '''Perform tests regarding XMLBuilder'''

    def test_constructor(self):
        builder = XMLBuilder(HOP_HOME, PROJECT_NAME, PARAMS, ENV_NAME)

        self.assertEqual(builder.task_params, PARAMS)
        self.assertEqual(builder.global_variables, GLOBAL_VARS)
        self.assertEqual(builder.project_folder, PROJECT_FOLDER)
        self.assertEqual(builder.project_home, PROJECT_HOME)
        self.assertEqual(builder.project_variables, PROJECT_VARS)
        self.assertEqual(builder.metastore_file, METASTORE_FILE)
        self.assertEqual(builder.environment_vars, ENVIRONMENT_VARS)

    def test_errors(self):
        builder = XMLBuilder(HOP_HOME, PROJECT_NAME, PARAMS, ENV_NAME)

        with self.assertRaises(AirflowException) as context:
            builder.get_workflow_xml('wrong_workflow')
        self.assertTrue('wrong_workflow not found' in str(context.exception))

        with self.assertRaises(AirflowException) as context:
            builder.get_pipeline_xml('wrong_pipe',PIPELINE_CONFIG)
        self.assertTrue('wrong_pipe not found' in str(context.exception))

        with self.assertRaises(AirflowException) as context:
            builder.get_pipeline_xml(PIPELINE, 'wrong_config')
        self.assertTrue('wrong_config not found' in str(context.exception))
