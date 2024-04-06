# ***********************************************************************
# ******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
# *************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
#
#  (c) 2020.                            (c) 2020.
#  Government of Canada                 Gouvernement du Canada
#  National Research Council            Conseil national de recherches
#  Ottawa, Canada, K1A 0R6              Ottawa, Canada, K1A 0R6
#  All rights reserved                  Tous droits réservés
#
#  NRC disclaims any warranties,        Le CNRC dénie toute garantie
#  expressed, implied, or               énoncée, implicite ou légale,
#  statutory, of any kind with          de quelque nature que ce
#  respect to the software,             soit, concernant le logiciel,
#  including without limitation         y compris sans restriction
#  any warranty of merchantability      toute garantie de valeur
#  or fitness for a particular          marchande ou de pertinence
#  purpose. NRC shall not be            pour un usage particulier.
#  liable in any event for any          Le CNRC ne pourra en aucun cas
#  damages, whether direct or           être tenu responsable de tout
#  indirect, special or general,        dommage, direct ou indirect,
#  consequential or incidental,         particulier ou général,
#  arising from the use of the          accessoire ou fortuit, résultant
#  software.  Neither the name          de l'utilisation du logiciel. Ni
#  of the National Research             le nom du Conseil National de
#  Council of Canada nor the            Recherches du Canada ni les noms
#  names of its contributors may        de ses  participants ne peuvent
#  be used to endorse or promote        être utilisés pour approuver ou
#  products derived from this           promouvoir les produits dérivés
#  software without specific prior      de ce logiciel sans autorisation
#  written permission.                  préalable et particulière
#                                       par écrit.
#
#  This file is part of the             Ce fichier fait partie du projet
#  OpenCADC project.                    OpenCADC.
#
#  OpenCADC is free software:           OpenCADC est un logiciel libre ;
#  you can redistribute it and/or       vous pouvez le redistribuer ou le
#  modify it under the terms of         modifier suivant les termes de
#  the GNU Affero General Public        la “GNU Affero General Public
#  License as published by the          License” telle que publiée
#  Free Software Foundation,            par la Free Software Foundation
#  either version 3 of the              : soit la version 3 de cette
#  License, or (at your option)         licence, soit (à votre gré)
#  any later version.                   toute version ultérieure.
#
#  OpenCADC is distributed in the       OpenCADC est distribué
#  hope that it will be useful,         dans l’espoir qu’il vous
#  but WITHOUT ANY WARRANTY;            sera utile, mais SANS AUCUNE
#  without even the implied             GARANTIE : sans même la garantie
#  warranty of MERCHANTABILITY          implicite de COMMERCIALISABILITÉ
#  or FITNESS FOR A PARTICULAR          ni d’ADÉQUATION À UN OBJECTIF
#  PURPOSE.  See the GNU Affero         PARTICULIER. Consultez la Licence
#  General Public License for           Générale Publique GNU Affero
#  more details.                        pour plus de détails.
#
#  You should have received             Vous devriez avoir reçu une
#  a copy of the GNU Affero             copie de la Licence Générale
#  General Public License along         Publique GNU Affero avec
#  with OpenCADC.  If not, see          OpenCADC ; si ce n’est
#  <http://www.gnu.org/licenses/>.      pas le cas, consultez :
#                                       <http://www.gnu.org/licenses/>.
#
#  $Revision: 4 $
#
# ***********************************************************************
#

import os
import pytest

from cadcdata import FileInfo
from collections import deque
from mock import call, patch, Mock

from caom2pipe import astro_composable as ac
from caom2pipe import execute_composable as ec
from caom2pipe import manage_composable as mc
from wallaby2caom2 import composable, storage_name


def test_store(test_config):
    test_config.change_working_directory('/tmp')
    test_f_name = 'WALLABY_J100342-270137_AverageModelCube_v2.fits'
    test_url = (
        f'vos:cirada/emission/PilotFieldReleases_Jun2021/'
        f'KinematicModels/Wallaby_Hydra_DR2_KinematicModels_v2/'
        f'WALLABY_J100342-270137_v2/{test_f_name}'
    )
    test_storage_name = storage_name.WallabyName(test_url)
    transferrer = Mock()
    cadc_data_client = Mock()
    clients_mock = Mock()
    clients_mock.data_client = cadc_data_client
    observable = mc.Observable(test_config)
    reader_mock = Mock()
    test_subject = ec.Store(test_config, observable, clients_mock, reader_mock, transferrer)
    test_context = {'storage_name': test_storage_name}
    test_subject.execute(test_context)
    assert cadc_data_client.put.called, 'expect a call'
    cadc_data_client.put.assert_called_with(
        '/tmp/WALLABY_J100342-270137',
        f'{test_storage_name.scheme}:{test_storage_name.collection}/{test_f_name}',
    ), 'wrong put args'
    assert transferrer.get.called, 'expect a transfer call'
    transferrer.get.assert_called_with(
        test_url,
        f'/tmp/WALLABY_J100342-270137/{test_f_name}',
    ), 'wrong transferrer args'


@patch('caom2utils.data_util.get_local_headers_from_fits')
@patch('cadcutils.net.ws.WsCapabilities.get_access_url')
@patch('caom2pipe.execute_composable.CaomExecute._caom2_read')
@patch('caom2pipe.execute_composable.CaomExecute._caom2_store')
@patch('caom2pipe.execute_composable.CaomExecute._visit_meta')
def test_run_use_local_files(
    meta_visit_mock,
    caom2_store_mock,
    caom2_read_mock,
    access_url_mock,
    headers_mock,
    test_data_dir,
    test_config,
    tmp_path,
):
    access_url_mock.return_value = 'https://localhost:8080'
    headers_mock.side_effect = ac.make_headers_from_file
    caom2_read_mock.return_value = None
    cwd = os.getcwd()
    os.chdir(tmp_path)
    test_config.change_working_directory(tmp_path.as_posix())
    test_config.task_types = [mc.TaskType.INGEST]
    test_config.proxy_file_name = 'cadcproxy.pem'
    test_config.use_local_files = True
    test_config.data_sources = [test_data_dir]
    test_config.data_source_extensions = ['.fits.header']
    with open(test_config.proxy_fqn, 'w') as f:
        f.write('test content')
    mc.Config.write_to_file(test_config)
    try:
        test_result = composable._run()
        assert test_result is not None, 'expect result'
        assert test_result == 0, 'expect success'
        assert meta_visit_mock.called, '_visit_meta call'
        # 13 == number of test files
        assert meta_visit_mock.call_count == 13, '_visit_meta call count'
        assert caom2_read_mock.called, '_caom2_store call'
        assert caom2_read_mock.call_count == 13, '_caom2_store call count'
        assert caom2_store_mock.called, '_caom2_store call'
        assert caom2_store_mock.call_count == 13, '_caom2_store call count'
    finally:
        os.chdir(cwd)


@patch('vos.Client')
@patch('caom2pipe.client_composable.ClientCollection')
@patch('caom2pipe.reader_composable.VaultReader._retrieve_file_info')
@patch('caom2pipe.data_source_composable.VaultDataSource.get_work')
@patch('caom2pipe.reader_composable.VaultReader._retrieve_headers')
@patch('cadcutils.net.ws.WsCapabilities.get_access_url')
@patch('caom2pipe.execute_composable.CaomExecute._caom2_read')
@patch('caom2pipe.execute_composable.CaomExecute._caom2_store')
@patch('caom2pipe.execute_composable.CaomExecute._visit_meta')
def test_run_use_local_files_false(
    meta_visit_mock,
    caom2_store_mock,
    caom2_read_mock,
    access_url_mock,
    headers_mock,
    get_work_mock,
    file_info_mock,
    clients_mock,
    vos_client_mock,
    test_data_dir,
    test_config,
    tmp_path,
):
    access_url_mock.return_value = 'https://localhost:8080'

    def _make_headers(ign, fqn):
        temp = os.path.basename(fqn)
        return ac.make_headers_from_file(f'{test_data_dir}/dr1/{temp}.header')
    headers_mock.side_effect = _make_headers
    f_name1 = 'vos:goliaths/test/WALLABY_J103250-301601_Hydra_TR1_spec.fits'
    f_name2 = 'vos:goliaths/test/WALLABY_J103939-280552_Hydra_TR2_spec.fits'
    f_uri1 = f'{test_config.scheme}:{test_config.collection}/{os.path.basename(f_name1)}'
    f_uri2 = f'{test_config.scheme}:{test_config.collection}/{os.path.basename(f_name2)}'
    temp_queue = deque()
    temp_queue.append(f_name1)
    temp_queue.append(f_name2)
    get_work_mock.return_value = temp_queue
    caom2_read_mock.return_value = None

    f1 = FileInfo(id=f_name1, file_type='application/fits', md5sum='def')
    f2 = FileInfo(id=f_name2, file_type='application/fits', md5sum='ghi')
    file_info_mock.side_effect = [f1, f2]

    cwd = os.getcwd()
    os.chdir(tmp_path)
    test_config.change_working_directory(tmp_path.as_posix())
    test_config.task_types = [mc.TaskType.INGEST]
    test_config.logging_level = 'INFO'
    test_config.proxy_file_name = 'cadcproxy.pem'
    test_config.use_local_files = False
    test_config.data_sources = ['vos:goliaths/test']
    test_config.data_source_extensions = ['.fits', '.txt']
    mc.Config.write_to_file(test_config)
    with open(test_config.proxy_fqn, 'w') as f:
        f.write('test content')
    try:
        test_result = composable._run()
        assert test_result is not None, 'expect result'
        assert test_result == 0, 'expect success'
        assert meta_visit_mock.called, '_visit_meta call'
        assert meta_visit_mock.call_count == 2, '_visit_meta call count'
        assert caom2_read_mock.called, '_caom2_store call'
        assert caom2_read_mock.call_count == 2, '_caom2_store call count'
        assert caom2_store_mock.called, '_caom2_store call'
        assert caom2_store_mock.call_count == 2, '_caom2_store call count'
        assert file_info_mock.call_count == 2, 'wrong file info call count'
        info_calls = [call(f_uri1, f_name1), call(f_uri2, f_name2)]
        file_info_mock.assert_has_calls(info_calls), 'file_info calls'
        assert headers_mock.call_count == 2, 'wrong headers call count'
        headers_mock.assert_has_calls(info_calls), 'headers calls'
    finally:
        os.chdir(cwd)


@patch('wallaby2caom2.composable.Client', autospec=True)
@patch('caom2pipe.client_composable.ClientCollection', autospec=True)
@patch('caom2pipe.execute_composable.OrganizeExecutes.do_one', autospec=True)
def test_run_remote(run_mock, clients_mock, vo_client_mock, test_config, tmp_path):
    test_obs_id = 'WALLABY_J124915+043926'
    test_f_name = 'WALLABY_J124915+043926_NGC_4808_High-Res_Kin_TR1_ModCube.fits'

    node1 = type('', (), {})()
    node1.props = {
        'date': '2020-09-15 19:55:03.067000+00:00',
        'size': 14,
    }
    node1.uri = f'vos://cadc.nrc.ca!vault/goliaths/moc/{test_f_name}'
    node1.type = 'vos:DataNode'
    node1.node_list = [node1]
    vo_client_mock.return_value.get_node.return_value = node1

    orig_dir = os.getcwd()
    test_config.data_sources = ['vos:goliaths/moc']
    test_config.change_working_directory(tmp_path)
    test_config.proxy_file_name = 'cadcproxy.pem'
    try:
        os.chdir(tmp_path)
        mc.Config.write_to_file(test_config)
        with open(test_config.proxy_fqn, 'w') as f:
            f.write('test content')

        # execution
        composable._run_remote()
        assert run_mock.called, 'should have been called'
        args, kwargs = run_mock.call_args
        test_storage = args[1]
        assert isinstance(test_storage, storage_name.WallabyName), type(test_storage)
        assert test_storage.obs_id == test_obs_id, 'wrong obs id'
        assert test_storage.file_name == test_f_name, 'wrong file name'
    finally:
        os.chdir(orig_dir)
