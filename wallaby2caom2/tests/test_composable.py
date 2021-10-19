# -*- coding: utf-8 -*-
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

from datetime import datetime, timezone, timedelta
from mock import patch, Mock

from cadctap import CadcTapClient
from caom2pipe import execute_composable as ec
from caom2pipe import manage_composable as mc
from caom2utils import get_gen_proc_arg_parser
from caom2 import SimpleObservation, Algorithm
from wallaby2caom2 import composable, WallabyName, COLLECTION, scrape
from wallaby2caom2 import SCHEME, APPLICATION
import test_main_app
import test_scrape

STATE_FILE = os.path.join(test_main_app.TEST_DATA_DIR, 'state.yml')


@patch('caom2pipe.manage_composable.query_endpoint_session')
@patch('caom2pipe.execute_composable.CaomExecute._fits2caom2_cmd')
@patch('caom2pipe.client_composable.CAOM2RepoClient')
@patch('caom2pipe.client_composable.StorageClientWrapper')
def test_run(
    data_client_mock, repo_mock, exec_mock, query_endpoint_mock
):
    query_endpoint_mock.side_effect = test_scrape._query_endpoint
    repo_mock.return_value.read.side_effect = _mock_repo_read
    repo_mock.return_value.create.side_effect = Mock()
    repo_mock.return_value.update.side_effect = _mock_repo_update
    data_client_mock.return_value.get_file_info.side_effect = (
        _mock_get_file_info
    )

    exec_mock.side_effect = _cmd_direct_mock

    getcwd_orig = os.getcwd
    os.getcwd = Mock(return_value=test_main_app.TEST_DATA_DIR)

    test_config = mc.Config()
    test_config.get_executors()

    test_f_name = 'VLASS1.2.ql.T07t13.J083838-153000.10.2048.v1.I.iter1.' \
                  'image.pbcor.tt0.subim.fits'
    with open(test_config.work_fqn, 'w') as f:
        f.write(f'{test_f_name}\n')

    # the equivalent of calling work.init_web_log()
    scrape.web_log_content['abc'] = 123

    try:
        # execution
        test_result = composable._run()
        assert test_result == 0, 'wrong result'
    finally:
        os.getcwd = getcwd_orig
        if os.path.exists(test_config.work_fqn):
            os.unlink(test_config.work_fqn)


@patch('wallaby2caom2.to_caom2')
@patch('caom2pipe.manage_composable.query_endpoint_session')
@patch('caom2pipe.client_composable.CAOM2RepoClient')
@patch('caom2pipe.client_composable.StorageClientWrapper')
@patch('cadcdata.CadcDataClient.get_file_info')
def test_run_state(
    get_file_info_mock,
    data_client_mock,
    repo_client_mock,
    query_mock,
    to_caom2_mock,
):
    test_start_time = datetime.now(tz=timezone.utc) - timedelta(minutes=5)
    test_bookmark = {
        'bookmarks': {
            'wallaby_timestamp': {'last_record': test_start_time},
        },
    }
    mc.write_as_yaml(test_bookmark, STATE_FILE)

    query_mock.side_effect = test_scrape._query_endpoint
    repo_client_mock.return_value.read.return_value = None
    to_caom2_mock.side_effect = _write_obs_mock
    get_file_info_mock.side_effect = _mock_get_file_info
    getcwd_orig = os.getcwd
    os.getcwd = Mock(return_value=test_main_app.TEST_DATA_DIR)
    try:
        test_result = composable._run_state()
        assert test_result is not None, 'expect result'
        assert test_result == 0, 'expect success'
    finally:
        os.getcwd = getcwd_orig
        for entry in [STATE_FILE]:
            if os.path.exists(entry):
                os.unlink(entry)


def test_store():
    test_config = mc.Config()
    test_config.logging_level = 'ERROR'
    test_config.working_directory = '/tmp'
    test_url = (
        'https://archive-new.nrao.edu/vlass/quicklook/VLASS2.1/'
        'T10t12/VLASS2.1.ql.T10t12.J073401-033000.10.2048.v1/'
        'VLASS2.1.ql.T10t12.J073401-033000.10.2048.v1.I.iter1.image.'
        'pbcor.tt0.rms.subim.fits'
    )
    test_storage_name = WallabyName(test_url)
    transferrer = Mock()
    cadc_data_client = Mock()
    observable = mc.Observable(
        mc.Rejected('/tmp/rejected.yml'), mc.Metrics(test_config))
    test_subject = ec.Store(test_config, test_storage_name, APPLICATION,
                            cadc_data_client, observable, transferrer)
    test_subject.execute(None)
    assert cadc_data_client.put.called, 'expect a call'
    cadc_data_client.put.assert_called_with(
        '/tmp/VLASS2.1.T10t12.J073401-033000',
        f'{SCHEME}:{COLLECTION}/VLASS2.1.ql.T10t12.J073401-033000.10.2048.'
        f'v1.I.iter1.image.pbcor.tt0.rms.subim.fits',
        None,
    ), 'wrong put args'
    assert transferrer.get.called, 'expect a transfer call'
    test_f_name = 'VLASS2.1.ql.T10t12.J073401-033000.10.2048.v1.I.iter1.' \
                  'image.pbcor.tt0.rms.subim.fits'
    transferrer.get.assert_called_with(
        f'https://archive-new.nrao.edu/vlass/quicklook/VLASS2.1/T10t12/'
        f'VLASS2.1.ql.T10t12.J073401-033000.10.2048.v1/{test_f_name}',
        f'/tmp/VLASS2.1.T10t12.J073401-033000/{test_f_name}',
    ), 'wrong transferrer args'


def _cmd_direct_mock():
    from caom2 import SimpleObservation, Algorithm
    obs = SimpleObservation(
        observation_id='VLASS1.2.T07t13.J083838-153000',
        collection=COLLECTION,
        algorithm=Algorithm(name='testing'),
    )
    mc.write_obs_to_file(
        obs,
        os.path.join(
            test_main_app.TEST_DATA_DIR,
            'logs/VLASS1.2.T07t13.J083838-153000.xml',
        ),
    )


def _mock_service_query():
    return None


def _mock_get_file_info(arg1, arg2):
    # arg2 is the file name
    return {'name': arg2}


def _mock_get_file():
    return None


def _mock_repo_read(arg1, arg2):
    return None


def _mock_repo_update():
    assert True


def _mock_get_cadc_headers(archive, file_id):
    return {'md5sum': 'md5:abc123'}


def _mock_x(archive, file_id, b, fhead):
    import logging
    logging.error(f'{archive} {file_id} {fhead}')
    logging.error(f'\n\n\ncalled called called \n\n\n')
    from astropy.io import fits

    x = """SIMPLE  =                    T / Written by IDL:  Fri Oct  6 01:48:35 2017
BITPIX  =                  -32 / Bits per pixel
NAXIS   =                    2 / Number of dimensions
NAXIS1  =                 2048 /
NAXIS2  =                 2048 /
DATATYPE= 'REDUC   '           /Data type, SCIENCE/CALIB/REJECT/FOCUS/TEST
TYPE    = 'image  '
END
"""
    delim = '\nEND'
    extensions = \
        [e + delim for e in x.split(delim) if e.strip()]
    headers = [fits.Header.fromstring(e, sep='\n') for e in extensions]
    return headers


def _write_obs_mock():
    args = get_gen_proc_arg_parser().parse_args()
    obs = SimpleObservation(collection=args.observation[0],
                            observation_id=args.observation[1],
                            algorithm=Algorithm(name='exposure'))
    mc.write_obs_to_file(obs, args.out_obs_xml)
