# -*- coding: utf-8 -*-
# ***********************************************************************
# ******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
# *************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
#
#  (c) 2018.                            (c) 2018.
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
import shutil

from caom2pipe import manage_composable as mc

from wallaby2caom2 import time_bounds_augmentation, quality_augmentation
from wallaby2caom2 import position_bounds_augmentation
from wallaby2caom2 import storage_name as sn

THIS_DIR = os.path.dirname(os.path.realpath(__file__))
TEST_DATA_DIR = os.path.join(THIS_DIR, 'data')
TEST_URI = 'ad:VLASS/VLASS1.1.ql.T01t01.J000228-363000.10.2048.v1.I.iter1.' \
           'image.pbcor.tt0.rms.subim.fits'


def test_aug_visit():
    with pytest.raises(mc.CadcException):
        time_bounds_augmentation.visit(None)
    with pytest.raises(mc.CadcException):
        quality_augmentation.visit(None)


@pytest.mark.skip('')
def test_aug_visit_position_bounds():
    test_file_id = 'VLASS1.2.ql.T24t07.J065836+563000.10.2048.v1.I.' \
                   'iter1.image.pbcor.tt0.subim'
    test_input_file = f'/test_files/{test_file_id}.fits'
    if not os.path.exists(test_input_file):
        shutil.copy(f'/usr/src/app/wallaby2caom2/int_test/test_files/'
                    f'{test_file_id}.fits',
                    test_input_file)
    test_file = os.path.join(TEST_DATA_DIR, 'fpf_start_obs.xml')
    test_obs = mc.read_obs_from_file(test_file)
    test_storage_name = sn.WallabyName(test_input_file)
    kwargs = {'working_directory': '/test_files',
              'storage_name': test_storage_name,
              'log_file_directory': os.path.join(TEST_DATA_DIR, 'logs')}
    test_result = position_bounds_augmentation.visit(test_obs, **kwargs)
    assert test_result is not None, 'should have a result status'
    assert len(test_result) == 1, 'modified chunks count'
    assert test_result['chunks'] == 2, 'chunk count'
    return_file = os.path.join(THIS_DIR, f'{test_file_id}__footprint.txt')
    assert not os.path.exists(return_file), 'bad cleanup'
    if os.path.exists(test_input_file):
        os.unlink(test_input_file)
