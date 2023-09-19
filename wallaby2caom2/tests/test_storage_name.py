# -*- coding: utf-8 -*-
# ***********************************************************************
# ******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
# *************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
#
#  (c) 2019.                            (c) 2019.
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

from os.path import basename

from wallaby2caom2 import storage_name as sn


def test_storage_name(test_config):
    target_name = 'NGC_4808_TR1'
    test_f_names = {
        'WALLABY_J124915+043926_NGC_4808_High-Res_Kin_TR1_FullResProcData.fits': 'kinematic_model_highres',
        'WALLABY_J124915+043926_NGC_4808_High-Res_TR1_spec.fits': 'source_data_highres',
        'WALLABY_J124915+043926_NGC_4808_Kin_TR1_FullResProcData.fits': 'kinematic_model',
        'WALLABY_J124915+043926_NGC_4808_TR1_spec.fits': 'source_data',
    }
    for test_f_name, product_id_prefix in test_f_names.items():
        test_url = (
            f'vos:cirada/emission/PilotFieldReleases_Jun2021/'
            f'KinematicModels/Wallaby_Hydra_DR2_KinematicModels_v2/'
            f'WALLABY_J100342-270137/{test_f_name}'
        )
        expected_obs_id = 'WALLABY_J124915+043926'
        expected_product_id = f'{product_id_prefix}_{target_name}'
        expected_fid = basename(test_url).replace('.fits', '')
        for ii in [test_url, test_f_name]:
            ts = sn.WallabyName(ii)
            assert ts.obs_id == expected_obs_id, 'wrong obs id'
            assert ts.product_id == expected_product_id, 'wrong product id'
            assert ts.file_name == basename(test_url), 'wrong fname'
            assert ts.file_id == expected_fid, 'wrong fid'
            assert (
                ts.file_uri == f'{test_config.scheme}:{test_config.collection}/{basename(test_url)}'
            ), 'wrong uri'
            assert ts.prev is None, 'wrong preview'
            assert ts.thumb == f'{expected_fid}_prev_256.png', 'wrong thumbnail'
            assert (
                ts.prev_uri == f'{test_config.preview_scheme}:{test_config.collection}/{ts.prev}'
            ), 'wrong preview uri'
            assert (
                ts.thumb_uri == f'{test_config.preview_scheme}:{test_config.collection}/{ts.thumb}'
            ), 'wrong thumbnail uri'
            assert len(ts.source_names) == 1, 'wrong length'


def test_preview(test_config):
    test_f_name = 'WALLABY_J124915+043926_NGC_4808_High-Res_Kin_TR1_DiagnosticPlot.png'
    ts = sn.WallabyName(test_f_name)
    assert ts.thumb == test_f_name.replace('.png', '') + '_prev_256.png'
