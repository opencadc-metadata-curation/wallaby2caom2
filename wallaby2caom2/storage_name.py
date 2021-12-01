# ***********************************************************************
# ******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
# *************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
#
#  (c) 2021.                            (c) 2021.
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
#  : 4 $
#
# ***********************************************************************
#

from caom2 import ProductType
from caom2pipe import manage_composable as mc


__all__ = ['WallabyName']

CIRADA_SCHEME = 'cirada'
COLLECTION_PATTERN = '*'


class WallabyName(mc.StorageName):
    """Isolate the relationship between the observation id and the file names. """

    def __init__(self, entry=None, file_name=None, source_names=None):
        if file_name is None:
            file_name = mc.CaomName.extract_file_name(entry).replace('.header', '')
        if source_names is None:
            source_names = [entry]
        super().__init__(file_name=file_name, source_names=source_names)

    def get_calibration_level(self):
        result = 2
        if 'High-Res' in self._file_name:
            result = 3
        return result

    def get_data_product_type(self):
        result = 'cube'
        if '_mom' in self._file_name or '_snr' in self._file_name:
            result = 'image'
        return result

    def get_product_type(self):
        if '.rms.' in self._file_name:
            return ProductType.NOISE
        elif self._file_name.endswith('.png'):
            return ProductType.PREVIEW
        elif (
            self._file_name.endswith('.txt')
            or 'ModelGeometry' in self._file_name
            or 'ModelRotationCurve' in self._file_name
            or 'ModelSurfaceDensity' in self._file_name
        ):
            return ProductType.AUXILIARY
        else:
            return ProductType.SCIENCE

    def get_proposal_id(self):
        return self._obs_id

    def is_dr2(self):
        return '_High-Res_' in self._file_id

    @property
    def lineage(self):
        if self._vos_url is None:
            return f'{self.product_id}/{self.file_uri}'
        else:
            return f'{self.product_id}/{self._vos_url}'

    @property
    def prev(self):
        if '.png' in self._file_name:
            return self._file_name
        else:
            return ''

    @property
    def thumb(self):
        return f'{self._file_id}_prev_256.png'

    @property
    def version(self):
        return self._version

    def set_file_id(self):
        if self._file_name is not None:
            self._file_id = WallabyName.remove_extensions(self._file_name)

    def set_obs_id(self, **kwargs):
        bits = self._file_id.split('_')
        self._obs_id = f'{bits[0]}_{bits[1]}'

    def set_product_id(self, **kwargs):
        if 'SoFiA' in self._file_id or 'High-Res' in self._file_id:
            result = self._file_id
        else:
            ans = self._file_id.split("_")
            if "Kin" in ans:
                ans.remove("Kin")
            fans = "_".join(ans[2:-1])

            result = 'kinematic_model'+"_"+fans
            if (
                '_cube' in self._file_id
                or '_mom' in self._file_id
                or '_chan' in self._file_id
                or '_mask' in self._file_id
                or '_spec' in self._file_id
            ):
                result = 'source_data'+"_"+fans
        self._product_id = result

    @staticmethod
    def get_version(file_name):
        bits = file_name.split('_')
        return bits[-2]

    @staticmethod
    def remove_extensions(file_name):
        return file_name.replace('.fits', '').replace('.header', '').replace('.png', '')
