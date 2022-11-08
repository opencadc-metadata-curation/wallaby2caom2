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

import logging
import traceback

from math import sqrt

from caom2 import ProductType, TypedOrderedDict, Part
from caom2pipe import astro_composable as ac
from caom2pipe.caom_composable import TelescopeMapping
from caom2pipe import manage_composable as mc
from wallaby2caom2 import storage_name as sn


__all__ = ['Telescope']


class WallabyValueRepair(mc.ValueRepairCache):

    VALUE_REPAIR = {
        'chunk.energy.axis.axis.cunit': {
            'm / s': 'm/s',
        }
    }

    def __init__(self):
        self._value_repair = WallabyValueRepair.VALUE_REPAIR
        self._key = None
        self._values = None
        self._logger = logging.getLogger(self.__class__.__name__)


class Telescope(TelescopeMapping):

    value_repair = WallabyValueRepair()

    def __init__(self, storage_name, headers):
        super().__init__(storage_name, headers)

    def accumulate_blueprint(self, bp, application=None):
        """Configure the VLASS-specific ObsBlueprint for the CAOM model
        SpatialWCS."""
        self._logger.debug('Begin accumulate_wcs.')
        product_type = self.get_product_type(0)
        if product_type == ProductType.SCIENCE:
            bp.configure_position_axes((1, 2))
            bp.configure_energy_axis(3)
            bp.configure_polarization_axis(4)

        super().accumulate_blueprint(bp, sn.APPLICATION)

        # observation level
        bp.set('Observation.type', 'OBJECT')

        # over-ride use of value from default keyword 'DATE'
        bp.set('Observation.metaRelease', '2022-01-01')
        #bp.clear('Observation.metaRelease')
        #bp.add_attribute('Observation.metaRelease', 'DATE-OBS')

        #bp.clear('Observation.target.name')
        #bp.add_attribute('Observation.target.name', 'FILNAM04')
        #bp.set('Observation.target.type', 'field')

        # Clare Chandler via JJK - 21-08-18
        bp.set('Observation.instrument.name', 'ASKAP')
        # From JJK - 27-08-18 - slack
        bp.set('Observation.proposal.title', 'WALLABY')
        bp.set('Observation.proposal.project', 'WALLABY')
        bp.set('Observation.proposal.id', 'get_proposal_id(uri)')

        # plane level
        bp.set('Plane.calibrationLevel', '2')
        bp.set('Plane.dataProductType', 'cube')

        # Clare Chandler via slack - 28-08-18
        bp.clear('Plane.provenance.name')
        bp.add_attribute('Plane.provenance.name', 'ORIGIN')
        bp.set('Plane.provenance.producer', 'CSIRO')
        # From JJK - 27-08-18 - slack
        bp.set('Plane.provenance.project', 'WALLABY')
        #bp.clear('Plane.provenance.runID')
        #bp.add_attribute('Plane.provenance.runID', 'FILNAM08')
        #bp.clear('Plane.provenance.lastExecuted')
        #bp.add_attribute('Plane.provenance.lastExecuted', 'DATE')

        # VLASS data is public, says Eric Rosolowsky via JJK May 30/18
        bp.clear('Plane.metaRelease')
        bp.set('Plane.metaRelease', '2022-01-01')
        bp.clear('Plane.dataRelease')
        bp.set('Plane.dataRelease', '2022-01-01')

        # artifact level
        bp.clear('Artifact.productType')
        bp.set('Artifact.productType', 'get_product_type(uri)')
        bp.set('Artifact.releaseType', 'data')

        # chunk level
        if product_type == ProductType.SCIENCE:
            bp.clear('Chunk.position.axis.function.cd11')
            bp.clear('Chunk.position.axis.function.cd22')
            bp.add_attribute('Chunk.position.axis.function.cd11', 'CDELT1')
            bp.set('Chunk.position.axis.function.cd12', 0.0)
            bp.set('Chunk.position.axis.function.cd21', 0.0)
            bp.add_attribute('Chunk.position.axis.function.cd22', 'CDELT2')

        # Clare Chandler via JJK - 21-08-18
        bp.set('Chunk.energy.bandpassName', 'L-band')
        bp.add_attribute('Chunk.energy.restfrq', 'RESTFREQ')
        self._logger.debug('End accumulate_wcs')

    def get_position_resolution(self, ext):
        if 'BMAJ' in self._headers[ext]:
            bmaj = self._headers[ext]['BMAJ']
            bmin = self._headers[ext]['BMIN']
            # From
            # https://open-confluence.nrao.edu/pages/viewpage.action?pageId=13697486
            # Clare Chandler via JJK - 21-08-18
            return 3600.0 * sqrt(bmaj * bmin)
        else:
            return 0.0

    def get_product_type(self, ext):
        if '.rms.' in self._storage_name.file_uri:
            return ProductType.NOISE
        elif self._storage_name.file_uri.endswith('.png'):
            return ProductType.THUMBNAIL
        elif (
            self._storage_name.file_uri.endswith('.txt')
            or 'ModelGeometry' in self._storage_name.file_uri
            or 'ModelRotationCurve' in self._storage_name.file_uri
            or 'ModelSurfaceDensity' in self._storage_name.file_uri
        ):
            return ProductType.AUXILIARY
        else:
            return ProductType.SCIENCE

    def get_proposal_id(self, ext):
        caom_name = mc.CaomName(self._storage_name.file_uri)
        bits = caom_name.file_name.split('.')
        return f'{bits[0]}.{bits[1]}'

    def get_time_refcoord_value(self, ext):
        dateobs = self._headers[ext].get('DATE-OBS')
        if dateobs is not None:
            result = ac.get_datetime(dateobs)
            if result is not None:
                return result.mjd
            else:
                return None

    def _update_artifact(self, artifact, clients=None):
        self._logger.debug(f'Begin _update_artifact for {artifact.uri}')
        if '.txt' in artifact.uri:
            return
        if artifact.product_type == ProductType.AUXILIARY:
            artifact.parts = TypedOrderedDict(Part,)
            return
        delete_these_parts = []
        for part in artifact.parts.values():
            for chunk in part.chunks:
                if chunk.position is not None:
                    chunk.position.resolution = self.get_position_resolution(0)
                if (
                    chunk.energy is not None
                    and chunk.energy_axis is None
                ):
                    chunk.energy_axis = chunk.naxis

                if (
                    chunk.position is None
                    and chunk.energy is None
                    and chunk.time is None
                    and chunk.polarization is None
                    and chunk.naxis is not None
                ):
                    # _spec files have a second BINTABLE HDU,
                    # with no WCS captured in C* keywords
                    delete_these_parts.append(part.name)

        for entry in delete_these_parts:
            artifact.parts.popitem(entry)
            self._logger.info(f'Remove part {entry} with no WCS.')
        if artifact.uri.startswith('vos:cirada'):
            old_uri = artifact.uri
            artifact.uri = old_uri.replace(
                'vos:cirada', 'vos://cadc.nrc.ca~vault/cirada'
            )
            self._logger.info(
                f'Change URI from {old_uri} to {artifact.uri}'
            )
        self._logger.debug('Done update.')

    def update(self, observation, file_info, clients=None):
        """
        Update the Artifact file-based metadata. Override if it's necessary
        to carry out more/different updates.

        :param observation: Observation instance
        :param file_info: FileInfo instance
        :param clients: ClientCollection instance
        :return:
        """
        self._logger.debug(f'Begin update for {observation.observation_id}.')
        try:
            super().update(observation, file_info, clients)
            Telescope.value_repair.repair(observation)
            self._logger.debug('Done update.')
            return observation
        except mc.CadcException as e:
            tb = traceback.format_exc()
            self._logger.debug(tb)
            self._logger.error(e)
            self._logger.error(
                f'Terminating ingestion for {observation.observation_id}'
            )
            return None
