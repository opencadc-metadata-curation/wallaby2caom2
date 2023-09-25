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


__all__ = ['DR2', 'Telescope']


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

    def __init__(self, storage_name, headers, clients, observable, observation, config):
        super().__init__(storage_name, headers, clients, observable, observation, config)

    def accumulate_blueprint(self, bp):
        """Configure the VLASS-specific ObsBlueprint for the CAOM model
        SpatialWCS."""
        self._logger.debug('Begin accumulate_wcs.')
        product_type = self._storage_name.get_product_type()
        if product_type == ProductType.SCIENCE:
            bp.configure_position_axes((1, 2))
            bp.configure_energy_axis(3)
            bp.configure_polarization_axis(4)

        super().accumulate_blueprint(bp)

        # observation level
        bp.set('Observation.type', 'OBJECT')
        bp.set('Observation.metaRelease', '2023-01-01')
        bp.set('Observation.instrument.name', 'ASKAP')
        bp.set('Observation.proposal.title', 'WALLABY')
        bp.set('Observation.proposal.project', 'WALLABY')
        bp.set('Observation.proposal.id', self._storage_name.get_proposal_id())

        # plane level
        bp.set('Plane.calibrationLevel', self._storage_name.get_calibration_level())
        bp.set('Plane.dataProductType', self._storage_name.get_data_product_type())

        bp.clear('Plane.provenance.name')
        bp.add_attribute('Plane.provenance.name', 'ORIGIN')
        bp.set('Plane.provenance.producer', 'CSIRO')
        bp.set('Plane.provenance.project', 'WALLABY')

        bp.set('Plane.metaRelease', '2023-01-01')
        bp.set('Plane.dataRelease', '2023-01-01')

        # artifact level
        bp.clear('Artifact.productType')
        bp.set('Artifact.productType', self._storage_name.get_product_type())
        bp.set('Artifact.releaseType', 'data')

        # chunk level
        if product_type == ProductType.SCIENCE:
            bp.clear('Chunk.position.axis.function.cd11')
            bp.clear('Chunk.position.axis.function.cd22')
            bp.add_attribute('Chunk.position.axis.function.cd11', 'CDELT1')
            bp.set('Chunk.position.axis.function.cd12', 0.0)
            bp.set('Chunk.position.axis.function.cd21', 0.0)
            bp.add_attribute('Chunk.position.axis.function.cd22', 'CDELT2')
            bp.set('Chunk.position.resolution', 'get_position_resolution()')

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

    def _update_artifact(self, artifact):
        self._logger.debug(f'Begin _update_artifact for {artifact.uri}')
        if '.txt' in artifact.uri:
            return
        if artifact.product_type == ProductType.AUXILIARY:
            artifact.parts = TypedOrderedDict(Part,)
            return
        delete_these_parts = []
        for part in artifact.parts.values():
            for chunk in part.chunks:
                if (
                    chunk.position is None
                    and chunk.energy is None
                    and chunk.time is None
                    and chunk.polarization is None
                    and chunk.naxis is not None
                ):
                    # some files have a second BINTABLE HDU with no WCS captured in C* keywords
                    delete_these_parts.append(part.name)
                
                if (
                    chunk.energy is not None 
                    and chunk.naxis is not None 
                    and chunk.naxis == 3 
                    and chunk.energy_axis is None
                ):
                    chunk.energy_axis = 3

            for entry in delete_these_parts:
                artifact.parts.popitem(entry)
                self._logger.info(f'Remove part {entry} with no WCS.')
        self._logger.debug('Done update.')

    def update(self, file_info):
        """
        Update the Artifact file-based metadata. Override if it's necessary
        to carry out more/different updates.

        :param file_info: FileInfo instance
        :return:
        """
        self._logger.debug(f'Begin update for {self._observation.observation_id}.')
        try:
            super().update(file_info)
            Telescope.value_repair.repair(self._observation)
            self._logger.debug('Done update.')
            return self._observation
        except mc.CadcException as e:
            tb = traceback.format_exc()
            self._logger.debug(tb)
            self._logger.error(e)
            self._logger.error(
                f'Terminating ingestion for {self._observation.observation_id}'
            )
            return None


class DR2(Telescope):

    def __init__(self, storage_name, headers, clients, observable, observation, config):
        super().__init__(storage_name, headers, clients, observable, observation, config)

    def accumulate_blueprint(self, bp):
        super().accumulate_blueprint(bp)
        bp.set('Observation.metaRelease', '2025-01-01')
        bp.set('Plane.metaRelease', '2025-01-01')
        bp.set('Plane.dataRelease', '2025-01-01')

        bp.clear('Observation.target.name')
        bp.add_attribute('Observation.target.name', 'OBJECT')
        bp.set('Observation.target.type', 'field')

        self._logger.debug('End accumulate_wcs')
