# ***********************************************************************
# ******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
# *************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
#
#  (c) 2024.                            (c) 2024.
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

from caom2.observation import Observation
from caom2pipe.caom_composable import copy_plane
from wallaby2caom2.storage_name import WallabyName


class ObservationUpdater():

    logger = logging.getLogger('ObservationUpdater')

    def update(self, observation, **kwargs):
        delete_planes = []
        new_planes = {}
        for plane in observation.planes.values():
            self.logger.error(f'Checking plane {plane.product_id}')
            for artifact in plane.artifacts.values():
                storage_name = WallabyName(artifact.uri)
                if storage_name.product_id != plane.product_id:
                    # self.logger.error(f'{storage_name.product_id} {plane.product_id}')
                    new_plane = new_planes.get(storage_name.product_id)
                    if new_plane is None:
                        new_plane = copy_plane(plane, storage_name.product_id)
                        new_planes[storage_name.product_id] = new_plane
                    self.logger.error(f'Re-homing {artifact.uri} from {plane.product_id} to {storage_name.product_id}')
                    new_plane.artifacts.add(artifact)
                    delete_planes.append(plane.product_id)

        temp = list(set(delete_planes))
        self.logger.error(f'Deleting {len(temp)} planes')
        for product_id in temp:
            self.logger.error(f'Deleting product_id {product_id} from {observation.observation_id}')
            observation.planes.pop(product_id)

        self.logger.error(f'Adding {len(new_planes)} planes')
        for product_id, new_plane in new_planes.items():
            self.logger.error(f'Adding plane {product_id} to {observation.observation_id}')
            observation.planes.add(new_plane)
        return True



    # def update(self, observation, **kwargs):
    #     """
    #     Processes an observation and updates it
    #     """
    #     assert isinstance(observation, Observation), (
    #         "observation %s is not an Observation".format(observation))
    #     print("Observation: {}".format(observation.observation_id))
    #     for plane in observation.planes.values():
    #         for artifact in plane.artifacts.values():
    #             if artifact.uri.startswith('ad:'):
    #                 olduri = artifact.uri
    #                 newuri = artifact.uri.replace('ad:', 'cadc:')
    #                 artifact.uri = newuri
    #                 print('\t{} -> {}'.format(olduri, artifact.uri))
    #     return True