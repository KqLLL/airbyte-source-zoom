#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
from typing import Any, List, Mapping, Tuple

from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_protocol.models import SyncMode

from airbyte_source_zoom.components import ServerToServerOauthAuthenticator
from airbyte_source_zoom.streams import Users, Meetings, MeetingInstances, MeetingParticipants


# Source
class SourceZoom(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        """
        :param config:  the user-input config object conforming to the connector's spec.yaml
        :param logger:  logger object
        :return Tuple[bool, any]: (True, None) if the input config can be used to connect to the API successfully, (False, error) otherwise.
        """
        try:
            auth = ServerToServerOauthAuthenticator(
                config=config,
                parameters=config,
                account_id=config["account_id"],
                client_id=config["client_id"],
                client_secret=config["client_secret"],
                authorization_endpoint=config["authorization_endpoint"],
            )
            records = Users(authenticator=auth).read_records(sync_mode=SyncMode.full_refresh)
            next(records)

        except StopIteration:
            # there are no records, but connection was fine
            return True, None
        except Exception as e:
            return False, f"Unable to connect to Zoom API with the provided credentials - {e}"

        return True, None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        """
        :param config: A Mapping of the user input configuration as defined in the connector spec.
        """

        auth = ServerToServerOauthAuthenticator(
            config=config,
            parameters=config,
            account_id=config["account_id"],
            client_id=config["client_id"],
            client_secret=config["client_secret"],
            authorization_endpoint=config["authorization_endpoint"],
        )  # Oauth2Authenticator is also available if you need oauth support

        return [
            Users(authenticator=auth),
            Meetings(
                authenticator=auth,
                meeting_type=config["meeting_type"]
            ),
            MeetingInstances(
                authenticator=auth,
                meeting_type=config["meeting_type"],
                start_datetime=config["start_datetime"]
            ),
            MeetingParticipants(
                authenticator=auth,
                start_datetime=config["start_datetime"],
                meeting_type=config["meeting_type"]
            ),
        ]
