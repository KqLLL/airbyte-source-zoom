import time
import requests

from abc import ABC
from datetime import datetime
from urllib.parse import quote
from typing import Any, Iterable, Mapping, MutableMapping, Optional, List

from airbyte_cdk.sources.streams.core import StreamData
from airbyte_cdk.sources.streams.http import HttpStream, HttpSubStream
from airbyte_protocol.models import SyncMode

# TODO: Change to use state caching
meeting_instance_records = []


class ZoomStream(HttpStream, ABC):
    url_base = "https://api.zoom.us/"
    page_size = 300

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        """
        :param response: the most recent response from the API
        :return If there is another page in the result, a mapping (e.g: dict) containing information needed to query the next page in the response.
                If there are no more pages in the result, return None.
        """
        if response.json().get("next_page_token"):
            return {
                "page_number": response.json().get("page_number") + 1,
                "next_page_token": response.json().get("next_page_token")
            }

        return None

    def backoff_time(self, response: requests.Response) -> Optional[float]:
        """
        Rate Limit Label: Medium
        ratelimit-limit: 60
        ratelimit-remaining: 59
        ratelimit-reset: 1633883401
        Docs: https://developers.zoom.us/docs/api/rest/rate-limits/
        """
        reset_time = response.headers.get("ratelimit-reset")
        backoff_time = float(reset_time) - time.time() if reset_time else 30
        if backoff_time < 0:
            backoff_time = 30

        return backoff_time

    def request_params(
            self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None,
            next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        params = {"page_size": self.page_size}
        if next_page_token:
            params["next_page_token"] = next_page_token["next_page_token"]

        return params

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """
        TODO: Override this method to define how a response is parsed.
        :return an iterable containing each record in the response
        """
        yield {}


# Basic incremental stream
class IncrementalZoomStream(ZoomStream, ABC):
    state_checkpoint_interval = None

    @property
    def cursor_field(self) -> str:
        """
        :return str: The name of the cursor field.
        """
        return "start_time"

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> \
            Mapping[str, Any]:
        return {self.cursor_field: latest_record[self.cursor_field]}


class Users(ZoomStream):
    primary_key = "id"
    user_status = ["active", "inactive", "pending"]

    @property
    def use_cache(self) -> bool:
        return True

    def path(
            self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None,
            next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "/v2/users"

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """
        :return an iterable containing each record in the response
        """

        yield self.read_records(sync_mode=SyncMode.full_refresh, stream_state=None, stream_slice=None)

    def read_records(
            self,
            sync_mode: SyncMode,
            cursor_field: List[str] = None,
            stream_slice: Mapping[str, Any] = None,
            stream_state: Mapping[str, Any] = None,
    ) -> Iterable[StreamData]:
        user_records = []
        request_params = self.request_params(stream_state=stream_state, stream_slice=stream_slice)
        for status in self.user_status:
            request_params["status"] = status
            response = self._session.request(url=f'{self.url_base}v2/users', method="GET", params=request_params)
            user_records.extend(response.json().get("users"))

        yield from user_records


class Meetings(HttpSubStream, ZoomStream):
    primary_key = "id"

    def __init__(self, authenticator, meeting_type, **kwargs):
        super().__init__(
            authenticator=authenticator,
            parent=Users(authenticator=authenticator),
            **kwargs
        )

        self.meeting_type = meeting_type

    @property
    def use_cache(self) -> bool:
        return True

    def parse_response(self,
                       response: requests.Response,
                       stream_slice: Mapping[str, Any] = None,
                       stream_state: Mapping[str, Any] = None) -> Iterable[Mapping]:
        for meeting in response.json().get("meetings"):
            yield meeting
        else:
            if stream_slice["parent"].get("pmi"):
                yield self.get_personal_meeting(stream_slice["parent"].get("pmi"))

    def get_personal_meeting(self, pmi):
        resp = self._session.request(url=f'{self.url_base}v2/meetings/{pmi}', method="GET")

        return resp.json()

    def path(
            self,
            stream_state: Mapping[str, Any] = None,
            stream_slice: Mapping[str, Any] = None,
            next_page_token: Mapping[str, Any] = None
    ) -> str:
        user_id = stream_slice["parent"].get("id")

        return f'v2/users/{user_id}/meetings'


class MeetingInstances(HttpSubStream, IncrementalZoomStream):
    primary_key = "uuid"
    datetime_format = "%Y-%m-%dT%H:%M:%SZ"

    _state = dict()

    def __init__(self, authenticator, start_datetime, meeting_type, **kwargs):
        super().__init__(
            authenticator=authenticator,
            parent=Meetings(
                authenticator=authenticator,
                meeting_type=meeting_type,
                **kwargs
            ),
            **kwargs
        )
        self.start_datetime = datetime.strptime(start_datetime, self.datetime_format)
        self.meeting_type = meeting_type

    @property
    def use_cache(self) -> bool:
        return True

    @property
    def state(self) -> Mapping[str, Any]:
        if self._state:
            return self._state
        return {}

    @state.setter
    def state(self, value: Mapping[str, Any]):
        self._state = {key: value for state in (self._state, value) for key, value in state.items()}

    def parse_response(self,
                       response: requests.Response,
                       stream_slice: Mapping[str, Any] = None,
                       stream_state: Mapping[str, Any] = None) -> Iterable[Mapping]:

        meeting_id = stream_slice["parent"].get("id")
        meeting_instances = sorted(response.json().get("meetings"), key=lambda x: x.get(self.cursor_field))
        for meeting_instance in meeting_instances:
            meeting_instance["meeting_id"] = meeting_id

            """
            Non-recurring meeting IDs will expire 30 days after the meeting is scheduled and those meetings will 
            not be listed via the zoom API.So we need to save the meeting message in meeting_instance.
            """
            meeting_instance["host_id"] = stream_slice["parent"].get("host_id")
            meeting_instance["timezone"] = stream_slice["parent"].get("timezone")
            meeting_instance["topic"] = stream_slice["parent"].get("topic")
            meeting_instance["type"] = stream_slice["parent"].get("type")

            yield meeting_instance

    def path(
            self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None,
            next_page_token: Mapping[str, Any] = None
    ) -> str:
        meeting_id = stream_slice["parent"].get("id")

        return f'v2/past_meetings/{meeting_id}/instances'

    def read_records(
            self,
            sync_mode: SyncMode,
            cursor_field: List[str] = None,
            stream_slice: Mapping[str, Any] = None,
            stream_state: Mapping[str, Any] = None,
    ) -> Iterable[StreamData]:
        if stream_state is None:
            return

        meeting_instances = self._read_pages(
            lambda req, res, state, _slice:
            self.parse_response(res, stream_slice=_slice, stream_state=state), stream_slice, stream_state
        )

        meeting_id = stream_slice["parent"].get("id")
        start_datetime_str = self.start_datetime.strftime(self.datetime_format)
        initial_state = {self.cursor_field: start_datetime_str}
        max_meeting_cursor = start_datetime_str
        if str(meeting_id) not in self.state:
            self.state = {str(meeting_id): initial_state}
        else:
            max_meeting_cursor = self.state[str(meeting_id)].get(self.cursor_field)

        for meeting_instance in meeting_instances:
            meeting_cursor = meeting_instance.get(self.cursor_field)
            if meeting_cursor > max_meeting_cursor:
                max_meeting_cursor = meeting_cursor
                meeting_instance_records.append(meeting_instance)
                yield meeting_instance
        self.state[str(meeting_id)][self.cursor_field] = max_meeting_cursor


class MeetingParticipants(HttpSubStream, ZoomStream):
    primary_key = "id"

    def __init__(self, authenticator, start_datetime, meeting_type, **kwargs):
        super().__init__(
            authenticator=authenticator,
            parent=MeetingInstances(
                authenticator=authenticator,
                start_datetime=start_datetime,
                meeting_type=meeting_type,
                **kwargs
            ),
            **kwargs
        )
        self.start_time = start_datetime

    @property
    def use_cache(self) -> bool:
        return False

    def parse_response(self,
                       response: requests.Response,
                       stream_slice: Mapping[str, Any] = None,
                       stream_state: Mapping[str, Any] = None) -> Iterable[Mapping]:
        if not response.json().get("participants"):
            return {}
        meeting_instance_uuid = stream_slice["parent"].get("uuid")
        for participant in response.json().get("participants"):
            participant["meeting_uuid"] = meeting_instance_uuid

            yield participant

    def path(
            self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None,
            next_page_token: Mapping[str, Any] = None
    ) -> str:
        meeting_instance_uuid = stream_slice["parent"].get("uuid")
        meeting_instance_uuid_encode = quote(quote(meeting_instance_uuid, safe=''))

        return f"v2/past_meetings/{meeting_instance_uuid_encode}/participants"

    def get_error_display_message(self, exception: BaseException) -> Optional[str]:
        """
        :param exception: The exception that was raised
        :return: A user-friendly message that indicates the cause of the error
        """
        if isinstance(exception, requests.HTTPError):
            if exception.response.status_code == 400:
                self.logger.Info(f"Bad request: {exception.response.text}")
                return None
            if exception.response.status_code == 404:
                self.logger.Info(f"Meeting participants not found: {exception.response.text}")
                return None
            return self.parse_response_error_message(exception.response)
        return None

    def stream_slices(
            self, sync_mode: SyncMode, cursor_field: List[str] = None, stream_state: Mapping[str, Any] = None
    ) -> Iterable[Optional[Mapping[str, Any]]]:
        for record in meeting_instance_records:
            yield {"parent": record}
