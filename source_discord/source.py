from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple

import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources.streams.http import HttpStream, HttpSubStream
from airbyte_cdk.sources.streams.http.auth.token import TokenAuthenticator
from airbyte_cdk.sources.streams.http.requests_native_auth import Oauth2Authenticator
import os
import json

# Oauth 2.0 authentication URL for amazon
TOKEN_URL = "https://discord.com/api/oauth2/token"

# Basic full refresh stream
class DiscordStream(HttpStream, ABC):
    url_base = "https://discord.com/api/"

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        return {}

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        json_response = response.json()
        records = json_response.get(self.data_field, []) if self.data_field is not None else json_response
        yield from records

class DiscordSubStream(HttpSubStream):

    def stream_slices(
        self, sync_mode: SyncMode, cursor_field: List[str] = None, stream_state: Mapping[str, Any] = None
    ) -> Iterable[Optional[Mapping[str, Any]]]:
        parent_stream_slices = self.parent.stream_slices(
            sync_mode=SyncMode.full_refresh, cursor_field=cursor_field, stream_state=stream_state
        )
        # iterate over all parent stream_slices
        for stream_slice in parent_stream_slices:
            parent_records = self.parent.read_records(sync_mode=SyncMode.full_refresh, stream_slice=stream_slice)

            # iterate over all parent records with current stream_slice
            for record in parent_records:
                yield {"parent": record, "sub_parent": stream_slice}

class CurrentUser(DiscordStream):
    primary_key = "id"
    data_field = None

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "users/@me"

class CurrentUserGuilds(DiscordStream):
    primary_key = "id"
    data_field = None

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "users/@me/guilds"

    def request_params(self, stream_state: Mapping[str,Any] = None , stream_slice: Mapping[str,Any] = None, next_page_token: Mapping[str,Any] = None):
        params = {"limit":2}
        if next_page_token: params.update(next_page_token)
        return params

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        json_response = response.json()
        records = json_response.get(self.data_field, []) if self.data_field is not None else json_response
        last_record_id = {"after":records[-1]["id"]} if len(records)>0 else None
        return last_record_id

    @property
    def use_cache(self) -> bool:
        return True

class Guilds(DiscordSubStream, DiscordStream):
    primary_key = "id"
    data_field = None

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        guild_id = stream_slice['parent']['id']
        return f"guilds/{guild_id}"

# Source
class SourceDiscord(AbstractSource):
    def custom_authentication(self,config):
        auth_method = 'Bot'
        auth_header = 'Authorization'
        _token = config["token"]
        
        return TokenAuthenticator(token=_token, auth_method= auth_method, auth_header= auth_header)

    def check_connection(self, logger, config) -> Tuple[bool, any]:
        
        authbot = self.custom_authentication(config)

        headers = authbot.get_auth_header()
        url = "https://discord.com/api/users/@me"
        try:
            session = requests.get(url, headers=headers)
            session.raise_for_status()
            return True, None
        except requests.exceptions.RequestException as e:
            return False, e

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        
        authbot = self.custom_authentication(config)

        return [CurrentUser(authenticator=authbot),CurrentUserGuilds(authenticator=authbot), Guilds(authenticator=authbot,config=config)]
