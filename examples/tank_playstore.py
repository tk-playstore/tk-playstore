import os
from tank_vendor.six.moves import urllib
from tank_vendor.six.moves import http_client
from tank_vendor.shotgun_api3.lib import httplib2

from tank.descriptor import TankAppStoreConnectionError
from tank.descriptor import InvalidAppStoreCredentialsError
from tank.descriptor import TankAppStoreError

from tank import LogManager

from tank.constants import SUPPORT_EMAIL

# use api json to cover py 2.5
from tank_vendor import shotgun_api3
from tank_vendor import six

json = shotgun_api3.shotgun.json

from playstore_io_descriptor import IODescriptorPlayStoreBase

log = LogManager.get_logger(__name__)


class IODescriptorTankPlayStore(IODescriptorPlayStoreBase):
    CORE_VERSION_ENTITY_TYPE = "CustomNonProjectEntity01"
    CORE_DOWNLOAD_EVENT_TYPE = "TankAppStore_CoreApi_Download"

    # The entity representing Applications
    APP_ENTITY_TYPE = "CustomNonProjectEntity02"
    # The entity representing app versions.
    APP_VERSION_ENTITY_TYPE = "CustomNonProjectEntity05"
    # The EventLog event type to emit when downloading an app.
    APP_DOWNLOAD_EVENT_TYPE = "TankAppStore_App_Download"

    # The entity representing Engines
    ENGINE_ENTITY_TYPE = "CustomNonProjectEntity03"
    # The entity representing engine versions.
    ENGINE_VERSION_ENTITY_TYPE = "CustomNonProjectEntity04"
    # The EventLog event type to emit when downloading an engine.
    ENGINE_DOWNLOAD_EVENT_TYPE = "TankAppStore_Engine_Download"

    # The entity representing Frameworks
    FRAMEWORK_ENTITY_TYPE = "CustomNonProjectEntity13"
    # The entity representing framework versions.
    FRAMEWORK_VERSION_ENTITY_TYPE = "CustomNonProjectEntity09"
    # The EventLog event type to emit when downloading a framework.
    FRAMEWORK_DOWNLOAD_EVENT_TYPE = "TankAppStore_Framework_Download"

    # The entity representing Configs
    CONFIG_ENTITY_TYPE = "CustomNonProjectEntity07"
    # The entity representing config versions.
    CONFIG_VERSION_ENTITY_TYPE = "CustomNonProjectEntity08"
    # The EventLog event type to emit when downloading a config.
    CONFIG_DOWNLOAD_EVENT_TYPE = "TankAppStore_Config_Download"

    # Dummy project required when writing event data to the system
    EVENTLOG_PROJECT = {"type": "Project", "id": 64}
    # An environment variable to disable PlayStore downloading access
    DISABLE_PLAYSTORE_ACCESS_ENV_VAR = "SHOTGUN_DISABLE_APPSTORE_ACCESS"
    # name of the app store specific proxy setting
    PLAY_STORE_HTTP_PROXY = "app_store_http_proxy"
    # Name of the bundle cache folder on disk
    PLAY_STORE_DISK_NAME = "app_store"

    # The address of your PlayStore site
    SGTK_PLAY_STORE = "https://tank.shotgunstudio.com"

    @LogManager.log_timing
    def _create_sg_play_store_connection(self):
        """
        Creates a shotgun connection that can be used to access the Toolkit play store.
        :returns: (sg, dict) where the first item is the shotgun api instance and the second
                  is an sg entity dictionary (keys type/id) corresponding to to the user used
                  to connect to the play store.
        """
        # maintain a cache for performance
        # cache is keyed by client shotgun site
        # this assumes that there is a strict
        # 1:1 relationship between play store accounts
        # and shotgun sites.

        if os.environ.get(self.DISABLE_PLAYSTORE_ACCESS_ENV_VAR, "0") == "1":
            message = (
                "The '%s' environment variable is active, preventing connection to play store."
                % self.DISABLE_PLAYSTORE_ACCESS_ENV_VAR
            )
            log.debug(message)
            raise TankAppStoreConnectionError(message)

        sg_url = self._sg_connection.base_url

        if sg_url not in self._play_store_connections:

            # Connect to associated Shotgun site and retrieve the credentials to use to
            # connect to the play store site
            try:
                (script_name, script_key) = self._get_play_store_key_from_shotgun()
            except urllib.error.HTTPError as e:
                if e.code == 403:
                    # edge case alert!
                    # this is likely because our session token in shotgun has expired.
                    # The authentication system is based around wrapping the shotgun API,
                    # and requesting authentication if needed. Because the play store
                    # credentials is a separate endpoint and doesn't go via the shotgun
                    # API, we have to explicitly check.
                    #
                    # trigger a refresh of our session token by issuing a shotgun API call
                    self._sg_connection.find_one("HumanUser", [])
                    # and retry
                    (script_name, script_key) = self._get_play_store_key_from_shotgun()
                else:
                    raise

            log.debug("Connecting to %s..." % self.SGTK_PLAY_STORE)
            # Connect to the play store and resolve the script user id we are connecting with.
            # Set the timeout explicitly so we ensure the connection won't hang in cases where
            # a response is not returned in a reasonable amount of time.
            play_store_sg = shotgun_api3.Shotgun(
                self.SGTK_PLAY_STORE,
                script_name=script_name,
                api_key=script_key,
                http_proxy=self._get_play_store_proxy_setting(),
                connect=False,
            )
            # set the default timeout for play store connections
            play_store_sg.config.timeout_secs = self.SGTK_PLAY_STORE_CONN_TIMEOUT

            # determine the script user running currently
            # get the API script user ID from shotgun
            try:
                script_user = play_store_sg.find_one(
                    "ApiUser",
                    filters=[["firstname", "is", script_name]],
                    fields=["type", "id"],
                )
            except shotgun_api3.AuthenticationFault:
                raise InvalidAppStoreCredentialsError(
                    "The Toolkit PlayStore credentials found in Shotgun are invalid.\n"
                    "Please contact %s to resolve this issue." % SUPPORT_EMAIL
                )
            # Connection errors can occur for a variety of reasons. For example, there is no
            # internet access or there is a proxy server blocking access to the Toolkit play store.
            except (
                httplib2.HttpLib2Error,
                httplib2.socks.HTTPError,
                http_client.HTTPException,
            ) as e:
                raise TankAppStoreConnectionError(e)
            # In cases where there is a firewall/proxy blocking access to the play store, sometimes
            # the firewall will drop the connection instead of rejecting it. The API request will
            # timeout which unfortunately results in a generic SSLError with only the message text
            # to give us a clue why the request failed.
            # The exception raised in this case is "ssl.SSLError: The read operation timed out"
            except httplib2.ssl.SSLError as e:
                if "timed" in e.message:
                    raise TankAppStoreConnectionError(
                        "Connection to %s timed out: %s"
                        % (play_store_sg.config.server, e)
                    )
                else:
                    # other type of ssl error
                    raise TankAppStoreError(e)
            except Exception as e:
                raise TankAppStoreError(e)

            if script_user is None:
                raise TankAppStoreError(
                    "Could not evaluate the current PlayStore User! Please contact support."
                )

            self._play_store_connections[sg_url] = (play_store_sg, script_user)

        return self._play_store_connections[sg_url]

    @LogManager.log_timing
    def _get_play_store_key_from_shotgun(self):
        """
        Given a Shotgun url and script credentials, fetch the play store key
        for this shotgun instance using a special controller method.
        Returns a tuple with (play_store_script_name, play_store_auth_key)
        :returns: tuple of strings with contents (script_name, script_key)
        """
        sg = self._sg_connection

        log.debug("Retrieving play store credentials from %s" % sg.base_url)

        # handle proxy setup by pulling the proxy details from the main shotgun connection
        if sg.config.proxy_handler:
            opener = urllib.request.build_opener(sg.config.proxy_handler)
            urllib.request.install_opener(opener)

        # now connect to our site and use a special url to retrieve the play store script key
        session_token = sg.get_session_token()
        post_data = {"session_token": session_token}
        response = urllib.request.urlopen(
            "%s/api3/sgtk_install_script" % sg.base_url,
            six.ensure_binary(urllib.parse.urlencode(post_data)),
        )
        html = response.read()
        data = json.loads(html)

        if not data["script_name"] or not data["script_key"]:
            raise InvalidAppStoreCredentialsError(
                "Toolkit PlayStore credentials could not be retrieved from Shotgun.\n"
                "Please contact %s to resolve this issue." % SUPPORT_EMAIL
            )

        log.debug(
            "Retrieved play store credentials for account '%s'." % data["script_name"]
        )

        return data["script_name"], data["script_key"]