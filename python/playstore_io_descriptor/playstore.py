# Copyright (c) 2016 Shotgun Software Inc.
#
# CONFIDENTIAL AND PROPRIETARY
#
# This work is provided "AS IS" and subject to the Shotgun Pipeline Toolkit
# Source Code License included in this distribution package. See LICENSE.
# By accessing, using, copying or modifying this work you indicate your
# agreement to the Shotgun Pipeline Toolkit Source Code License. All rights
# not expressly granted therein are reserved by Shotgun Software Inc.

"""
Toolkit PlayStore Descriptor.
"""

import os
import fnmatch

from tank.util import shotgun
from tank.util import pickle
from tank.util import UnresolvableCoreConfigurationError, ShotgunAttachmentDownloadError
from tank.util.user_settings import UserSettings
from tank.descriptor import TankAppStoreError
from tank.descriptor import TankDescriptorError

from tank import LogManager
from tank.descriptor import constants
from tank.descriptor.io_descriptor.downloadable import IODescriptorDownloadable

# use api json to cover py 2.5
from tank_vendor import shotgun_api3

json = shotgun_api3.shotgun.json

log = LogManager.get_logger(__name__)


# file where we cache the app store metadata for an item
METADATA_FILE = ".cached_metadata.pickle"


class IODescriptorPlayStoreBase(IODescriptorDownloadable):
    """
    Represents a toolkit play store item.
    {type: play_store, name: tk-core, version: v12.3.4}
    {type: play_store, name: NAME, version: VERSION}
    """

    # cache play store connections for performance
    _play_store_connections = {}

    # The entity representing tk-core
    CORE_VERSION_ENTITY_TYPE = "CustomNonProjectEntity01"
    # The EventLog event type to emit when downloading tk-core
    CORE_DOWNLOAD_EVENT_TYPE = "PlayStore_CoreApi_Download"

    # The entity representing Applications
    APP_ENTITY_TYPE = "CustomNonProjectEntity02"
    # The entity representing app versions.
    APP_VERSION_ENTITY_TYPE = "CustomNonProjectEntity03"
    # The field on an AppVersion linking back to the App
    APP_LINK_FIELD = "sg_tank_app"
    # The EventLog event type to emit when downloading an app.
    APP_DOWNLOAD_EVENT_TYPE = "PlayStore_App_Download"

    # The entity representing Engines
    ENGINE_ENTITY_TYPE = "CustomNonProjectEntity04"
    # The entity representing engine versions.
    ENGINE_VERSION_ENTITY_TYPE = "CustomNonProjectEntity05"
    # The field on an EngineVersion linking back to the Engine
    ENGINE_LINK_FIELD = "sg_tank_engine"
    # The EventLog event type to emit when downloading an engine.
    ENGINE_DOWNLOAD_EVENT_TYPE = "PlayStore_Engine_Download"

    # The entity representing Frameworks
    FRAMEWORK_ENTITY_TYPE = "CustomNonProjectEntity06"
    # The entity representing framework versions.
    FRAMEWORK_VERSION_ENTITY_TYPE = "CustomNonProjectEntity07"
    # The field on an FrameworkVersion linking back to the Framework
    FRAMEWORK_LINK_FIELD = "sg_tank_framework"
    # The EventLog event type to emit when downloading a framework.
    FRAMEWORK_DOWNLOAD_EVENT_TYPE = "PlayStore_Framework_Download"

    # The entity representing Configs
    CONFIG_ENTITY_TYPE = "CustomNonProjectEntity08"
    # The entity representing config versions.
    CONFIG_VERSION_ENTITY_TYPE = "CustomNonProjectEntity09"
    # The field on an ConfigVersion linking back to the Config
    CONFIG_LINK_FIELD = "sg_tank_config"
    # The EventLog event type to emit when downloading a config.
    CONFIG_DOWNLOAD_EVENT_TYPE = "PlayStore_Config_Download"

    # The field containing the zip payload
    TANK_CODE_PAYLOAD_FIELD = "sg_payload"
    # Dummy project required when writing event data to the system
    EVENTLOG_PROJECT = {}
    # [Internal] An environment variable to expose "rev" entities
    PLAY_STORE_QA_MODE_ENV_VAR = "TANK_QA_ENABLED"
    # An environment variable to disable PlayStore downloading access
    DISABLE_PLAYSTORE_ACCESS_ENV_VAR = "SHOTGUN_DISABLE_PLAYSTORE_ACCESS"
    # name of the app store specific proxy setting
    PLAY_STORE_HTTP_PROXY = "play_store_http_proxy"
    # Name of the bundle cache folder on disk
    PLAY_STORE_DISK_NAME = "play_store"

    # The address of your PlayStore site
    SGTK_PLAY_STORE = "https://yoursite.shotgunstudio.com"
    # Timeout in secs to apply to TK play store connections
    SGTK_PLAY_STORE_CONN_TIMEOUT = 5


    @property
    def playstore_entity_mapping(self):
        return {
            constants.DESCRIPTOR_APP: self.APP_ENTITY_TYPE,
            constants.DESCRIPTOR_FRAMEWORK: self.FRAMEWORK_ENTITY_TYPE,
            constants.DESCRIPTOR_ENGINE: self.ENGINE_ENTITY_TYPE,
            constants.DESCRIPTOR_CONFIG: self.CONFIG_ENTITY_TYPE,
            constants.DESCRIPTOR_INSTALLED_CONFIG: None,
            constants.DESCRIPTOR_CORE: None,
        }
    
    @property
    def playstore_version_entity_mapping(self):
        return {
            constants.DESCRIPTOR_APP: self.APP_VERSION_ENTITY_TYPE,
            constants.DESCRIPTOR_FRAMEWORK: self.FRAMEWORK_VERSION_ENTITY_TYPE,
            constants.DESCRIPTOR_ENGINE: self.ENGINE_VERSION_ENTITY_TYPE,
            constants.DESCRIPTOR_CONFIG: self.CONFIG_VERSION_ENTITY_TYPE,
            constants.DESCRIPTOR_INSTALLED_CONFIG: None,
            constants.DESCRIPTOR_CORE: self.CORE_VERSION_ENTITY_TYPE,
        }

    @property
    def playstore_link_field_mapping(self):
        return {
            constants.DESCRIPTOR_APP: self.APP_LINK_FIELD,
            constants.DESCRIPTOR_FRAMEWORK: self.FRAMEWORK_LINK_FIELD,
            constants.DESCRIPTOR_ENGINE: self.ENGINE_LINK_FIELD,
            constants.DESCRIPTOR_CONFIG: self.CONFIG_LINK_FIELD,
            constants.DESCRIPTOR_INSTALLED_CONFIG: None,
            constants.DESCRIPTOR_CORE: None,
        }

    @property
    def playstore_download_event_mapping(self):
        return {
            constants.DESCRIPTOR_APP: self.APP_DOWNLOAD_EVENT_TYPE,
            constants.DESCRIPTOR_FRAMEWORK: self.FRAMEWORK_DOWNLOAD_EVENT_TYPE,
            constants.DESCRIPTOR_ENGINE: self.ENGINE_DOWNLOAD_EVENT_TYPE,
            constants.DESCRIPTOR_CONFIG: self.CONFIG_DOWNLOAD_EVENT_TYPE,
            constants.DESCRIPTOR_INSTALLED_CONFIG: None,
            constants.DESCRIPTOR_CORE: self.CORE_DOWNLOAD_EVENT_TYPE,
        }

    @property
    def version_fields_to_cache(self):
        return [
            "id",
            "code",
            "sg_status_list",
            "description",
            "tags",
            "sg_detailed_release_notes",
            "sg_documentation",
            self.TANK_CODE_PAYLOAD_FIELD,
        ]

    @property
    def bundle_fields_to_cache(self):
        return [
        "id",
        "sg_system_name",
        "sg_status_list",
        "sg_deprecation_message",
    ]

    def __init__(self, descriptor_dict, sg_connection, bundle_type):
        """
        Constructor
        :param descriptor_dict: descriptor dictionary describing the bundle
        :param sg_connection: Shotgun connection to associated site
        :param bundle_type: Either Descriptor.APP, CORE, ENGINE or FRAMEWORK or CONFIG
        :return: Descriptor instance
        """
        super(IODescriptorPlayStoreBase, self).__init__(
            descriptor_dict, sg_connection, bundle_type
        )

        self._validate_descriptor(
            descriptor_dict, required=["type", "name", "version"], optional=["label"]
        )

        self._sg_connection = sg_connection
        self._bundle_type = bundle_type
        self._name = descriptor_dict.get("name")
        self._version = descriptor_dict.get("version")
        self._label = descriptor_dict.get("label")

    def __str__(self):
        """
        Human readable representation
        """
        display_name_lookup = {
            constants.DESCRIPTOR_APP: "App",
            constants.DESCRIPTOR_FRAMEWORK: "Framework",
            constants.DESCRIPTOR_ENGINE: "Engine",
            constants.DESCRIPTOR_CONFIG: "Config",
            constants.DESCRIPTOR_CORE: "Core",
        }

        # Toolkit PlayStore App tk-multi-loader2 v1.2.3
        # Toolkit PlayStore Framework tk-framework-shotgunutils v1.2.3
        # Toolkit PlayStore Core v1.2.3
        if self._bundle_type == constants.DESCRIPTOR_CORE:
            display_name = "Toolkit PlayStore Core %s" % self._version
        else:
            display_name = display_name_lookup[self._bundle_type]
            display_name = "Toolkit PlayStore %s %s %s" % (
                display_name,
                self._name,
                self._version,
            )

        if self._label:
            display_name += " [label %s]" % self._label

        return display_name

    def _create_sg_play_store_connection(self):
        """
        Creates a shotgun connection that can be used to access the Toolkit play store.
        :returns: (sg, dict) where the first item is the shotgun api instance and the second
                  is an sg entity dictionary (keys type/id) corresponding to to the user used
                  to connect to the play store.
        """
        raise NotImplementedError

    def _load_cached_play_store_metadata(self, path):
        """
        Loads the metadata for a path in the play store
        :param path: path to bundle location on disk
        :return: metadata dictionary or None if not found
        """
        cache_file = os.path.join(path, METADATA_FILE)
        if os.path.exists(cache_file):
            fp = open(cache_file, "rb")
            try:
                metadata = pickle.load(fp)
            finally:
                fp.close()
        else:
            log.debug(
                "%r Could not find cached metadata file %s - "
                "will proceed with empty play store metadata." % (self, cache_file)
            )
            metadata = {}

        return metadata

    @LogManager.log_timing
    def _refresh_metadata(self, path, sg_bundle_data=None, sg_version_data=None):
        """
        Refreshes the metadata cache on disk. The metadata cache contains
        play store information such as deprecation status, label information
        and release note data.
        For performance, the metadata can be provided by the caller. If
        not provided, the method will retrieve it from the play store.
        If the descriptor resides in a read-only bundle cache, for example
        baked into a DCC distribution, the cache will not be updated.
        :param path: The path to the bundle where cache info should be written
        :param sg_bundle_data, sg_version_data: Shotgun data to cache
        :returns: A dictionary with keys 'sg_bundle_data' and 'sg_version_data',
                  containing Shotgun metadata.
        """
        log.debug("Attempting to refresh play store metadata for %r" % self)

        cache_file = os.path.join(path, METADATA_FILE)
        log.debug("Will attempt to refresh cache in %s" % cache_file)

        if (
            sg_version_data
        ):  # no none-check for sg_bundle_data param since this is none for tk-core
            log.debug("Will cache pre-fetched cache data.")
        else:
            log.debug("Connecting to Shotgun to retrieve metadata for %r" % self)

            # get the appropriate shotgun play store types and fields
            bundle_entity_type = self.playstore_entity_mapping[self._bundle_type]
            version_entity_type = self.playstore_version_entity_mapping[self._bundle_type]
            link_field = self.playstore_link_field_mapping[self._bundle_type]

            # connect to the play store
            (sg, _) = self._create_sg_play_store_connection()

            if self._bundle_type == self.CORE:
                # special handling of core since it doesn't have a high-level 'bundle' entity
                sg_bundle_data = None

                sg_version_data = sg.find_one(
                    self.CORE_VERSION_ENTITY_TYPE,
                    [["code", "is", self._version]],
                    self.version_fields_to_cache,
                )
                if sg_version_data is None:
                    raise TankDescriptorError(
                        "The Play Store does not have a version '%s' of Core!"
                        % self._version
                    )
            else:
                # engines, apps etc have a 'bundle level entity' in the play store,
                # e.g. something representing the app or engine.
                # then a version entity representing a particular version
                sg_bundle_data = sg.find_one(
                    bundle_entity_type,
                    [["sg_system_name", "is", self._name]],
                    self.bundle_fields_to_cache,
                )

                if sg_bundle_data is None:
                    raise TankDescriptorError(
                        "The Play Store does not contain an item named '%s'!"
                        % self._name
                    )

                # now get the version
                sg_version_data = sg.find_one(
                    version_entity_type,
                    [[link_field, "is", sg_bundle_data], ["code", "is", self._version]],
                    self.version_fields_to_cache,
                )
                if sg_version_data is None:
                    raise TankDescriptorError(
                        "The Play Store does not have a "
                        "version '%s' of item '%s'!" % (self._version, self._name)
                    )

        # create metadata
        metadata = {
            "sg_bundle_data": sg_bundle_data,
            "sg_version_data": sg_version_data,
        }

        # try to write to location - but it may be located in a
        # readonly bundle cache - if the caching fails, gracefully
        # fall back and log
        try:
            fp = open(cache_file, "wb")
            try:
                pickle.dump(metadata, fp)
                log.debug("Wrote play store metadata cache '%s'" % cache_file)
            finally:
                fp.close()
        except Exception as e:
            log.debug(
                "Did not update play store metadata cache '%s': %s" % (cache_file, e)
            )

        return metadata

    def _get_bundle_cache_path(self, bundle_cache_root):
        """
        Given a cache root, compute a cache path suitable
        for this descriptor, using the 0.18+ path format.
        :param bundle_cache_root: Bundle cache root path
        :return: Path to bundle cache location
        """
        return os.path.join(
            bundle_cache_root, self.PLAY_STORE_DISK_NAME, self.get_system_name(), self.get_version()
        )

    def _get_cache_paths(self):
        """
        Get a list of resolved paths, starting with the primary and
        continuing with alternative locations where it may reside.
        Note: This method only computes paths and does not perform any I/O ops.
        :return: List of path strings
        """
        # get default cache paths from base class
        paths = super(IODescriptorPlayStoreBase, self)._get_cache_paths()

        # for compatibility with older versions of core, prior to v0.18.x,
        # add the old-style bundle cache path as a fallback. As of v0.18.x,
        # the bundle cache subdirectory names were shortened and otherwise
        # modified to help prevent MAX_PATH issues on windows. This call adds
        # the old path as a fallback for cases where core has been upgraded
        # for an existing project. NOTE: This only works because the bundle
        # cache root didn't change (when use_bundle_cache is set to False).
        # If the bundle cache root changes across core versions, then this will
        # need to be refactored.
        legacy_folder = self._get_legacy_bundle_install_folder(
            self.PLAY_STORE_DISK_NAME,
            self._bundle_cache_root,
            self._bundle_type,
            self.get_system_name(),
            self.get_version(),
        )
        if legacy_folder:
            paths.append(legacy_folder)

        return paths

    ###############################################################################################
    # data accessors

    def get_system_name(self):
        """
        Returns a short name, suitable for use in configuration files
        and for folders on disk
        """
        return self._name

    def get_deprecation_status(self):
        """
        Returns information about deprecation.
        May download the item from the play store in order
        to retrieve the metadata.
        :returns: Returns a tuple (is_deprecated, message) to indicate
                  if this item is deprecated.
        """
        # make sure we have the app payload + metadata
        self.ensure_local()
        # grab metadata
        metadata = self._load_cached_play_store_metadata(self.get_path())
        sg_bundle_data = metadata.get("sg_bundle_data") or {}
        if sg_bundle_data.get("sg_status_list") == "dep":
            msg = sg_bundle_data.get("sg_deprecation_message", "No reason given.")
            return (True, msg)
        else:
            return (False, "")

    def get_version(self):
        """
        Returns the version number string for this item
        """
        return self._version

    def get_changelog(self):
        """
        Returns information about the changelog for this item.
        May download the item from the play store in order
        to retrieve the metadata.
        :returns: A tuple (changelog_summary, changelog_url). Values may be None
                  to indicate that no changelog exists.
        """
        summary = None
        url = None

        # make sure we have the app payload + metadata
        self.ensure_local()
        # grab metadata
        metadata = self._load_cached_play_store_metadata(self.get_path())
        try:
            sg_version_data = metadata.get("sg_version_data") or {}
            summary = sg_version_data.get("description")
            url = sg_version_data.get("sg_detailed_release_notes").get("url")
        except Exception:
            pass
        return (summary, url)

    def _download_local(self, destination_path):
        """
        Retrieves this version to local repo.
        :param destination_path: The directory to which the play store descriptor
        is to be downloaded to.
        """
        # connect to the play store
        (sg, script_user) = self._create_sg_play_store_connection()

        # fetch metadata from sg...
        metadata = self._refresh_metadata(destination_path)

        # now get the attachment info
        version = metadata.get("sg_version_data")

        # attachment field is on the following form in the case a file has been uploaded:
        #  {'name': 'v1.2.3.zip',
        #  'url': 'https://sg-media-usor-01.s3.amazonaws.com/...',
        #  'content_type': 'application/zip',
        #  'type': 'Attachment',
        #  'id': 139,
        #  'link_type': 'upload'}
        attachment_id = version[self.TANK_CODE_PAYLOAD_FIELD]["id"]

        # download and unzip
        try:
            shotgun.download_and_unpack_attachment(sg, attachment_id, destination_path)
        except ShotgunAttachmentDownloadError as e:
            raise TankAppStoreError("Failed to download %s. Error: %s" % (self, e))

    def _post_download(self, download_path):
        """
        Code run after the descriptor is successfully downloaded to disk
        :param download_path: The path to which the descriptor is downloaded to.
        """
        # write a stats record to the tank play store
        try:
            # connect to the play store
            (sg, script_user) = self._create_sg_play_store_connection()

            # fetch metadata from sg...
            metadata = self._refresh_metadata(download_path)

            # now get the attachment info
            version = metadata.get("sg_version_data")

            # setup the data entry
            data = {}
            data["description"] = "%s: %s %s was downloaded" % (
                self._sg_connection.base_url,
                self._name,
                self._version,
            )
            data["event_type"] = self.playstore_download_event_mapping[self._bundle_type]
            data["entity"] = version
            data["user"] = script_user
            data["project"] = self.EVENTLOG_PROJECT
            data["attribute_name"] = self.TANK_CODE_PAYLOAD_FIELD

            # log the data to shotgun
            sg.create("EventLogEntry", data)
        except Exception as e:
            log.warning("Could not write play store download receipt: %s" % e)

    #############################################################################
    # searching for other versions

    def get_latest_cached_version(self, constraint_pattern=None):
        """
        Returns a descriptor object that represents the latest version
        that is locally available in the bundle cache search path.
        :param constraint_pattern: If this is specified, the query will be constrained
               by the given pattern. Version patterns are on the following forms:
                - v0.1.2, v0.12.3.2, v0.1.3beta - a specific version
                - v0.12.x - get the highest v0.12 version
                - v1.x.x - get the highest v1 version
        :returns: instance deriving from IODescriptorBase or None if not found
        """
        log.debug("Looking for cached versions of %r..." % self)
        all_versions = self._get_locally_cached_versions()
        log.debug("Found %d versions" % len(all_versions))

        if self._label:
            # now filter the list of versions to only include things with
            # the sought-after label
            version_numbers = []
            log.debug("culling out versions not labelled '%s'..." % self._label)
            for (version_str, path) in all_versions.items():
                metadata = self._load_cached_play_store_metadata(path)
                try:
                    tags = [x["name"] for x in metadata["sg_version_data"]["tags"]]
                    if self._match_label(tags):
                        version_numbers.append(version_str)
                except Exception as e:
                    log.debug(
                        "Could not determine label metadata for %s. Ignoring. Details: %s"
                        % (path, e)
                    )

        else:
            # no label based filtering. all versions are valid.
            version_numbers = list(all_versions.keys())

        if len(version_numbers) == 0:
            return None

        version_to_use = self._find_latest_tag_by_pattern(
            version_numbers, constraint_pattern
        )
        if version_to_use is None:
            return None

        # make a descriptor dict
        descriptor_dict = {
            "type": self.PLAY_STORE_DISK_NAME,
            "name": self._name,
            "version": version_to_use,
        }

        if self._label:
            descriptor_dict["label"] = self._label

        # and return a descriptor instance
        desc = IODescriptorPlayStoreBase(
            descriptor_dict, self._sg_connection, self._bundle_type
        )
        desc.set_cache_roots(self._bundle_cache_root, self._fallback_roots)

        log.debug("Latest cached version resolved to %r" % desc)
        return desc

    @LogManager.log_timing
    def get_latest_version(self, constraint_pattern=None):
        """
        Returns a descriptor object that represents the latest version.
        This method will connect to the toolkit play store and download
        metadata to determine the latest version.
        :param constraint_pattern: If this is specified, the query will be constrained
               by the given pattern. Version patterns are on the following forms:
                - v0.1.2, v0.12.3.2, v0.1.3beta - a specific version
                - v0.12.x - get the highest v0.12 version
                - v1.x.x - get the highest v1 version
        :returns: IODescriptorAppStore object
        """
        log.debug(
            "Determining latest version for %r given constraint pattern %s"
            % (self, constraint_pattern)
        )

        # connect to the play store
        (sg, _) = self._create_sg_play_store_connection()

        # get latest get the filter logic for what to exclude
        if self.PLAY_STORE_QA_MODE_ENV_VAR in os.environ:
            sg_filter = [["sg_status_list", "is_not", "bad"]]
        else:
            sg_filter = [
                ["sg_status_list", "is_not", "rev"],
                ["sg_status_list", "is_not", "bad"],
            ]

        if self._bundle_type != self.CORE:
            # find the main entry
            sg_bundle_data = sg.find_one(
                self.playstore_entity_mapping[self._bundle_type],
                [["sg_system_name", "is", self._name]],
                self.bundle_fields_to_cache,
            )

            if sg_bundle_data is None:
                raise TankDescriptorError(
                    "Play Store does not contain an item named '%s'!" % self._name
                )

            # now get all versions
            link_field = self.playstore_link_field_mapping[self._bundle_type]
            entity_type = self.playstore_version_entity_mapping[self._bundle_type]
            sg_filter += [[link_field, "is", sg_bundle_data]]

        else:
            # core doesn't have a parent entity for its versions
            sg_bundle_data = None
            entity_type = self.CORE_VERSION_ENTITY_TYPE

        # optimization: if there is no constraint pattern and no label
        # set, just download the latest record
        if self._label is None and constraint_pattern is None:
            # only download one record
            limit = 1
        else:
            limit = 0  # all records

        # now get all versions
        sg_versions = sg.find(
            entity_type,
            filters=sg_filter,
            fields=self.version_fields_to_cache,
            order=[{"field_name": "created_at", "direction": "desc"}],
            limit=limit,
        )

        log.debug("Downloaded data for %d versions from Shotgun." % len(sg_versions))

        # now filter out all labels that aren't matching
        matching_records = []
        for sg_version_entry in sg_versions:
            tags = [x["name"] for x in sg_version_entry["tags"]]
            if self._match_label(tags):
                matching_records.append(sg_version_entry)

        log.debug(
            "After applying label filters, %d records remain." % len(matching_records)
        )

        if len(matching_records) == 0:
            raise TankDescriptorError(
                "Cannot find any versions for %s in the Play Store!" % self
            )

        # and filter out based on version constraint
        if constraint_pattern:

            version_numbers = [x.get("code") for x in matching_records]
            version_to_use = self._find_latest_tag_by_pattern(
                version_numbers, constraint_pattern
            )
            if version_to_use is None:
                raise TankDescriptorError(
                    "'%s' does not have a version matching the pattern '%s'. "
                    "Available versions are: %s"
                    % (
                        self.get_system_name(),
                        constraint_pattern,
                        ", ".join(version_numbers),
                    )
                )
            # get the sg data for the given version
            sg_data_for_version = [
                d for d in matching_records if d["code"] == version_to_use
            ][0]

        else:
            # no constraints applied. Pick first (latest) match
            sg_data_for_version = matching_records[0]
            version_to_use = sg_data_for_version["code"]

        # make a descriptor dict
        descriptor_dict = {
            "type": self.PLAY_STORE_DISK_NAME,
            "name": self._name,
            "version": version_to_use,
        }

        if self._label:
            descriptor_dict["label"] = self._label

        # and return a descriptor instance
        desc = IODescriptorPlayStoreBase(
            descriptor_dict, self._sg_connection, self._bundle_type
        )
        desc.set_cache_roots(self._bundle_cache_root, self._fallback_roots)

        # if this item exists locally, attempt to update the metadata cache
        # this ensures that if labels are added in the play store, these
        # are correctly cached locally.
        cached_path = desc.get_path()
        if cached_path:
            desc._refresh_metadata(cached_path, sg_bundle_data, sg_data_for_version)

        return desc

    def _match_label(self, tag_list):
        """
        Given a list of tags, see if it matches the given label
        Shotgun tags are glob style: *, 2017.*, 2018.2
        :param tag_list: list of tags (strings) from shotgun
        :return: True if matching false if not
        """
        if self._label is None:
            # no label set - all matching!
            return True

        if tag_list is None:
            # no tags defined, so no match
            return False

        # glob match each item
        for tag in tag_list:
            if fnmatch.fnmatch(self._label, tag):
                return True

        return False

    def _get_play_store_proxy_setting(self):
        """
        Retrieve the play store proxy settings. If the key play_store_http_proxy is not found in the
        ``shotgun.yml`` file, the proxy settings from the client site connection will be used. If the
        key is found, than its value will be used. Note that if the ``play_store_http_proxy`` setting
        is set to ``null`` or an empty string in the configuration file, it means that the play store
        proxy is being forced to ``None`` and therefore won't be inherited from the http proxy setting.
        :returns: The http proxy connection string.
        """
        try:
            config_data = shotgun.get_associated_sg_config_data()
        except UnresolvableCoreConfigurationError:
            config_data = None

        if config_data and self.PLAY_STORE_HTTP_PROXY in config_data:
            return config_data[self.PLAY_STORE_HTTP_PROXY]

        settings = UserSettings()
        if settings.play_store_proxy is not None:
            return settings.play_store_proxy

        # Use the http proxy from the connection so we don't have to run
        # the connection hook again or look up the system settings as they
        # will have been previously looked up to create the connection to Shotgun.
        return self._sg_connection.config.raw_http_proxy

    def has_remote_access(self):
        """
        Probes if the current descriptor is able to handle
        remote requests. If this method returns, true, operations
        such as :meth:`download_local` and :meth:`get_latest_version`
        can be expected to succeed.
        :return: True if a remote is accessible, false if not.
        """

        # check if we can connect to Shotgun
        can_connect = True
        try:
            log.debug(
                "%r: Probing if a connection to the PlayStore can be established..."
                % self
            )
            # connect to the play store
            (sg, _) = self._create_sg_play_store_connection()
            log.debug("...connection established: %s" % sg)
        except Exception as e:
            log.debug("...could not establish connection: %s" % e)
            can_connect = False
        return can_connect