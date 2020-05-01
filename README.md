# tk-playstore

tk-playstore is a set of tooling that allows us to release Shotgun Toolkit Apps to our own custom app store


## History

The Shotgun Toolkit official App Store is structured in the following way:

Each client site gets a ApiUser with the script_name/api_key accessible at https://yoursite.shotgunstudio.com/api3/sgtk_install_script when doing a POST request with `{"session_token": sg_isntance.get_session_token()}`.  

The remote site is https://tank.shotgunstudio.com.

### Shotgun App Store Schema  
There are several CustomNonProjectEntity types that have been setup to become an AppStore.  


| Entity Type | Entity Display Name | Purpose |
| ----------- | ------------------- | ------- |
| CustomNonProjectEntity01 | Tank Core Version Name | Versions of tk-core |
| CustomNonProjectEntity02 | Tank App Name (Internal Use Only) | Types of toolkit applications |
| CustomNonProjectEntity03 | Tank Engine Name - DEPRECATED | Types of toolkit engines |
| CustomNonProjectEntity04 | Tank Engine Version Name | Versions of a specific toolkit engine |
| CustomNonProjectEntity05 | Tank App Version Name | Versions of a specific toolkit application |
| CustomNonProjectEntity06 | App Category Name | Category of Application - Seems unused |
| CustomNonProjectEntity07 | Tank Config Name - DEPRECATED | Different Pipeline Configs |
| CustomNonProjectEntity08 | Tank Config Version Name | Versions of a specific pipeline config |
| CustomNonProjectEntity09 | Tank Framework Version Name | Versions of a specific toolkit framework  |
| CustomNonProjectEntity10 | Tank Document Name | Types of documents and documentation |
| CustomNonProjectEntity11 | Tank Document Version Name | Versions and links to documents or documentation |
| CustomNonProjectEntity12 | Shotgun Desktop Version Name | Shotgun Desktop binaries |
| CustomNonProjectEntity13 | Tank Framework Name - DEPRECATED | Types of toolkit frameworks |
| CustomNonProjectEntity14 | Vendor Name | Not sure... Just contains ShotgunSoftware and Baseblack |
| CustomNonProjectEntity15 | Client Name | Client List... sort of half up to date... |

## tk-playstore

The toolkit playstore is a framework of tooling to allow you to host your own "Shotgun App Store"  

This framework leverages the [register_descriptor hook](https://github.com/shotgunsoftware/tk-core/pull/768) that I wrote, allowing us to create a custom IODescriptor type.  

# Installation

Installation of tk-playstore is in a few parts.  


## Release tools

`tk-playstore-release` contains tooling that allow you to register new applications, engines and frameworks to your playstore.
It also includes tooling to setup your site initially.    


## Viewing Tools

`tk-playstore-view` contains tooling to visualize your custom playstore.  


## Descriptors

`tk-playstore` contains `IODescriptorPlayStoreBase` in `io_descriptors/playstore.py` which is your baseclass descriptor for setting up your own PlayStore descriptor   


To release applications to your own playstore automatically, see [Setting Up CI](#setting-up-ci)













### Setting Up CI