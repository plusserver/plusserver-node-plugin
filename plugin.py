"""Tellus node plugin reference implementation to create virtual machines. SCS-compliant and based on OpenStack"""

import os

import openstack

from common.types import (
    CreateResponseBody,
    DeleteResponseBody,
    IpAddress,
    Offering,
    StatusResponseBody,
)
from node.application.plugins.abstract import ConfigurationPlugin


class InternalException(Exception):
    def __init__(self, message: str, code: int):
        super().__init__(message)
        self.code = code


class PlusServerPlugin(ConfigurationPlugin):
    """
    Plusserver Tellus node plugin implementation based on OpenStack.
    """

    def __init__(
        self,
        log_prefix: str,
        image_name: str,
        auth_url: str,
        region: str,
        project_name: str,
        username: str,
        password: str,
        user_domain: str,
        project_domain: str,
    ):
        """Initialize the PlusServerPlugin with the necessary configuration parameters.

        :param log_prefix: Prefix string for logging function
        :param image_name: Name of the OpenStack image. E.g. 'Ubuntu 24.04'
        :param auth_url: OS_AUTH_URL from OpenStack RC file
        :param region: OS_REGION_NAME from OpenStack RC file
        :param project_name: OS_PROJECT_NAME from OpenStack RC file
        :param username: OS_USERNAME from OpenStack RC file
        :param password: OpenStack Password for project $project_name as user $username
        :param user_domain: OS_USER_DOMAIN_NAME from OpenStack RC file
        :param project_domain: OS_PROJECT_DOMAIN_ID from OpenStack RC file
        """
        self.configurations: dict = {}
        self.server_name_prefix: str = "tellus-vm-"
        self.log_prefix = log_prefix
        self.image_name = image_name
        self.auth_url = auth_url
        self.region = region
        self.project_name = project_name
        self.username = username
        self.password = password
        self.user_domain = user_domain
        self.project_domain = project_domain

    def _log(self, message: str):
        """Print a log message with the specified prefix.

        :param message: The message to log
        """
        print(self.log_prefix, message)

    def _create_keypair(
        self, conn: openstack.connection.Connection, keypair_name: str, pub_key: str
    ) -> openstack.compute.v2.keypair.Keypair:
        """Create or find a keypair with keypair_name.

        :param conn: OpenStack connection
        :param keypair_name: Name of the keypair
        :param pub_key: User's public key to access the VM
        :return: The created OpenStack compute Keypair object
        """
        try:
            keypair = conn.compute.find_keypair(keypair_name, ignore_missing=True)
        except openstack.exceptions.DuplicateResource as e:
            self._log(str(e))
            raise InternalException(
                f"Multiple keypairs with the name {keypair_name} already exist (duplicate resource)",
                400,
            ) from e

        if not keypair:
            try:
                keypair = conn.compute.create_keypair(
                    name=keypair_name, public_key=pub_key
                )
            except Exception as e:
                self._log(str(e))
                raise InternalException(
                    "The provided public key data is invalid", 400
                ) from e

        return keypair

    def _create_server(
        self,
        conn: openstack.connection.Connection,
        pub_key: str,
        image_name: str,
        memory: int,
        cores: int,
        disk_space: int,
        key: str,
    ) -> openstack.compute.v2.server.Server:
        """Create a server in OpenStack.

        :param conn: OpenStack connection
        :param pub_key: User's SSH public key.
        :param image_name: Name of the image to use for the server.
        :param memory: Amount of RAM in GB for the server
        :param cores: Numer of VCPUs for the server
        :param disk_space: Amount of disk space in GB for the server
        :param key: Unique identifier used for the VM and OpenStack keypair
        :return: The created OpenStack server instance
        """
        # check if server already exists
        for server in conn.compute.servers():
            if server.name.find(key) != -1:
                raise InternalException(f"VM with key '{key}' already exists", 400)

        # choose image
        image = conn.image.find_image(image_name, ignore_missing=True)
        if image.get("min_ram") / 1000 > memory:
            raise InternalException("Not enough memory to run the selected image", 400)
        if image.get("min_disk") > disk_space:
            raise InternalException(
                "Not enough disk space to run the selected image", 400
            )

        # select a flavor from available SCS-compatbible flavors
        flavor_name = f"SCS-{cores}V-{memory}-{disk_space}"  # e.g. SCS-1V-2-10
        self._log(f"Creating flavor: {flavor_name}")
        flavor = conn.compute.find_flavor(flavor_name, ignore_missing=True)
        if not flavor:
            raise InternalException(f"Flavor {flavor_name} does not exist", 404)

        # set network
        network_name = f"{os.environ['OS_PROJECT_NAME']}-network"
        try:
            network = conn.network.find_network(network_name, ignore_missing=False)
        except Exception as e:
            raise InternalException(
                f"Could not find network {network_name}", 404
            ) from e

        # reuse or create keypair
        keypair_name = self.server_name_prefix + key
        keypair = self._create_keypair(
            conn,
            keypair_name=keypair_name,
            pub_key=pub_key,
        )

        # start server
        try:
            server = conn.compute.create_server(
                name=self.server_name_prefix + key,
                image_id=image.id,
                flavor_id=flavor.id,
                networks=[{"uuid": network.id}],
                key_name=keypair.name,
                description="created by tellus node",
            )
            server = conn.compute.wait_for_server(server, status="ACTIVE")
        except Exception as e:
            self._log(str(e))
            raise InternalException("Unable to create server", 400)

        return server

    def _delete_server(self, conn: openstack.connection.Connection, server_name: str):
        """Delete a created OpenStack server.

        :param conn: OpenStack connection
        :param server_name: Name (or ID) of the server
        """
        try:
            server = conn.compute.find_server(server_name, ignore_missing=True)
            if server:
                conn.compute.delete_server(server)
        except Exception as e:
            self._log(str(e))
            raise InternalException(
                f"Unable to delete server {server_name}", 404
            ) from e

    def _delete_keypair(
        self,
        conn: openstack.connection.Connection,
        keypair_name: str,
    ):
        """Delete an OpenStack keypair.

        :param conn: OpenStack connection
        :param keypair_name: Name of the keypair
        """
        try:
            keypair = conn.compute.find_keypair(keypair_name, ignore_missing=True)
            if keypair:
                conn.compute.delete_keypair(keypair)
        except Exception as e:
            self._log(str(e))
            raise InternalException(
                f"Unable to delete keypair {keypair_name}", 404
            ) from e

    def status(self, key: str) -> tuple[StatusResponseBody, int]:
        self._log(f"STATUS RESOURCE ({key})")
        configuration = self.configurations.get(key.upper())

        if not configuration:
            return StatusResponseBody(error=f"Unknown configuration '{key}'"), 404
        else:
            with openstack.connect(
                app_name="tellus-node-plugin",
                auth_url=self.auth_url,
                project_name=self.project_name,
                username=self.username,
                password=self.password,
                region_name=self.region,
                user_domain_name=self.user_domain,
                project_domain_name=self.project_domain,
            ) as conn:
                try:
                    conn.authorize()  # test connection
                except Exception:
                    return StatusResponseBody(
                        status="down",
                        error="There was a problem with authentication",
                    ), 401
                try:
                    server = conn.compute.get_server(configuration["id"])
                    self._log(server)
                    status = server["status"]
                    try:
                        # extract floating ip from server dict
                        ipv4 = server["addresses"][
                            f"{os.environ['OS_PROJECT_NAME']}-network"
                        ][1]["addr"]
                        ip_addresses = [IpAddress(type="ipv4", prefix="32", value=ipv4)]
                    except Exception as e:
                        self._log(str(e))
                        return StatusResponseBody(
                            status="down",
                            error="Could not get the VM's public IP address",
                        ), 404

                    if status == "ACTIVE":
                        return StatusResponseBody(
                            status="up", ip_addresses=ip_addresses
                        ), 200
                    elif status == "BUILDING":
                        return StatusResponseBody(
                            status="preparing", ip_addresses=ip_addresses
                        ), 200
                    else:
                        return StatusResponseBody(
                            status="down", error=f"VM is currently in state: {status}"
                        ), 200
                except Exception as e:
                    return StatusResponseBody(status="down", error=str(e)), 500

    def create(self, offering: Offering) -> tuple[CreateResponseBody, int]:
        self._log(f"CREATE RESOURCE ({offering.order_id.upper()})")

        if self.configurations.get(offering.order_id.upper()):
            self._log(
                f"Duplicate key from offering: configurations['{offering.order_id}'] already exists"
            )
            return CreateResponseBody(
                error=f"VM with key '{offering.order_id}' already exists"
            ), 400

        if not offering.virtual_machine_service_offering:
            return CreateResponseBody(error="This plugin can only provision VMs"), 400

        vmso = offering.virtual_machine_service_offering
        try:
            with openstack.connect(
                app_name="tellus-node-plugin",
                auth_url=self.auth_url,
                project_name=self.project_name,
                username=self.username,
                password=self.password,
                region_name=self.region,
                user_domain_name=self.user_domain,
                project_domain_name=self.project_domain,
            ) as conn:
                try:
                    conn.authorize()  # test connection
                except Exception:
                    return CreateResponseBody(
                        error="There was a problem with authentication",
                    ), 401

                server = self._create_server(
                    conn=conn,
                    pub_key=vmso.ssh_keys[0],
                    image_name=self.image_name,
                    memory=round(vmso.server_flavor.ram.to_unit("GByte")),
                    cores=round(vmso.server_flavor.cpu.cores),
                    disk_space=round(vmso.server_flavor.boot_volume.to_unit("GByte")),
                    key=offering.order_id.upper(),
                )
                ip = conn.add_auto_ip(server)
        except InternalException as e:
            return CreateResponseBody(
                error=e.message
            ), e.code  # return custom http status code
        except Exception as e:
            return CreateResponseBody(
                error=str(e)
            ), 500  # return default http status code

        self._log(f"SERVER: {str(server)}")
        self.configurations[offering.order_id.upper()] = {
            "type": "vm",
            "id": server.id,
        }

        return CreateResponseBody(), 201

    def update(self, key: str, offering: Offering) -> tuple[str, int]:
        raise NotImplementedError

    def destroy(self, key: str) -> tuple[DeleteResponseBody, int]:
        self._log(f"DESTROY RESOURCE ({key})")

        if not self.configurations.get(key.upper()):
            return DeleteResponseBody(error=f"Unknown configuration '{key}'"), 404

        try:
            with openstack.connect(
                app_name="tellus-node-plugin",
                auth_url=self.auth_url,
                project_name=self.project_name,
                username=self.username,
                password=self.password,
                region_name=self.region,
                user_domain_name=self.user_domain,
                project_domain_name=self.project_domain,
            ) as conn:
                try:
                    conn.authorize()  # test connection
                except Exception:
                    return DeleteResponseBody(
                        error="There was a problem with authentication",
                    ), 401

                self._delete_server(
                    conn=conn, server_name=self.server_name_prefix + key.upper()
                )
                self._delete_keypair(
                    conn=conn, keypair_name=self.server_name_prefix + key.upper()
                )
        except InternalException as e:
            return DeleteResponseBody(
                error=e.message
            ), e.code  # return custom http status code
        except Exception as e:
            return DeleteResponseBody(
                error=str(e)
            ), 500  # return default http status code
        self.configurations.pop(key.upper())

        return DeleteResponseBody(), 204


def create_plusserver_plugin() -> PlusServerPlugin:
    """Creates an SCS-compliant pluscloud open node plugin based on OpenStack.
    Configuration is done using using environment variables from a .env file.
    Parameters can also be changed using Docker environment variables.

    :return: PlusServerPlugin
    """
    return PlusServerPlugin(
        log_prefix=os.environ.get("NODE_LOG_PREFIX", "INFO:"),
        image_name=os.environ.get("IMAGE_NAME", "Ubuntu 24.04"),
        auth_url=os.environ["OS_AUTH_URL"],
        region=os.environ["OS_REGION_NAME"],
        project_name=os.environ["OS_PROJECT_NAME"],
        username=os.environ["OS_USERNAME"],
        password=os.environ["OS_PASSWORD"],
        user_domain=os.environ["OS_USER_DOMAIN_NAME"],
        project_domain=os.environ["OS_PROJECT_DOMAIN_ID"],
    )
