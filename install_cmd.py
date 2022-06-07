import configparser
import subprocess

from pathlib import Path
from typing import Union

from setuptools.command.install import install


class InstallWrapper(install):
    def run(self):
        super().run()
        certbot_runner()


def certbot_runner():
    conf_obj = configparser.ConfigParser()

    conf_dir = Path("~/.config/diycrate_server").expanduser().resolve()
    if not conf_dir.is_dir():
        conf_dir.mkdir()
    cloud_credentials_file_path = conf_dir / "box.ini"
    if not cloud_credentials_file_path.is_file():
        cloud_credentials_file_path.write_text("")
    conf_obj.read(cloud_credentials_file_path)

    domain = str(
        Path(
            conf_obj.get(
                "ssl",
                "cacert_pem_path",
                fallback="/etc/letsencrypt/live/diycrate.xyz/cert.pem",
            )
        ).parent.name
    )

    certificates_cmd_result: Union[subprocess.CompletedProcess, None]
    try:
        certificates_cmd_result = subprocess.run(
            ["certbot", "certificates"], check=True, capture_output=True
        )
    except subprocess.CalledProcessError as e:
        certificates_cmd_result = None
        print(
            f"certbot may not be installed, "
            f"or the command issued may not be configured correctly. "
            f":(. "
            f"return code: {e.returncode}"
        )
    if certificates_cmd_result and domain not in bytes.decode(
        certificates_cmd_result.stdout
    ):
        try:
            subprocess.run(["certbot", "--standalone"], check=True)
        except subprocess.CalledProcessError as e:
            print(
                f"certbot may not be installed, "
                f"or the command issued may not be configured correctly. "
                f":(. "
                f"return code: {e.returncode}"
            )
        else:
            print("certbot configuration complete :)")

    elif certificates_cmd_result:
        print("certbot already configured :)")
