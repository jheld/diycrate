import configparser
import subprocess

from pathlib import Path
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

    certificates_cmd_result: subprocess.CompletedProcess = subprocess.run(
        ["certbot", "certificates"], check=True, capture_output=True
    )
    if domain not in bytes.decode(certificates_cmd_result.stdout):
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

    else:
        print("certbot already configured :)")
