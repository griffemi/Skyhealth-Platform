import subprocess

from skyhealth.config import settings


def main():
    subprocess.run(["dbt", "build", "--target", settings.dbt_target], check=True)

if __name__ == "__main__":
    main()
