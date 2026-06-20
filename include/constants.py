from pathlib import Path

from cosmos import ExecutionConfig, ProfileConfig

housing_path  = Path("/usr/local/airflow/dbt/housing")
dbt_executable = Path("/usr/local/airflow/dbt_venv/bin/dbt")

venv_execution_config = ExecutionConfig(
    dbt_executable_path=str(dbt_executable),
)

housing_profile_config = ProfileConfig(
    profile_name="housing",
    target_name="dev",
    profiles_yml_filepath=housing_path / "profiles.yml",
)
