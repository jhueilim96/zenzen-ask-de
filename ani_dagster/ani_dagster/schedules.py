from dagster_dbt import build_schedule_from_dbt_selection


dbt_schedules = [
    # build_schedule_from_dbt_selection(
    #     [dbt_assset],
    #     job_name="materialize_dbt_models",
    #     cron_schedule="0 0 * * *",
    #     dbt_select="fqn:*",
    # ),
]
