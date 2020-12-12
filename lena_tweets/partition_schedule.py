"""
Adapted from Dagster's hourly schedule to have a minute partition.
"""
import datetime
import warnings

import pendulum
from croniter import croniter
from dagster import check
from dagster.core.definitions.partition import (
    PartitionSetDefinition,
    create_default_partition_selector_fn,
)
from dagster.core.errors import DagsterInvalidDefinitionError
from dagster.utils.partitions import (
    DEFAULT_DATE_FORMAT,
    DEFAULT_HOURLY_FORMAT_WITHOUT_TIMEZONE,
    DEFAULT_HOURLY_FORMAT_WITH_TIMEZONE,
    DEFAULT_MONTHLY_FORMAT,
)


def minute_schedule(
    pipeline_name,
    start_date,
    name=None,
    tags_fn_for_date=None,
    solid_selection=None,
    should_execute=None,
    environment_vars=None,
    end_date=None,
    execution_timezone=None,
):
    """Create a schedule that runs every minute.
    The decorated function will be called as the ``run_config_fn`` of the underlying
    `ScheduleDefinition` and should take a `ScheduleExecutionContext` as its only argument, 
    returning the environment dict for the scheduled execution.
    Args:
        pipeline_name (str): The name of the pipeline to execute when the schedule runs.
        start_date (datetime.datetime): The date from which to run the schedule.
        name (Optional[str]): The name of the schedule to create. By default, this will be the name
            of the decorated function.
        tags_fn_for_date (Optional[Callable[[datetime.datetime], Optional[Dict[str, str]]]]): A
            function that generates tags to attach to the schedules runs. Takes the date of the
            schedule run and returns a dictionary of tags (string key-value pairs).
        solid_selection (Optional[List[str]]): A list of solid subselection (including single
            solid names) to execute when the schedule runs. e.g. ``['*some_solid+', 'other_solid']``
        mode (Optional[str]): The pipeline mode in which to execute this schedule.
            (Default: 'default')
        should_execute (Optional[Callable[ScheduleExecutionContext, bool]]): A function that runs at
            schedule execution tie to determine whether a schedule should execute or skip. Takes a
            :py:class:`~dagster.ScheduleExecutionContext` and returns a boolean (``True`` if the
            schedule should execute). Defaults to a function that always returns ``True``.
        environment_vars (Optional[Dict[str, str]]): Any environment variables to set when executing
            the schedule.
        end_date (Optional[datetime.datetime]): The last time to run the schedule to, defaults to
            current time.
        execution_timezone (Optional[str]): Timezone in which the schedule should run. Only works
            with DagsterDaemonScheduler, and must be set when using that scheduler.
    """
    check.opt_str_param(name, "name")
    check.inst_param(start_date, "start_date", datetime.datetime)
    check.opt_inst_param(end_date, "end_date", datetime.datetime)
    check.opt_callable_param(tags_fn_for_date, "tags_fn_for_date")
    check.opt_nullable_list_param(solid_selection, "solid_selection", of_type=str)
    mode = "default"
    check.opt_callable_param(should_execute, "should_execute")
    check.opt_dict_param(environment_vars, "environment_vars", key_type=str, value_type=str)
    check.str_param(pipeline_name, "pipeline_name")
    check.opt_str_param(execution_timezone, "execution_timezone")

    if start_date.second != 0:
        warnings.warn(
            "`start_date` must be at the beginning of the minute for a per minute schedule. "
        )

    cron_schedule = "* * * * *"

    fmt = (
        DEFAULT_HOURLY_FORMAT_WITH_TIMEZONE
        if execution_timezone
        else DEFAULT_HOURLY_FORMAT_WITHOUT_TIMEZONE
    )

    execution_time_to_partition_fn = lambda d: pendulum.instance(d).subtract(
        minutes=1
    )

    partition_fn = schedule_partition_range(
        start_date,
        end=end_date,
        cron_schedule=cron_schedule,
        fmt=fmt,
        timezone=execution_timezone,
        execution_time_to_partition_fn=execution_time_to_partition_fn,
    )

    def inner(fn):
        check.callable_param(fn, "fn")

        schedule_name = name or fn.__name__

        tags_fn_for_partition_value = lambda partition: {}
        if tags_fn_for_date:
            tags_fn_for_partition_value = lambda partition: tags_fn_for_date(partition.value)

        partition_set = PartitionSetDefinition(
            name="{}_partitions".format(schedule_name),
            pipeline_name=pipeline_name,
            partition_fn=partition_fn,
            run_config_fn_for_partition=lambda partition: fn(partition.value),
            solid_selection=solid_selection,
            tags_fn_for_partition=tags_fn_for_partition_value,
            mode=mode,
        )

        return partition_set.create_schedule_definition(
            schedule_name,
            cron_schedule,
            should_execute=should_execute,
            environment_vars=environment_vars,
            partition_selector=create_default_partition_selector_fn(
                delta_fn=execution_time_to_partition_fn, fmt=fmt,
            ),
            execution_timezone=execution_timezone,
        )

    return inner




def schedule_partition_range(
    start, end, cron_schedule, fmt, timezone, execution_time_to_partition_fn,
):
    from dagster.core.definitions.partition import Partition

    check.inst_param(start, "start", datetime.datetime)
    check.opt_inst_param(end, "end", datetime.datetime)
    check.str_param(cron_schedule, "cron_schedule")
    check.str_param(fmt, "fmt")
    check.opt_str_param(timezone, "timezone")
    check.callable_param(execution_time_to_partition_fn, "execution_time_to_partition_fn")

    if end and start > end:
        raise DagsterInvariantViolationError(
            'Selected date range start "{start}" is after date range end "{end}'.format(
                start=start.strftime(fmt), end=end.strftime(fmt),
            )
        )

    def get_schedule_range_partitions():
        tz = timezone if timezone else pendulum.now().timezone.name
        _start = (
            start.in_tz(tz)
            if isinstance(start, pendulum.Pendulum)
            else pendulum.instance(start, tz=tz)
        )

        if not end:
            _end = pendulum.now(tz)
        elif isinstance(end, pendulum.Pendulum):
            _end = end.in_tz(tz)
        else:
            _end = pendulum.instance(end, tz=tz)

        end_timestamp = _end.timestamp()

        partitions = []
        for next_time in schedule_execution_time_iterator(_start.timestamp(), cron_schedule, tz):

            partition_time = execution_time_to_partition_fn(next_time)

            if partition_time.timestamp() > end_timestamp:
                break

            if partition_time.timestamp() < _start.timestamp():
                continue

            partitions.append(Partition(value=partition_time, name=partition_time.strftime(fmt)))

        return partitions[:-1]

    return get_schedule_range_partitions


def schedule_execution_time_iterator(start_timestamp, cron_schedule, execution_timezone):
    check.float_param(start_timestamp, "start_timestamp")
    check.str_param(cron_schedule, "cron_schedule")
    check.opt_str_param(execution_timezone, "execution_timezone")
    timezone_str = execution_timezone if execution_timezone else pendulum.now().timezone.name

    start_datetime = pendulum.from_timestamp(start_timestamp, tz=timezone_str)

    date_iter = croniter(cron_schedule, start_datetime)

    # Go back one iteration so that the next iteration is the first time that is >= start_datetime
    # and matches the cron schedule
    date_iter.get_prev(datetime.datetime)
    next_date = pendulum.instance(date_iter.get_next(datetime.datetime)).in_tz(timezone_str)

    yield next_date

    cron_parts = cron_schedule.split(" ")

    check.invariant(len(cron_parts) == 5)

    is_numeric = [part.isnumeric() for part in cron_parts]

    delta_fn = None

    # Special-case common intervals (hourly/daily/weekly/monthly) since croniter iteration can be
    # much slower than adding a fixed interval
    if cron_schedule.endswith(" * *") and all(is_numeric[0:3]):  # monthly
        delta_fn = lambda d, num: d.add(months=num)
        should_hour_change = False
    elif (
        all(is_numeric[0:2]) and is_numeric[4] and cron_parts[2] == "*" and cron_parts[3] == "*"
    ):  # weekly
        delta_fn = lambda d, num: d.add(weeks=num)
        should_hour_change = False
    elif all(is_numeric[0:2]) and cron_schedule.endswith(" * * *"):  # daily
        delta_fn = lambda d, num: d.add(days=num)
        should_hour_change = False
    elif is_numeric[0] and cron_schedule.endswith(" * * * *"):  # hourly
        delta_fn = lambda d, num: d.add(hours=num)
        should_hour_change = True

    while True:
        if delta_fn:
            curr_hour = next_date.hour

            next_date_cand = delta_fn(next_date, 1)
            new_hour = next_date_cand.hour

            if not should_hour_change and new_hour != curr_hour:
                # If the hour changes during a daily/weekly/monthly schedule, it
                # indicates that the time shifted due to falling in a time that doesn't
                # exist due to a DST transition (for example, 2:30AM CST on 3/10/2019).
                # Instead, execute at the first time that does exist (the start of the hour),
                # but return to the original hour for all subsequent executions so that the
                # hour doesn't stay different permanently.

                check.invariant(new_hour == curr_hour + 1)
                yield next_date_cand.replace(minute=0)

                next_date_cand = delta_fn(next_date, 2)
                check.invariant(next_date_cand.hour == curr_hour)

            next_date = next_date_cand
        else:
            next_date = pendulum.instance(date_iter.get_next(datetime.datetime)).in_tz(timezone_str)

        yield next_date