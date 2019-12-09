from datetime import datetime, timedelta
import logging
import sys

import boto3
import crawler
import table_alias_file

log = logging.getLogger('paytm.main')
glue = boto3.client('glue')


def run_crawler_update_alias_cleanup(
    env: str, aws_role: str, database_name: str, group_id: str, target_date: str
):
    exported_table_prefix = 'l{group_id}_'.format(group_id=group_id)
    formatted_date = target_date.replace('-', '_')
    crawler_name = '{env}_export_{exported_table_prefix}dt_{formatted_date}_to_{database_name}'.format(
        env=env,
        exported_table_prefix=exported_table_prefix,
        formatted_date=formatted_date,
        database_name=database_name
    )
    log.info('Starting a crawler job for {crawler_name}.'.format(crawler_name=crawler_name))

    template = crawler.load_template('group_exporter')
    exported_table_alias = '{exported_table_prefix}dt_{formatted_date}'.format(
        exported_table_prefix=exported_table_prefix, formatted_date=formatted_date
    )
    crawler_json = crawler.substibute(template,
        crawler_name=crawler_name,
        aws_role=aws_role,
        database_name=database_name,
        group_id=group_id,
        target_date=target_date,
        table_prefix=exported_table_prefix
    )
    log.info('Json for {crawler_name}:\n {crawler_json}'.format(
        crawler_name=crawler_name, crawler_json=crawler_json
    ))
    crawler.run_crawler(crawler_name, crawler_json)

    file_key = table_alias_file_path(env)
    log.info('Updating table alias file {0}.'.format(file_key))
    table_alias_file.update(file_key, exported_table_alias)

    log.info('Removing old tables.')
    _remove_old_tables(database_name, group_id, target_date, keep_days=3)

    log.info('Finished the crawler job for {crawler_name}.'.format(crawler_name=crawler_name))


def table_alias_file_path(env: str) -> str:
    return '{env}/mapfeatures/athena/tablenames/tables.csv'.format(env=env)


def validate_env(env: str):
    assert env in ['dev', 'stg', 'prod']


def _parse_sys_argv(_, env, aws_role, database_name, group_id, current_date):
    validate_env(env)
    target_date = (datetime.strptime(current_date, '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')
    if env.lower() != 'prod':
        database_name = '{env}_{database_name}'.format(env=env, database_name=database_name)

    return [env, aws_role, database_name, group_id, target_date]


def _remove_old_tables(database_name: str, group_id: str, target_date_str: str, keep_days: int) -> None:
    def table_name(day):
        return 'l{group_id}_dt_{date_str}'.format(
            group_id=group_id,
            date_str=day.strftime('%Y_%m_%d')
        )

    def delete_tables(table_names):
        log.info('Deleting tables: {0}.'.format(table_names))
        glue.batch_delete_table(DatabaseName=database_name, TablesToDelete=table_names)

    target_date = datetime.strptime(target_date_str, '%Y-%m-%d')
    deletion_lookback_days = 10
    window_start = target_date - timedelta(days=keep_days)
    days_to_del = [window_start - timedelta(days=i) for i in range(0, deletion_lookback_days)]
    tables_to_del = [table_name(day) for day in days_to_del]
    delete_tables(tables_to_del)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    args = _parse_sys_argv(*sys.argv)
    run_crawler_update_alias_cleanup(*args)
