import singer
from singer import Transformer, metadata, utils
from tap_sftp import client, stats
from tap_sftp.singer_write import write_record
from tap_sftp.aws_ssm import AWS_SSM
from tap_sftp.singer_encodings import csv_handler

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import backoff

LOGGER = singer.get_logger()


def sync_ftp(sftp_file, stream, table_spec, config, state, table_name):
    records_streamed = sync_file(sftp_file, stream, table_spec, config)
    # Return the state update instead of writing it directly
    state_update = {
        'table_name': table_name,
        'bookmark_key': 'modified_since',
        'bookmark_value': sftp_file['last_modified'].isoformat()
    }
    return records_streamed, state_update


def sync_stream(config, state, stream):
    table_visible_name = stream.stream
    table_original_name = stream.tap_stream_id
    
    modified_since = utils.strptime_to_utc(singer.get_bookmark(state, table_visible_name, 'modified_since') or
                                           config['start_date'])
    search_subdir = config.get("search_subdirectories", True)

    LOGGER.info('Syncing table "%s".', table_visible_name)
    LOGGER.info('Getting files modified since %s.', modified_since)

    sftp_client = client.connection(config)
    table_spec = [table_config for table_config in config["tables"] if table_config["table_name"] == table_original_name]
    if len(table_spec) == 0:
        LOGGER.info("No table configuration found for '%s', skipping stream", table_visible_name)
        return 0
    if len(table_spec) > 1:
        LOGGER.info("You can only have one file path per table name. Multiple table configurations found for '%s', skipping stream.", table_name)
        return 0
    table_spec = table_spec[0]

    files = sftp_client.get_files(
        table_spec["search_prefix"],
        table_spec["search_pattern"],
        modified_since,
        search_subdir
    )
    sftp_client.close()

    LOGGER.info('Found %s files to be synced.', len(files))

    records_streamed = 0
    if not files:
        return records_streamed

    # Collect state updates from all threads
    state_updates = []
    
    with ThreadPoolExecutor(max_workers=8) as executor:
        future_sftp = {executor.submit(sync_ftp, sftp_file, stream, table_spec, config, state, table_visible_name): sftp_file for sftp_file in files}
        for future in as_completed(future_sftp):
            result = future.result()
            records_streamed += result[0]
            state_updates.append(result[1])

    # Sort state updates by datetime to ensure latest file's state is written last
    state_updates.sort(key=lambda x: datetime.fromisoformat(x['bookmark_value']))
    
    # Apply all state updates at once
    for state_update in state_updates:
        state = singer.write_bookmark(state, state_update['table_name'], state_update['bookmark_key'], state_update['bookmark_value'])
    
    # Write state once after all updates are collected
    singer.write_state(state)

    LOGGER.info('Wrote %s records for table "%s".', records_streamed, table_visible_name)

    return records_streamed


@backoff.on_exception(
        backoff.expo,
        (IOError,OSError),
        max_tries=3,
        jitter=backoff.random_jitter,
        factor=2)
def sync_file(sftp_file_spec, stream, table_spec, config):
    LOGGER.info('Syncing file "%s".', sftp_file_spec["filepath"])
    sftp_client = client.connection(config)
    decryption_configs = config.get('decryption_configs')
    if decryption_configs:
        decryption_configs['key'] = AWS_SSM.get_decryption_key(decryption_configs.get('SSM_key_name'))
    # Attempt to re-read from the file incase of IO or OS error
    with sftp_client.get_file_handle(sftp_file_spec, decryption_configs) as file_handle:
        if decryption_configs:
            sftp_file_spec['filepath'] = file_handle.name

        # Add file_name to opts and flag infer_compression to support gzipped files
        opts = {'key_properties': table_spec.get('key_properties', []),
                'delimiter': table_spec.get('delimiter', ','),
                'file_name': sftp_file_spec['filepath'],
                'encoding': table_spec.get('encoding', config.get('encoding'))}

        readers = csv_handler.get_row_iterators(file_handle, options=opts, infer_compression=True)

        records_synced = 0

        for reader in readers:
            LOGGER.info('Synced Record Count: 0')
            with Transformer() as transformer:
                for row in reader:
                    custom_columns = {
                        '_sdc_source_file': sftp_file_spec["filepath"],

                        # index zero, +1 for header row
                        '_sdc_source_lineno': records_synced + 2
                    }
                    rec = {**row, **custom_columns}

                    to_write = transformer.transform(rec, stream.schema.to_dict(), metadata.to_map(stream.metadata))

                    write_record(stream.stream, to_write)
                    records_synced += 1
                    if records_synced % 100000 == 0:
                        LOGGER.info(f'Synced Record Count: {records_synced}')
            LOGGER.info(f'Sync Complete - Records Synced: {records_synced}')

    stats.add_file_data(table_spec, sftp_file_spec['filepath'], sftp_file_spec['last_modified'], records_synced)

    if config.get('delete_after_sync'):
        sftp_client.sftp.remove(sftp_file_spec["filepath"])
        LOGGER.info(f"Deleting remote file: {sftp_file_spec['filepath']}")
    sftp_client.close()
    del sftp_client

    return records_synced
