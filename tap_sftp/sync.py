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
    records_streamed, max_row_timestamp, replication_key_column_used = sync_file(sftp_file, stream, table_spec, config, state, table_name)
    # Return the state update instead of writing it directly
    # Auto-enable replication key if replication_key_column is provided
    replication_key_column = table_spec.get('replication_key_column', '').strip() if table_spec.get('replication_key_column') else None
    enable_row_level = table_spec.get('enable_replication_key', False) or bool(replication_key_column)
    
    # If row-level is enabled but no valid timestamp from rows, keep previous value from state
    if enable_row_level and not max_row_timestamp:
        # Try to get previous value from state
        previous_timestamp_str = singer.get_bookmark(state, table_name, 'replication_key_value')
        if previous_timestamp_str:
            # Keep previous value - don't lose progress
            max_row_timestamp = utils.strptime_to_utc(previous_timestamp_str)
            if records_streamed > 0:
                LOGGER.warning(
                    f'Replication key: No valid timestamps found in processed rows. '
                    f'Keeping previous replication_key_value from state: {max_row_timestamp.isoformat()}'
                )
            else:
                LOGGER.info(
                    f'Replication key: No rows processed. '
                    f'Keeping previous replication_key_value from state: {max_row_timestamp.isoformat()}'
                )
        else:
            # No previous value - leave as None (will process all rows)
            if records_streamed > 0:
                LOGGER.warning(
                    f'Replication key: No valid timestamps found and no previous value in state. '
                    f'Will process all rows (replication_key_value = None)'
                )
    
    state_update = {
        'table_name': table_name,
        'bookmark_key': 'modified_since',
        'modified_since': sftp_file['last_modified'].isoformat(),
        'replication_key_column': replication_key_column_used,  # Store the column name used for replication key
        'replication_key_value': max_row_timestamp.isoformat() if max_row_timestamp else None
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
        LOGGER.info("You can only have one file path per table name. Multiple table configurations found for '%s', skipping stream.", table_visible_name)
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
    state_updates.sort(key=lambda x: datetime.fromisoformat(x['modified_since']))
    
    # Apply all state updates at once
    for state_update in state_updates:
        # Always update modified_since (used for file-level filtering)
        state = singer.write_bookmark(
            state, 
            state_update['table_name'], 
            'modified_since', 
            state_update['modified_since']
        )
        # Store replication key column name if used
        if state_update.get('replication_key_column') is not None:
            state = singer.write_bookmark(
                state,
                state_update['table_name'],
                'replication_key_column',
                state_update['replication_key_column']
            )
        # Store max timestamp of processed rows for replication key filtering
        if state_update.get('replication_key_value'):
            state = singer.write_bookmark(
                state,
                state_update['table_name'],
                'replication_key_value',
                state_update['replication_key_value']
            )
    
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
def sync_file(sftp_file_spec, stream, table_spec, config, state, table_name):
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
        
        # Check if replication key filtering is enabled
        # Auto-enable replication key if replication_key_column is provided
        replication_key_column = table_spec.get('replication_key_column', '').strip() if table_spec.get('replication_key_column') else None
        enable_row_level = table_spec.get('enable_replication_key', False) or bool(replication_key_column)
        is_csv = table_spec.get('delimiter') is not None
        
        # Prepare for replication key filtering if needed
        row_level_bookmark = None
        selected_column = None
        rows_filtered = 0
        max_row_timestamp = None  # Track max timestamp of processed rows
        invalid_timestamps_count = 0  # Track invalid timestamps when replication key is active
        
        if enable_row_level and is_csv:
            # Use the replication key column (default to 'updated_at' if not specified but enabled)
            if not replication_key_column:
                replication_key_column = 'updated_at'
            
            # Try to use replication_key_value first, fallback to modified_since
            replication_key_value_str = singer.get_bookmark(state, table_name, 'replication_key_value')
            if replication_key_value_str:
                row_level_bookmark = utils.strptime_to_utc(replication_key_value_str)
                LOGGER.info(f'Using replication_key_value from state: {replication_key_value_str}')
            else:
                # Fallback to modified_since for backward compatibility
                row_level_bookmark_str = singer.get_bookmark(state, table_name, 'modified_since')
                if row_level_bookmark_str:
                    row_level_bookmark = utils.strptime_to_utc(row_level_bookmark_str)
                    LOGGER.info(f'Using modified_since for replication key filtering (replication_key_value not found): {row_level_bookmark_str}')
            
            LOGGER.info(
                f'Replication key enabled. Looking for column: {replication_key_column}. '
                f'Using bookmark datetime: {row_level_bookmark or "None (will use all rows)"}'
            )

        for reader in readers:
            # If replication key is active, check if column exists
            if enable_row_level and replication_key_column and not selected_column:
                if replication_key_column in reader.fieldnames:
                    selected_column = replication_key_column
                    LOGGER.info(f'Using column "{selected_column}" for replication key filtering')
                else:
                    LOGGER.warning(
                        f'Replication key column "{replication_key_column}" not found in CSV. '
                        f'Available columns: {list(reader.fieldnames)[:10]}. '
                        f'Falling back to file-level incremental sync.'
                    )
                    enable_row_level = False
                if not row_level_bookmark:
                    LOGGER.warning(
                        f'Replication key enabled but no bookmark found in state. '
                        f'Will process all rows (first sync).'
                    )
            
            LOGGER.info('Synced Record Count: 0')
            with Transformer() as transformer:
                for row in reader:
                    # Apply row-level filter if active
                    if enable_row_level and selected_column:
                        updated_at_str = row.get(selected_column)
                        if updated_at_str:
                            try:
                                updated_at = utils.strptime_to_utc(updated_at_str)
                                # If we have a bookmark, filter rows
                                if row_level_bookmark and updated_at <= row_level_bookmark:
                                    rows_filtered += 1
                                    continue  # Skip this row
                                # Track max timestamp of processed rows (for all valid timestamps)
                                if max_row_timestamp is None or updated_at > max_row_timestamp:
                                    max_row_timestamp = updated_at
                            except (ValueError, TypeError) as e:
                                invalid_timestamps_count += 1
                                LOGGER.warning(
                                    f"Replication key: Invalid timestamp format in column '{selected_column}': '{updated_at_str}'. "
                                    f"Row will be emitted anyway (cannot filter without valid timestamp). Error: {e}"
                                )
                                # Row continues to be processed and emitted (no continue statement)
                                # We skip tracking invalid dates for max_row_timestamp
                        # If updated_at_str is empty/null, emit record (backward compatible)
                        elif enable_row_level:
                            # Log when column exists but value is empty
                            LOGGER.debug(
                                f"Replication key active but '{selected_column}' is empty/null. "
                                f"Emitting record (backward compatible)."
                            )
                    
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

    if enable_row_level and rows_filtered > 0:
        LOGGER.info(
            f'Replication key: {rows_filtered} rows filtered out (timestamp <= bookmark)'
        )
    
    if enable_row_level and max_row_timestamp:
        LOGGER.info(
            f'Replication key: Max timestamp of processed rows: {max_row_timestamp.isoformat()}'
        )
    
    # Warn if invalid timestamps found when replication key is active
    if enable_row_level and invalid_timestamps_count > 0:
        LOGGER.warning(
            f'Replication key: Found {invalid_timestamps_count} rows with invalid timestamps. '
            f'Previous replication_key_value from state will be retained.'
        )

    return records_synced, max_row_timestamp, selected_column
