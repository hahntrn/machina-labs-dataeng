import constants as C
import dask.dataframe as dd
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os
import logging
import json

class DataPipeline:
    def __init__(self, logger=None):
        self.logger = logger
        if logger is None:
            self.logger = logging.getLogger('DataPipeline')

    def read_parquet(self, data_path):
        # check correct extension
        if not data_path.endswith('.parquet'):
            error_message = f'"{data_path}" is not a parquet file.'
            self.logger.error(error_message)
            raise ValueError(error_message)
        return pd.read_parquet(data_path, engine='pyarrow')

    def save(self, data, save_dir, run_uuid, filetype):
        """Save the data to a csv or parquet file."""
        if not os.path.exists(save_dir):
            os.mkdir(save_dir)
        # supress scientific notation for uuid
        if filetype == 'csv':
            data.to_csv(f'{save_dir}{run_uuid:.0f}.csv', index=True)
        elif filetype == 'parquet':
            data.to_parquet(f'{save_dir}{run_uuid:.0f}.parquet', engine='pyarrow')
        elif filetype == 'json':
            with open(f'{save_dir}{run_uuid:.0f}.json', 'w') as f:
                json.dump(data, f)

    def check_columns(self, time_series, columns):
        """Check that the columns are present in the dataframe."""
        for column in columns:
            if column not in time_series.columns:
                error_message = f'"{column}" is not a column name in the dataframe.'
                self.logger.error(error_message)
                raise ValueError(error_message)

    def load_features(self, load_path):
        features_dict = {}
        for file in os.listdir(load_path):
            if file.endswith('.csv'):
                run_uuid = np.float64(file.split('.')[0])
                features_dict[run_uuid] = pd.read_csv(f'{load_path}{file}', index_col=0)
        return features_dict

    def convert_timeseries_to_features_for_run(self, run_features, run_uuid, save=False, filetype='csv'):
        self.check_columns(run_features, C.TIME_SERIES_COLUMN_NAMES)
        run_features = run_features.pivot_table(
                values='value', index='time', columns='feature', fill_value=np.nan
            ).sort_index(ascending=True)
        if save:
            self.save(run_features, C.RAW_FEATURES_SAVE_PATH, run_uuid, filetype)
        
        for feature_name in C.FEATURES_COLUMN_NAMES:
            if feature_name not in run_features.columns:
                # fill with NaN if feature is not recorded for this run
                run_features[feature_name] = np.nan
            else:
                # interpolate missing valuesda
                run_features[feature_name] = run_features[feature_name].interpolate(method='time')
        run_features = run_features.reset_index()
        if save:
            self.save(run_features, C.INTERPOLATED_FEATURES_SAVE_PATH, run_uuid, filetype)
        return run_features

    def convert_timeseries_to_features(self, data, save=False, filetype='csv'):
        self.check_columns(data, C.TIME_SERIES_COLUMN_NAMES)

        # convert time to datetime, drop NaT, poorly formatted times
        data['time'] = pd.to_datetime(data['time'], errors='coerce').dropna()

        # create a feature column for each field
        data['feature'] = data['field'] + '_' + data['robot_id'].astype(str)

        return {
            run_uuid: self.convert_timeseries_to_features_for_run(run_features, run_uuid, save, filetype)
            for run_uuid, run_features in data.groupby(['run_uuid'])
        }


    def calculate_engineered_features_for_run(self, run_features, run_uuid, save=False, filetype='csv'):
        """ Calculate engineered features for a single run. """
        self.check_columns(run_features, C.FEATURES_COLUMN_NAMES)
        run_features = run_features.fillna(0) # fill all NaN with 0 for calculations
        run_features['dt'] = run_features['time'].diff().dt.total_seconds() * 1000 # convert to milliseconds
        for robot_id in C.ROBOT_IDS:
            for axis in C.AXES:
                # displacement, velocity, and acceleration
                run_features[f'd{axis}_{robot_id}'] = run_features[f'{axis}_{robot_id}'].diff()
                run_features[f'v{axis}_{robot_id}'] = run_features[f'd{axis}_{robot_id}'] / run_features['dt']
                run_features[f'a{axis}_{robot_id}'] = run_features[f'v{axis}_{robot_id}'].diff() / run_features['dt']

            # total velocity, acceleration, force
            for feature in C.TOTAL_FEATURES:
                run_features[f'{feature}{robot_id}'] = sum(run_features[f'{feature}{axis}_{robot_id}'] for axis in C.AXES)
        if save:
            self.save(run_features, C.ENGINEERED_FEATURES_SAVE_PATH, run_uuid, filetype)
        return run_features

    def calculate_engineered_features(self, features_dict, save=False, filetype='csv'):
        """ Returns a dictionary of dataframes of engineered features for each run_uuid.
        """
        for run_uuid, run_features in features_dict.items():
            features_dict[run_uuid] = self.calculate_engineered_features_for_run(run_features, run_uuid, save, filetype)
        return features_dict

    def get_engineered_features(self, features_dict, run_uuid, load_dir=None, save=False, filetype='csv'):
        """ Returns a dataframe of engineered features for a given run_uuid.
        If save_dir is provided, the dataframe is loaded from the csv file.
        Otherwise, the dataframe is loaded from the features dictionary.
        """
        if load_dir is not None:
            load_path = f'{load_dir}/{run_uuid}.csv'
            return pd.read_csv(load_path, index_col=0)
        return features_dict[run_uuid][C.ENGINEERED_FEATURES_COLUMN_NAMES]

    def calculate_runtime_stats_for_run(self, run_features, run_uuid, save=False):
        """ Calculate 
        - run start time
        - run start time
        - run stop time
        - total runtime
        - total distance traveled
        """
        
        distance_traveled_per_robot = [np.sqrt(
                        run_features[f'dx_{robot_id}']**2 + 
                        run_features[f'dy_{robot_id}']**2 + 
                        run_features[f'dz_{robot_id}']**2
                    ).sum() for robot_id in C.ROBOT_IDS]
        self.logger.info(f'distance traveled difference between robots 1 and 2: \
            {abs(distance_traveled_per_robot[0] - distance_traveled_per_robot[1])}')

        start_time = run_features['time'].iloc[0]
        stop_time = run_features['time'].iloc[-1]
        datetime_format = '%Y-%m-%d %H:%M:%S.%f'
        stats = {
            'run_start_time': start_time.strftime(datetime_format),
            'run_stop_time': stop_time.strftime(datetime_format),
            'total_runtime': str(stop_time - start_time),
            'total_distance_traveled': sum(distance_traveled_per_robot) / 2,
            'total_distance_traveled_1': distance_traveled_per_robot[0],
            'total_distance_traveled_2': distance_traveled_per_robot[1],
        }
        if save:
            self.save(stats, C.RUNTIME_STATS_SAVE_PATH, run_uuid, filetype='json')
        return stats
    
    def calculate_runtime_stats(self, features_dict, save=False):
        runtime_stats = {}
        for run_uuid, run_features in features_dict.items():
            runtime_stats[run_uuid] = self.calculate_runtime_stats_for_run(run_features, run_uuid)
            self.logger.info(f'Runtime stats for run {run_uuid}:\n', self.runtime_stats[run_uuid])
        if save:
            self.save(runtime_stats, C.RUNTIME_STATS_SAVE_PATH, filetype='json')
        return runtime_stats

