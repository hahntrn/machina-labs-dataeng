DATA_PATH = '../data/sample.parquet'
DATA_DIR = '../data/'
RAW_FEATURES_SAVE_PATH = f'{DATA_DIR}raw_features/'
INTERPOLATED_FEATURES_SAVE_PATH = f'{DATA_DIR}interpolated_features/'
ENGINEERED_FEATURES_SAVE_PATH = f'{DATA_DIR}engineered_features/'
RUNTIME_STATS_SAVE_PATH = f'{DATA_DIR}runtime_stats/'

TIME_SERIES_COLUMN_NAMES = ['run_uuid', 'time', 'field', 'robot_id', 'value']
FEATURES_COLUMN_NAMES = ['fx_1', 'fx_2', 'fy_1', 'fy_2', 'fz_1', 'fz_2', 'x_1', 'x_2', 'y_1', 'y_2', 'z_1', 'z_2']
ENGINEERED_FEATURES_COLUMN_NAMES = ['vx_1', 'vy_1', 'vz_1', 'vx_2', 'vy_2', 'vz_2', 'ax_1', 'ay_1', 'az_1', 'ax_2', 'ay_2', 'az_2', 'v1', 'v2', 'a1', 'a2', 'f1', 'f2']

ROBOT_IDS = ['1', '2']
AXES = ['x', 'y', 'z']
TOTAL_FEATURES = ['v', 'a', 'f'] # for calculating total velocity, acceleration, force