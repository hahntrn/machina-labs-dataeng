import fire
import logging
from data_processing import *

def process_data(load_path: str = C.DATA_PATH) -> None:
    """ Process a batch of timeseries data and extract features.
    Args:
        data_path (str): Path to the data to process.
        existing_data_path (str): Path to existing data to append to.
        verbose (bool): For debugging purposes.
    """
    logger = logging.getLogger('DataPipeline')
    pipeline = DataPipeline(logger)
    data = pipeline.read_parquet(load_path)
    
    # features dict run_uuid to a dataframe
    features_dict = pipeline.convert_timeseries_to_features(data, save=True)
    features_dict = pipeline.calculate_engineered_features(features_dict, save=True)
    stats_dict = pipeline.calculate_runtime_stats(features_dict, save=True)
    for k,v in stats_dict.items():
        print(k, v)

def process_data_narrow(load_path: str = C.DATA_PATH) -> None:
    """ Process a batch of timeseries data and extract features.
    Args:
        data_path (str): Path to the data to process.
        existing_data_path (str): Path to existing data to append to.
        verbose (bool): For debugging purposes.
    """
    logger = logging.getLogger('DataPipeline')
    pipeline = DataPipeline(logger)
    data = pipeline.read_parquet(load_path)
    
    features_dict = pipeline.convert_timeseries_to_features(data, save=True)
    for uuid, run_features in features_dict.items():
        run_features = pipeline.calculate_engineered_features_for_run(run_features, uuid, save=True)
        run_stats = pipeline.calculate_runtime_stats_for_run(run_features, uuid, save=True)
        print(uuid, run_stats)


def convert_timeseries_to_features(load_path: str = C.DATA_PATH, save_path: str = C.INTERPOLATED_FEATURES_SAVE_PATH) -> None:
    """ Convert a batch of timeseries data to features.
    Args:
        load_path (str): Path to the data to process.
        save_path (str): Path to save the processed data.
    """
    logger = logging.getLogger('DataPipeline')
    pipeline = DataPipeline(logger)
    data = pipeline.read_parquet(load_path)
    features_dict = pipeline.convert_timeseries_to_features(data, save=True, filetype='parquet')
    pipeline.save_csv(features_dict, save_path)

def calculate_engineered_features(load_path: str, save_path: str) -> None:
    """ Calculate engineered features for a batch of features.
    Args:
        load_path (str): Path to the data to process.
        save_path (str): Path to save the processed data.
    """
    logger = logging.getLogger('DataPipeline')
    pipeline = DataPipeline(logger)
    data = pipeline.read_parquet(load_path)
    features_dict = pipeline.calculate_engineered_features(data, save=True, filetype='parquet')
    pipeline.save_parquet(features_dict, save_path)

def calculate_runtime_stats(load_path: str = C.DATA_PATH, save_path: str = C.ENGINEERED_FEATURES_SAVE_PATH) -> None:
    """ Calculate runtime stats for a batch of features.
    Args:
        load_path (str): Path to the data to process.
        save_path (str): Path to save the processed data.
    """
    logger = logging.getLogger('DataPipeline')
    pipeline = DataPipeline(logger)
    data = pipeline.read_parquet(load_path)
    stats = pipeline.calculate_runtime_stats(data)
    pipeline.save_parquet(stats, save_path)

def main():
    fire.Fire()

main()