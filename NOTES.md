

# Usage
```
cd src
main.py process_data
```

```
main.py process_data --load_path '../data/new_data.parquet'
```

# Design Decisions
- Pandas framework was chosen because for this simple ETL, the operations are straightforward in the tabular format, the data volume is small enough to be processed in-memory, and data usage seems to allow for independence between separate runs since all statistics and features are run-specific. A relational database can be a good option when these factors change, e.g. scaling up to hundreds or thousands of runs, a single run gets too big to be processed at once due to either high sampling rate or additional sensors or longer run duration, adding features that need to combine sensor readings from multiple different runs.
- The data from each run can be saved as a separate csv file.
- Per-run functions (`convert_timeseries_to_features_per_run`, `calculate_engineered_features_for_run`, `calculate_runtime_stats_for_run`) can be parallelized at each stage for different runs. Entire pipeline for a run can also be parallelized since they have narrow dependecies. Separating the timeseries into different runs must be done sequentially, but could still be parallelized by batches. Switching from using Pandas to equivalent in Dask.Dataframe is one good option for parallelizing tasks when scaling up.
- Available option to save intermediate states of the dataframe so that they can be easily accessed.
- Interpolating missing data according to the time difference between two observations as opposed to matching the timestamp allows for more accurate velocity and acceleration calculations since the exact time difference between timestamps are preserved. If the accuracy suffers, I would try a spline interpolation method such as cubic. If we find that interpolation results in too many extraneous datapoints, we might consider resampling the the data at a lower frequency such as 0.01s. 
