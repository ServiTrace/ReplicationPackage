# %% Imports
import pandas as pd
from plotnine import *
import numpy as np
from stochastic.processes.continuous import FractionalBrownianMotion
from sb_importer import *

# %%
def upscale_trace(per_minute_rates_arr, scale_factor=1, scale_type='linear', scale_rate_per_second=None):  # noqa E501
        # Adjust scale_factor to achieve a given scale_rate
        if scale_rate_per_second:
            mean_rate_per_second = invocations_per_minute.mean() / 60
            if mean_rate_per_second == 0:
                scale_factor = 1
            else:
                scale_factor = scale_rate_per_second / mean_rate_per_second

        bm = FractionalBrownianMotion(hurst=0.8, t=10)
        magnitude_multiplier = 100  # Need to increase magnitude or values become too small
        samples = bm.sample(60 * len(per_minute_rates_arr)) * magnitude_multiplier
        all_bm_samples = samples + np.abs(np.floor(samples.min()))

        per_second_rates = np.array([])
        for i in range(len(per_minute_rates_arr)):
            current_rate_minute = per_minute_rates_arr[i]
            bm_samples = all_bm_samples[i * 60:(i + 1) * 60]

            # Scale random samples by actual request rate per minute
            total_units = bm_samples.sum()
            requests_per_unit = current_rate_minute / total_units
            upscaled_samples = bm_samples * requests_per_unit

            per_second_rates = np.append(per_second_rates, upscaled_samples)

        scaled_per_second_rates = None
        if scale_type == 'linear':
            scaled_per_second_rates = per_second_rates * scale_factor
        elif scale_type == 'compound':
            # Use compounding to scale
            compounding_fractions = per_second_rates / per_second_rates.sum()
            max_fraction = np.quantile(compounding_fractions, 0.95)
            # Find exponent required to scale the maximum requests per second by the scale_factor
            exponent = np.log(scale_factor) / np.log(1 + max_fraction)
            scaled_per_second_rates = per_second_rates * np.power(1 + compounding_fractions, exponent)  # noqa E501
            # Clip extremes
            clip_threshold = np.quantile(scaled_per_second_rates, 0.95)
            scaled_per_second_rates[scaled_per_second_rates > clip_threshold] = clip_threshold
        else:
            raise Exception(f'Unknown scaling type: {scale_type}')

        return np.round(scaled_per_second_rates)

# %% Read Azure traces
root = '../data/workload_traces/20min_picks'
fluct = pd.read_csv(root + '/fluctuating.csv').transpose().values[0]
jump = pd.read_csv(root + '/jump.csv').transpose().values[0]
spikes = pd.read_csv(root + '/spikes.csv').transpose().values[0]
steady = pd.read_csv(root + '/steady.csv').transpose().values[0]

# %% Combine traces into one df
fluct_df = pd.DataFrame({
    'values': np.concatenate([np.round(np.repeat(fluct / 60, 60)), upscale_trace(fluct)]),
    'origin': np.repeat(['default', 'upscaled'], 60*len(fluct)),
    'time': np.tile(np.arange(0, 60*len(fluct)), 2),
    'type': 'Fluctuating'
})
jump_df = pd.DataFrame({
    'values': np.concatenate([np.round(np.repeat(jump / 60, 60)), upscale_trace(jump)]),
    'origin': np.repeat(['default', 'upscaled'], 60*len(fluct)),
    'time': np.tile(np.arange(0, 60*len(fluct)), 2),
    'type': 'Jump'
})
spikes_df = pd.DataFrame({
    'values': np.concatenate([np.round(np.repeat(spikes / 60, 60)), upscale_trace(spikes)]),
    'origin': np.repeat(['default', 'upscaled'], 60*len(fluct)),
    'time': np.tile(np.arange(0, 60*len(fluct)), 2),
    'type': 'Spikes'
})
steady_df = pd.DataFrame({
    'values': np.concatenate([np.round(np.repeat(steady / 60, 60)), upscale_trace(steady)]),
    'origin': np.repeat(['default', 'upscaled'], 60*len(fluct)),
    'time': np.tile(np.arange(0, 60*len(fluct)), 2),
    'type': 'Steady'
})
all_df = pd.concat([fluct_df, jump_df, spikes_df, steady_df]).reset_index()

# %% Plot
def onlyints(ticks):
    return [str(int(tick)) if (isinstance(tick, int) or tick.is_integer()) else '' for tick in ticks]

plt = ggplot(all_df) +\
    theme_light(base_size=14) +\
    theme(figure_size=(12, 1.5), legend_position=(0.5, 1.25),
         legend_background=element_rect(alpha=0), legend_margin=0,
         subplots_adjust={'wspace': 0.12},
         legend_box_margin=0) +\
    geom_line(aes(x='time', y='values', group='origin', color='origin',
                 linetype='origin')) +\
    facet_wrap('type', nrow=1, scales='free_y') +\
    xlab('Time [s]') + ylab('Num. Requests') +\
    guides(color=guide_legend(nrow=1, title=''), linetype=guide_legend(nrow=1, title='')) +\
    scale_color_manual(values=['tomato', 'dodgerblue'], labels=['Azure trace', 'Upscaled trace']) +\
    scale_linetype_manual(values=['solid', (0, (2,4))], labels=['Azure trace', 'Upscaled trace']) +\
    scale_y_continuous(labels=onlyints)

plt.save(path=plots_path(), filename='azure_vs_upscaled.pdf')
plt